package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.CountingRateLimiter
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.File
import java.io.PrintWriter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestTimesParticular = CircularFifoQueue<Long>()
    private val requestTimesAll = mutableListOf<Long>()

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests, true)
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        val file = File("file.txt")
        logger.info("requestAverageProcessingTime: [${requestAverageProcessingTime}]")
        logger.info("rateLimitPerSec: [$rateLimitPerSec]")
        logger.info("paymentStartedAt: [$paymentStartedAt]")
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")


        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()
        val acquire = semaphore.tryAcquire(requestAverageProcessingTime.toSeconds(), TimeUnit.SECONDS)
        try {

            if (!acquire) {
                logger.error("[$accountName] Payment $paymentId could not acquire semaphore before the deadline. Time left:  ms")
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        now(),
                        transactionId,
                        reason = "Could not acquire semaphore before the deadline"
                    )
                }
                return
            }

            run loop@{
                (1..10).forEach {
                    var i = 0;
                    rateLimiter.tickBlocking()
                    if (now() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline breached")
                        }
                        return@loop
                    }
                    val reqStart = now()
                    client.newCall(request).execute().use { response ->
                        val reqEnd = now()
                        val reqTime = reqEnd - reqStart
                        requestTimesAll.add(reqTime)
                        requestTimesParticular.add(reqTime)
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        if (!body.result) {
                            file.appendText("- ${reqEnd-reqStart} ${i}\n")
                            i += 1
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, body: $response")
                            return@forEach
                        }

                        file.appendText("+ ${reqEnd-reqStart} ${i}\n")
                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return@loop
                    }
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }

        } finally {
            if (acquire) semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()