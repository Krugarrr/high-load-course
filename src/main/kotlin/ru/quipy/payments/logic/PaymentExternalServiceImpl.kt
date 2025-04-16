package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
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
import java.io.IOException
import java.io.InterruptedIOException
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


    private val connectionPool = ConnectionPool(
        maxIdleConnections = 100,
        keepAliveDuration = 5,
        timeUnit = TimeUnit.MINUTES
    )

    private val client = OkHttpClient.Builder()
        .callTimeout(1200, TimeUnit.MILLISECONDS)
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .dispatcher(Dispatcher().apply {
            maxRequests = properties.parallelRequests
            maxRequestsPerHost = properties.parallelRequests
        })
        .connectionPool(connectionPool)
        .retryOnConnectionFailure(true)
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests, true)
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val file = File("case5.txt")
        logger.info("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}" +
                    "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var acquire = semaphore.tryAcquire(requestAverageProcessingTime.toSeconds(), TimeUnit.SECONDS)
        if (!acquire) {
            logger.error("[$accountName] Payment $paymentId semaphore acquire timeout")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore acquire timeout")
            }
            return
        }

        rateLimiter.tickBlocking()

        client.newCall(request).enqueue (object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    val body = try {
                        response.body?.string()?.let { mapper.readValue(it, ExternalSysResponse::class.java) }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Response parse error for $paymentId", e)
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    val success = body?.result ?: false
                    logger.info("[$accountName] Payment $paymentId processed: $success")

                    paymentESService.update(paymentId) {
                        it.logProcessing(success, now(), transactionId, reason = body?.message)
                    }
                } finally {
                    response.close()
                    if (acquire)
                        semaphore.release()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                try {
                    logger.error("[$accountName] Payment failed for $paymentId", e)
                    val reason = when (e) {
                        is SocketTimeoutException -> "Request timeout"
                        else -> e.message ?: "Unknown error"
                    }

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = reason)
                    }
                } finally {
                    if (acquire)
                        semaphore.release()
                }
            }
        })
    }

    fun calcPercentile(): Long {
//        if (requestTimesParticular.count() < 100) return 1200;
//        val sortedBuffer = requestTimesParticular.sorted()
//        val sortedBuffer90PC = sortedBuffer.take((sortedBuffer.size * 0.9).toInt())
//        return sortedBuffer90PC.last()
        return 1200

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()