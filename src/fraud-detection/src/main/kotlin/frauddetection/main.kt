/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package frauddetection

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import oteldemo.Demo.*
import java.time.Duration.ofMillis
import java.time.Instant
import java.util.*
import kotlin.system.exitProcess
import kotlin.math.min
import dev.openfeature.contrib.providers.flagd.FlagdOptions
import dev.openfeature.contrib.providers.flagd.FlagdProvider
import dev.openfeature.sdk.Client
import dev.openfeature.sdk.EvaluationContext
import dev.openfeature.sdk.ImmutableContext
import dev.openfeature.sdk.Value
import dev.openfeature.sdk.OpenFeatureAPI

const val topic = "orders"
const val groupID = "fraud-detection"
const val POLL_TIMEOUT_MS = 100L
const val MAX_RETRIES = 3
const val HEALTH_CHECK_THRESHOLD_MINUTES = 5L

private val logger: Logger = LogManager.getLogger(groupID)
private var lastMessageTime = Instant.now()

fun main() {
    val options = FlagdOptions.builder()
    .withGlobalTelemetry(true)
    .build()
    val flagdProvider = FlagdProvider(options)
    OpenFeatureAPI.getInstance().setProvider(flagdProvider)

    val props = Properties()
    props[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java.name
    props[GROUP_ID_CONFIG] = groupID
    val bootstrapServers = System.getenv("KAFKA_ADDR")
    if (bootstrapServers == null) {
        println("KAFKA_ADDR is not supplied")
        exitProcess(1)
    }
    props[BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    val consumer = KafkaConsumer<String, ByteArray>(props).apply {
        subscribe(listOf(topic))
    }

    var totalCount = 0L
    var retryCount = 0

    consumer.use {
        while (true) {
            try {
                val records = consumer.poll(ofMillis(POLL_TIMEOUT_MS))
                
                if (records.isEmpty) {
                    logger.debug("No records received in polling cycle")
                    continue
                }
                
                // Reset retry count on successful poll
                retryCount = 0
                
                records.forEach { record ->
                    totalCount = processRecordWithRetry(record, totalCount)
                }
                
                lastMessageTime = Instant.now()
                
            } catch (e: Exception) {
                logger.error("Consumer error occurred", e)
                retryCount++
                
                if (retryCount >= MAX_RETRIES) {
                    logger.error("Max retries exceeded, consumer will exit")
                    throw e
                }
                
                // Exponential backoff with cap at 30 seconds
                val backoffTime = min(1000L * retryCount, 30000L)
                logger.warn("Retrying in ${backoffTime}ms (attempt $retryCount/$MAX_RETRIES)")
                Thread.sleep(backoffTime)
            }
        }
    }
}

/**
 * Processes a single Kafka record with retry logic for parsing errors
 */
fun processRecordWithRetry(record: org.apache.kafka.clients.consumer.ConsumerRecord<String, ByteArray>, currentCount: Long): Long {
    var attempts = 0
    val maxAttempts = 3
    
    while (attempts < maxAttempts) {
        try {
            val newCount = currentCount + 1
            
            if (getFeatureFlagValue("kafkaQueueProblems") > 0) {
                logger.info("FeatureFlag 'kafkaQueueProblems' is enabled, sleeping 1 second")
                Thread.sleep(1000)
            }
            
            val orders = OrderResult.parseFrom(record.value())
            logger.info("Consumed record with orderId: ${orders.orderId}, and updated total count to: $newCount")
            return newCount
            
        } catch (e: Exception) {
            attempts++
            logger.warn("Failed to process record (attempt $attempts/$maxAttempts)", e)
            
            if (attempts >= maxAttempts) {
                logger.error("Max processing attempts exceeded for record, skipping")
                return currentCount + 1 // Still increment count to avoid getting stuck
            }
            
            // Short delay before retry
            Thread.sleep(100)
        }
    }
    
    return currentCount
}

/**
 * Health check function to verify consumer is actively processing messages
 */
fun isConsumerHealthy(): Boolean {
    val timeSinceLastMessage = java.time.Duration.between(lastMessageTime, Instant.now())
    val isHealthy = timeSinceLastMessage.toMinutes() <= HEALTH_CHECK_THRESHOLD_MINUTES
    
    if (!isHealthy) {
        logger.warn("Consumer health check failed: no messages consumed in ${timeSinceLastMessage.toMinutes()} minutes")
    }
    
    return isHealthy
}

/**
* Retrieves the status of a feature flag from the Feature Flag service.
*
* @param ff The name of the feature flag to retrieve.
* @return `true` if the feature flag is enabled, `false` otherwise or in case of errors.
*/
fun getFeatureFlagValue(ff: String): Int {
    val client = OpenFeatureAPI.getInstance().client
    // TODO: Plumb the actual session ID from the frontend via baggage?
    val uuid = UUID.randomUUID()

    val clientAttrs = mutableMapOf<String, Value>()
    clientAttrs["session"] = Value(uuid.toString())
    client.evaluationContext = ImmutableContext(clientAttrs)
    val intValue = client.getIntegerValue(ff, 0)
    return intValue
}
