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
import java.util.*
import java.util.concurrent.Executors
import kotlin.system.exitProcess
import dev.openfeature.contrib.providers.flagd.FlagdOptions
import dev.openfeature.contrib.providers.flagd.FlagdProvider
import dev.openfeature.sdk.Client
import dev.openfeature.sdk.EvaluationContext
import dev.openfeature.sdk.ImmutableContext
import dev.openfeature.sdk.Value
import dev.openfeature.sdk.OpenFeatureAPI

const val topic = "orders"
const val groupID = "fraud-detection"

private val logger: Logger = LogManager.getLogger(groupID)

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
    props[MAX_POLL_RECORDS_CONFIG] = "500"  // Process up to 500 records per poll
    props[ENABLE_AUTO_COMMIT_CONFIG] = "false"  // Manual commit for better control
    props[MAX_POLL_INTERVAL_MS_CONFIG] = "300000"  // 5 minutes
    props[SESSION_TIMEOUT_MS_CONFIG] = "45000"  // 45 seconds
    props[FETCH_MIN_BYTES_CONFIG] = "1024"  // Wait for at least 1KB
    props[MAX_PARTITION_FETCH_BYTES_CONFIG] = "1048576"  // 1MB per partition
    
    val bootstrapServers = System.getenv("KAFKA_ADDR")
    if (bootstrapServers == null) {
        println("KAFKA_ADDR is not supplied")
        exitProcess(1)
    }
    props[BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    
    val consumer = KafkaConsumer<String, ByteArray>(props).apply {
        subscribe(listOf(topic))
    }

    val executor = Executors.newFixedThreadPool(4)  // 4 parallel processors
    var totalCount = 0L

    consumer.use {
        while (true) {
            try {
                val records = consumer.poll(ofMillis(1000))
                if (!records.isEmpty) {
                    val futures = records.map { record ->
                        executor.submit {
                            try {
                                val orders = OrderResult.parseFrom(record.value())
                                logger.info("Processing orderId: ${orders.orderId}")
                            } catch (e: Exception) {
                                logger.error("Failed to process record", e)
                            }
                        }
                    }
                    
                    // Wait for all to complete
                    futures.forEach { it.get() }
                    consumer.commitSync()
                    totalCount += records.count()
                    logger.info("Committed ${records.count()} records, total processed: $totalCount")
                }
            } catch (e: Exception) {
                logger.error("Error polling/processing records", e)
                Thread.sleep(1000)  // Brief pause on error
            }
        }
    }
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
