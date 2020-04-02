package org.world.data

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.logging.log4j.LogManager
import org.world.toJson
import java.util.*

private val logger = LogManager.getLogger(KafkaConnection::class.java)

private const val DEFAULT_HOST = "localhost"
private const val DEFAULT_PORT = 32769

class KafkaConnection(server: String = DEFAULT_HOST, port: Int = DEFAULT_PORT, private val topic: String) : DataTarget {
    private val producer: Producer<String, String>

    init {
        producer = KafkaProducer(buildProperties(server, port))
    }

    override fun insertDocument(document: Map<*, *>) {
        logger.debug("Inserting document id = " + document["id"] + " on topic = " + topic)
        producer.send(ProducerRecord(topic, toJson(document, logger)))
    }

    private fun buildProperties(host: String = DEFAULT_HOST, port: Int = DEFAULT_PORT): Properties {
        val props = Properties()
        props["bootstrap.servers"] = "$host:$port"
        props["acks"] = "all"
        props["retries"] = 0
        props["batch.size"] = 16384
        props["linger.ms"] = 1
        props["buffer.memory"] = 33554432
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        return props
    }

    override fun close() {
        producer.close()
    }
}