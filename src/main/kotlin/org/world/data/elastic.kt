package org.world.data

import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.world.data.FilteredTwitterConnection.Companion.FIELD_TIMESTAMP
import org.world.data.FilteredTwitterConnection.Companion.FIELD_TIMESTAMP_MS
import org.world.toJson
import java.net.InetAddress
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*

private val logger = LogManager.getLogger(ElasticSink::class.java)
private val df: DateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
private const val DEFAULT_PORT = 9300
private const val DEFAULT_CLUSTER_NAME = "docker-cluster"

private fun getDate(date: Any?): String {
    return df.format(date.toString().toLong())
}

class ElasticSink internal constructor(
    hostname: String?, port: Int = DEFAULT_PORT,
    private val collection: String,
    private val type: String
) : DataTarget {
    private val client: Client

    init {
        val address = TransportAddress(InetAddress.getByName(hostname), port)
        val settings = Settings.builder().put("cluster.name", DEFAULT_CLUSTER_NAME).build()
        client = PreBuiltTransportClient(settings).addTransportAddress(address)
    }

    override fun insertDocument(document: Map<*, *>) {
        logger.info("Inserting document id = " + document["id"])
        val source: String? = toJson(fixDocument(document), logger)
        val response = client.prepareIndex(collection, type).setSource(source, XContentType.JSON).get()
        if (response.status() != RestStatus.CREATED) {
            logger.error("Error while inserting document cause: " + response.status())
        }
    }

    private fun fixDocument(document: Map<*, *>): Map<*, *> {
        val result: MutableMap<String, Any> = mutableMapOf()

        result[FIELD_TIMESTAMP] = getDate(document[FIELD_TIMESTAMP_MS])

        for ((key, value) in document) {
            if (key.toString().contains(".")) {
                result.putAll(convertKey(key.toString(), value!!))
            } else {
                result[key.toString()] = value!!
            }
        }
        return result
    }

    private fun convertKey(key: String, value: Any): Map<String, Any> {
        val split = key.split("\\.").toTypedArray()
        val result: MutableMap<String, Any> =
            HashMap()
        var lastMap = result
        for (i in 0 until split.size - 1) {
            val newMap: MutableMap<String, Any> =
                HashMap()
            lastMap[split[i]] = newMap
            lastMap = newMap
        }
        lastMap[split[split.size - 1]] = value
        return result
    }

    override fun close() {
        client.close()
    }
}
