package org.world.data

import org.apache.http.HttpHost
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.world.data.FilteredTwitterConnection.Companion.FIELD_TIMESTAMP
import org.world.data.FilteredTwitterConnection.Companion.FIELD_TIMESTAMP_MS
import org.world.toJson
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*

private val logger = LogManager.getLogger(ElasticSink::class.java)

private val df: DateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
private const val DEFAULT_PORT = 9200

private fun getDate(date: Any?): String {
    return df.format(date.toString().toLong())
}

class ElasticSink(
    hostname: String? = "localhost",
    port: Int = DEFAULT_PORT,
    private val collection: String
) : DataTarget {
    private val client = RestHighLevelClient(RestClient.builder(HttpHost(hostname, port, "http")))

    override fun insertDocument(document: Map<*, *>) {
        logger.info("Inserting document id = " + document["id"])
        val request = IndexRequest(collection).id("id").source(toJson(fixDocument(document), logger), XContentType.JSON)
        val response = client.index(request, RequestOptions.DEFAULT)
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
