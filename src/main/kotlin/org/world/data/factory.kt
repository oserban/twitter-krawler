package org.world.data

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.File
import java.io.IOException
import java.net.URI
import java.util.*

const val PROP_START_ID = "start_id"
const val PROP_LAST_ID = "last_id"
const val PROP_LANG = "lang"
const val PROP_CONFIG = "config"

private val logger: Logger = LogManager.getLogger("Factory")

fun createTarget(uri: URI): DataTarget {
    var path = uri.path
    require(!(path == null || path.isEmpty())) { "Invalid uri path = $uri" }
    if (path.startsWith("/")) {
        path = path.substring(1).trim { it <= ' ' }
    }

    return when (uri.scheme.toLowerCase()) {
        "kafka" -> KafkaConnection(uri.host, uri.port, path)
        "mongodb" -> MongoConnection(uri)
        "elastic", "es" -> ElasticSink(uri.host, uri.port, path)
        "console" -> ConsoleSink()
        else -> throw IOException("Unable to build target based on the URI: $uri")
    }
}

fun createSource(uri: URI, properties: Map<String, *>): DataSource {
    return when (uri.scheme.toLowerCase()) {
        "twitter" -> TwitterConnection(loadConfig(properties[PROP_CONFIG] as String))
        "mongodb" -> {
            val startId = properties[PROP_START_ID] as Long
            val lastId = properties[PROP_LAST_ID] as Long
            val lang = properties[PROP_LANG] as String?
            MongoConnection(uri, startId = startId, lastId = lastId, language = lang)
        }
        else -> throw IOException("Unable to build target based on the URI: $uri")
    }
}

private fun loadConfig(configFile: String): Properties {
    logger.info("Loading configuration = $configFile")
    val configProperties = Properties()
    File(configFile).inputStream().use { configProperties.load(it) }
    logger.info(configProperties.toString())
    return configProperties
}