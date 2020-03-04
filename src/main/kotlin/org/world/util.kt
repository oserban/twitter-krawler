package org.world

import com.google.gson.GsonBuilder
import com.google.gson.JsonIOException
import org.apache.logging.log4j.Logger

private val gson = GsonBuilder().create()

fun toJson(element: Map<*, *>?, logger: Logger): String? {
    return if (element == null || element.isEmpty()) {
        null
    } else try {
        gson.toJson(element)
    } catch (e: JsonIOException) {
        logger.error("Error while converting to json", e)
        null
    }
}