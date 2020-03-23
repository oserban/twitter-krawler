package org.world

import com.beust.jcommander.internal.Console
import com.google.gson.GsonBuilder
import com.google.gson.JsonIOException
import org.apache.logging.log4j.Logger
import javax.naming.OperationNotSupportedException

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

class LoggerConsole(private val logger: Logger) : Console {
    override fun print(msg: String?) {
        logger.info(msg)
    }

    override fun println(msg: String?) {
        logger.info(msg)
    }

    override fun readPassword(echoInput: Boolean): CharArray {
        throw OperationNotSupportedException()
    }
}