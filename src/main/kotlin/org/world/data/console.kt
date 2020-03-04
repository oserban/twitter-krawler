package org.world.data

import org.apache.logging.log4j.LogManager
import org.world.toJson


class ConsoleSink : DataTarget {
    private val logger = LogManager.getLogger("Console")

    override fun insertDocument(document: Map<*, *>) {
        logger.info(toJson(document, logger))
    }

    override fun close() {}
}