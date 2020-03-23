package org.world

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import com.beust.jcommander.ParameterException
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.world.data.*
import java.io.IOException
import java.net.URI
import kotlin.system.exitProcess

private val logger: Logger = LogManager.getLogger("TwitterCrawler")

private const val EXIT_CODE = 1


fun main(args: Array<String>) {
    val params = RunParams()
    val cmdParser = JCommander(params)
    cmdParser.programName = "crawler"
    cmdParser.console = LoggerConsole(logger)

    try {
        cmdParser.parse(*args)
    } catch (e: ParameterException) {
        logger.error("Error running crawler: ${e.message}")
        cmdParser.usage()
        exitProcess(EXIT_CODE)
    }

    if (params.help) {
        cmdParser.usage()
        exitProcess(0)
    }
    printParameters(params)
    printEnv()
    val timestamp = System.currentTimeMillis()
    try {
        createTarget(parseURI(params.target)).use { target: DataTarget ->
            createSource(parseURI(params.source), fromParams(params)).use { source: DataSource ->
                addShutdownHook(source)
                source.processDocuments(target)
            }
        }
    } catch (e: IOException) {
        logger.info("Unable to locate configuration file. Exiting ... ", e)
    } finally {
        logger.info("Processing ended. Processing time (hh:mm:ss): " + format(System.currentTimeMillis() - timestamp))
    }
}

private fun parseURI(source: String) = when {
    source.contains(":///") -> URI.create(source)
    source.contains("://") -> URI.create(source)
    else -> URI.create("$source:///")
}

private fun fromParams(params: RunParams) = mapOf(PROP_CONFIG to params.configFile)

private fun addShutdownHook(source: DataSource) {
    Runtime.getRuntime().addShutdownHook(Thread(Runnable {
        try {
            source.close()
        } catch (e: Exception) {
            // -- ignore
        }
    }))
}

private fun printParameters(runParameters: RunParams) {
    val sb = StringBuilder()
    for (field in runParameters.javaClass.declaredFields) {
        try {
            sb.append("\t").append(field.name).append(" = ").append(field[runParameters]).append("\n")
        } catch (e: IllegalAccessException) {
            //-- ignore
        }
    }
    logger.info("Service parameters: \n$sb")
}

private fun printEnv() {
    val sb = StringBuilder()
    for ((key, value) in System.getenv()) {
        sb.append("\t").append(key).append(" = ").append(value).append("\n")
    }
    logger.info("Environment variables: \n$sb")
}

private fun format(timestamp: Long): String {
    return String.format("%tT", timestamp)
}

class RunParams {
    @Parameter(names = ["-h", "--help"], description = "Print this help", help = true, echoInput = true)
    var help = false

    @Parameter(
        names = ["--target"],
        required = true,
        description = "The data target connection: elasticsearch, kafka, mongo or console"
    )
    var target = ""

    @Parameter(names = ["--source"], required = true, description = "The data source connection: twitter or mongo")
    var source = ""

    @Parameter(names = ["--config"], description = "The twitter config file")
    var configFile = "/config.properties"
}