package org.world.data

import com.fasterxml.jackson.databind.ObjectMapper
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.Location
import com.twitter.hbc.core.endpoint.Location.Coordinate
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.slf4j.LoggerFactory
import org.world.data.FilteredTwitterConnection.Companion.FIELD_CREATED_AT
import org.world.data.FilteredTwitterConnection.Companion.FIELD_CREATED_DATE
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

data class AuthDetails(val consumerKey: String?, val consumerSecret: String?, val token: String?, val secret: String?)

private fun parseAuth(configProperties: Properties) = AuthDetails(
    consumerKey = configProperties.getProperty("consumerkey"),
    consumerSecret = configProperties.getProperty("consumersecret"),
    secret = configProperties.getProperty("secret"),
    token = configProperties.getProperty("token")
)

private val logger = LoggerFactory.getLogger(TwitterConnection::class.java)

class HoseBirdWrapper {
    private val queue: BlockingQueue<String>
    private var client: Client? = null
    private val endpoint: StatusesFilterEndpoint

    init {
        queue = LinkedBlockingQueue(10000)
        endpoint = StatusesFilterEndpoint()
    }

    fun configure(
        boundingBox: Location?,
        keywords: List<String?>?,
        userIds: List<Long?>?,
        languages: List<String?>?,
        details: AuthDetails
    ) {
        if (boundingBox != null) {
            logger.info("Tracking location = $boundingBox")
            endpoint.locations(listOf(boundingBox))
        }
        if (keywords != null && keywords.isNotEmpty()) {
            logger.info("Tracking keywords = $keywords")
            endpoint.trackTerms(keywords)
        }
        if (userIds != null && userIds.isNotEmpty()) {
            logger.info("Tracking userIds = $userIds")
            endpoint.followings(userIds)
        }
        if (languages != null && languages.isNotEmpty()) {
            logger.info("Tracking languages = $languages")
            endpoint.languages(languages)
        }
        val auth: Authentication = OAuth1(details.consumerKey, details.consumerSecret, details.token, details.secret)

        // Create a new BasicClient. By default gzip is enabled.
        client = ClientBuilder()
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(StringDelimitedProcessor(queue))
            .build()
    }

    fun take(): String {
        return queue.take()
    }

    fun connect() {
        client!!.connect()
    }

    fun stop() {
        client!!.stop()
    }
}

private val mapper = ObjectMapper()

class TwitterConnection internal constructor(config: Properties) : FilteredTwitterConnection {
    private val hbc: HoseBirdWrapper?
    private var running = true

    init {
        hbc = HoseBirdWrapper()
        hbc.configure(
            parseLocation(config),
            parseKeywords(config),
            parseUserIds(config),
            parseLanguages(config),
            parseAuth(config)
        )
    }

    private fun parseLocation(configProperties: Properties): Location? {
        return if (configProperties.containsKey("swlatitude")) {
            // Dimensions of the bounding box if geo-located
            val swlatitude = java.lang.Double.valueOf(configProperties.getProperty("swlatitude"))
            val swlongitude = java.lang.Double.valueOf(configProperties.getProperty("swlongitude"))
            val nelatitude = java.lang.Double.valueOf(configProperties.getProperty("nelatitude"))
            val nelongitude = java.lang.Double.valueOf(configProperties.getProperty("nelongitude"))
            Location(Coordinate(swlongitude, swlatitude), Coordinate(nelongitude, nelatitude))
        } else {
            null
        }
    }

    private fun parseKeywords(config: Properties): List<String?>? {
        return if (config.containsKey("keywords")) {
            config.getProperty("keywords").split(",")
                .filterNot(String::isNullOrBlank)
                .map(String::trim).filterNot(String::isEmpty)
        } else {
            null
        }
    }

    private fun parseLanguages(config: Properties): List<String?>? {
        return if (config.containsKey("languages")) {
            config.getProperty("languages").split(",")
                .filterNot(String::isNullOrBlank)
                .map(String::trim).filterNot(String::isEmpty)
        } else {
            null
        }
    }

    private fun parseUserIds(config: Properties): List<Long?>? {
        return if (config.containsKey("userIds")) {
            config.getProperty("userIds").split(",")
                .filterNot(String::isNullOrBlank).map(String::trim)
                .filterNot(String::isEmpty).map(String::toLong)
        } else {
            null
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun processDocuments(target: DataTarget) {
        var msgRead = 0
        var startTime = System.currentTimeMillis()
        logger.info("Crawler started.")
        try {
            logger.info("Contacting Twitter.")
            hbc!!.connect()
            startTime = System.currentTimeMillis()
            while (running) {
                val msg = hbc.take()
                val tweetMap: Map<String?, Any?> = mapper.readValue(msg, Map::class.java) as Map<String?, Any?>

                // These are warnings that we're breaching firehose limits and missing tweets
                if (tweetMap.containsKey("limit")) {
                    logger.debug("Limit message received: $msg")
                    continue
                }

                val filteredMap = filterTweet(tweetMap)
                try {
                    filteredMap[FIELD_CREATED_DATE] = getTwitterDate(tweetMap[FIELD_CREATED_AT] as String?).time
                    target.insertDocument(filteredMap)
                } catch (e: Exception) {
                    logger.error("Exception writing tweet = $msg")
                }
                msgRead++
            }
        } catch (e: InterruptedException) {
            logger.debug("Interrupted exception! $e")
        } catch (e: Exception) {
            logger.debug("Unexpected exception: $e")
            logger.debug("Attempting to print stack trace", e)
        } finally {
            logger.info("Stopping the hosebird client.")
            logger.info("Read " + msgRead + " tweets in " + (System.currentTimeMillis() - startTime) + " ms.")
        }
    }

    override fun close() {
        running = false
        if (hbc != null) {
            logger.info("HoseBird stopped.")
            hbc.stop()
        }
    }
}