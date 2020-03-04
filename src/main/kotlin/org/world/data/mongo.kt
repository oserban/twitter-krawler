package org.world.data

import com.mongodb.*
import org.apache.logging.log4j.LogManager
import java.io.IOException

private val logger = LogManager.getLogger(MongoConnection::class.java)

class MongoConnection(
    mongoHost: String? = null, mongoPort: Int? = null, dbName: String? = null,
    mongoCollectionName: String? = null,
    private val startId: Long? = null, private val lastId: Long? = null,
    private val language: String? = null
) : DataSource, DataTarget {

    private val mongoClient: MongoClient
    private val mongoCollection: DBCollection

    init {
        val clientURI = MongoClientURI(createHostUri(mongoHost, mongoPort))
        mongoClient = MongoClient(clientURI)
        if (dbName == null) {
            throw IOException("Invalid database provided")
        }
        val db = mongoClient.getDB(dbName)
        mongoCollection = db.getCollection(mongoCollectionName)
    }

    private fun createHostUri(host: String?, port: Int?): String {
        val sb = StringBuilder("mongodb://")
        if (host != null) {
            sb.append(host)
        } else {
            sb.append("localhost")
        }
        if (port != null && port > 0) {
            sb.append(":").append(port)
        }
        sb.append("/")
        return sb.toString()
    }

    @Suppress("UNCHECKED_CAST")
    override fun processDocuments(target: DataTarget) {
        val cursor = mongoCollection.find(buildQuery())
        cursor.sort(BasicDBObject("id", 1))
        for (doc in cursor) {
            target.insertDocument(doc.toMap() as Map<Any, Any>)
        }
    }

    private fun buildQuery(): DBObject? {
        var query: DBObject? = null
        if (lastId != null && lastId > 0) {
            query = BasicDBObject("id", BasicDBObject("\$lt", lastId))
        }
        if (startId != null && startId >= 0) {
            if (query == null) {
                query = BasicDBObject("id", BasicDBObject("\$gte", startId))
            } else {
                (query["id"] as BasicDBObject)["\$gte"] = startId
            }
        }
        if (language != null) {
            if (query == null) {
                query = BasicDBObject("lang", language)
            } else {
                query.put("lang", language)
            }
        }
        return query
    }

    override fun insertDocument(document: Map<*, *>) {
        logger.info("Inserting document id = " + document["id"] + " into " + mongoCollection.fullName)
        mongoCollection.insert(BasicDBObject(document))
    }

    override fun close() {
        mongoClient.close()
    }
}