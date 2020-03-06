package org.world.data

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoCollection
import org.apache.logging.log4j.LogManager
import java.io.IOException
import java.net.URI

private val logger = LogManager.getLogger(MongoConnection::class.java)

class MongoConnection(
    mongoURI: URI,
    private val startId: Long? = null, private val lastId: Long? = null,
    private val language: String? = null
) : DataSource, DataTarget {

    private val mongoClient: MongoClient
    private val mongoCollection: MongoCollection<BasicDBObject>

    init {
        val clientURI = MongoClientURI(mongoURI.toString())
        mongoClient = MongoClient(clientURI)
        if (clientURI.database.isNullOrEmpty() || clientURI.collection.isNullOrEmpty()) {
            throw IOException("Invalid database or collection provided")
        }
        val db = mongoClient.getDatabase(clientURI.database!!)
        mongoCollection = db.getCollection(clientURI.collection!!, BasicDBObject::class.java)
    }

    @Suppress("UNCHECKED_CAST")
    override fun processDocuments(target: DataTarget) {
        val cursor = mongoCollection.find(buildQuery())
        cursor.sort(BasicDBObject("id", 1))
        for (doc in cursor) {
            target.insertDocument(doc.toMap() as Map<Any, Any>)
        }
    }

    private fun buildQuery(): BasicDBObject {
        var query: BasicDBObject? = null
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
                query["lang"] = language
            }
        }
        return query!!
    }

    override fun insertDocument(document: Map<*, *>) {
        logger.info("Inserting document id = " + document["id"] + " into " + mongoCollection.namespace.fullName)
        mongoCollection.insertOne(BasicDBObject(document))
    }

    override fun close() {
        mongoClient.close()
    }
}