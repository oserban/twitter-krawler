package org.world.data

import java.io.Closeable
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*

interface DataTarget : Closeable {
    fun insertDocument(document: Map<*, *>)
}

interface DataSource : Closeable {
    fun processDocuments(target: DataTarget)
}

interface FilteredTwitterConnection : DataSource {
    companion object {
        private val sf = SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH)
        private val includedFields = arrayOf(
            "coordinates", "created_at", "entities", "geo", "id", "id_str", "tin_reply_to_screen_name",
            "in_reply_to_status_id_str", "in_reply_to_user_id_str", "lang", "place", "source", "text", "timestamp_ms"
        )
        private val includedUserFields = arrayOf(
            "created_at", "description", "followers_count", "following", "friends_count", "id_str",
            "lang", "location", "name", "screen_name", "statuses_count", "time_zone", "utc_offset"
        )
        const val FIELD_CREATED_DATE = "created_date"
        const val FIELD_CREATED_AT = "created_at"
        const val FIELD_TIMESTAMP = "@timestamp"
        const val FIELD_TIMESTAMP_MS = "timestamp_ms"

        init {
            sf.isLenient = true
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun filterTweet(inputMap: Map<String?, Any?>): MutableMap<String, Any?> {
        val filteredMap: MutableMap<String, Any?> = mutableMapOf()

        for (field in includedFields) {
            filteredMap[field] = inputMap[field]
        }
        val filteredUser: MutableMap<String, Any?> = mutableMapOf()

        val user = inputMap["user"] as Map<String, Any>?
        for (field in includedUserFields) {
            filteredUser[field] = user!![field]
        }
        filteredMap["user"] = filteredUser
        return filteredMap
    }

    fun getTwitterDate(date: String?): Date {
        return try {
            sf.parse(date)
        } catch (e: ParseException) {
            Date()
        }
    }
}