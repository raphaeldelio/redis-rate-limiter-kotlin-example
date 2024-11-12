package org.example

import redis.clients.jedis.Jedis
import java.util.*

class SlidingWindowLogRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val windowSize: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val windowStartTime = currentTime - windowSize * 1000

        val key = "rate_limit:$clientId"
        val uniqueMember = "$currentTime-${UUID.randomUUID()}"  // In case we have multiple requests at the same millisecond

        // Transaction will also pipeline
        val result = jedis.multi().run {
            zremrangeByScore(key, 0.0, windowStartTime.toDouble())
            zadd(key, currentTime.toDouble(), uniqueMember)
            expire(key, windowSize)
            zrange(key, 0, -1)
            exec()
        }

        if (result.isEmpty()) {
            throw IllegalStateException("Empty result from Redis pipeline")
        }

        val requestCount = (result[3] as List<*>).size
        return requestCount <= limit
    }

    fun isAllowedHashAlternative(clientId: String): Boolean {
        val key = "rate_limit:$clientId"
        val fieldKey = UUID.randomUUID().toString()

        val requestCount = jedis.hlen(key)
        val isAllowed = requestCount < limit

        if (isAllowed) {
            jedis.multi().run {
                hset(key, fieldKey, "")
                hexpire(key, windowSize, *Array(1000) { fieldKey })
                exec()
            }
        }
        return isAllowed
    }

    fun isAllowedStringAlternative(clientId: String): Boolean {
        val requestKeyPattern = "rate_limit:$clientId:*"
        val requestCount = jedis.keys(requestKeyPattern).size
        val isAllowed = requestCount < limit

        if (isAllowed) {
            val uniqueKey = "rate_limit:$clientId:${UUID.randomUUID()}"
            jedis.setex(uniqueKey, windowSize, "")
        }

        return isAllowed
    }
}