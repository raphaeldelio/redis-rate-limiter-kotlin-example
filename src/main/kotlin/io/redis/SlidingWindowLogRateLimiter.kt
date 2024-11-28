package io.redis

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
        val result = jedis.multi().run {
            zremrangeByScore(key, 0.0, windowStartTime.toDouble())
            zcard(key)
            exec()
        }

        if (result.isEmpty()) {
            throw IllegalStateException("Empty result from Redis pipeline")
        }

        val requestCount = result[1] as Long
        val isAllowed = requestCount < limit

        if (isAllowed) {
            jedis.multi().run {
                val uniqueMember = "$currentTime-${UUID.randomUUID()}" // Ensures uniqueness even within the same millisecond
                zadd(key, currentTime.toDouble(), uniqueMember)
                expire(key, windowSize)
                exec()
            }
        }

        return isAllowed
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
        val requestCount = jedis.keys(requestKeyPattern).size // Not advisable because the keys command is O(N)
        val isAllowed = requestCount < limit

        if (isAllowed) {
            val uniqueKey = "rate_limit:$clientId:${UUID.randomUUID()}"
            jedis.setex(uniqueKey, windowSize, "")
        }

        return isAllowed
    }
}