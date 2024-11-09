package org.example

import redis.clients.jedis.Jedis
import java.util.*

class SlidingWindowRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val windowSize: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val windowStartTime = currentTime - windowSize * 1000
        val key = "rate_limit:$clientId"
        val uniqueMember = "$currentTime-${UUID.randomUUID()}"

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
}