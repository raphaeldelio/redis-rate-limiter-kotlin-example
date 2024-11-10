package org.example

import redis.clients.jedis.Jedis
import redis.clients.jedis.args.ExpiryOption

class FixedWindowRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val windowSize: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val key = "rate_limit:$clientId"
        val windowDurationSeconds = windowSize

        // Transaction will also pipeline
        val result = jedis.multi().run {
            incr(key)
            expire(key, windowDurationSeconds, ExpiryOption.NX) // Set expire only if expiration was not set before
            get(key)
            exec()
        }

        if (result.isEmpty()) {
            throw IllegalStateException("Empty result from Redis transaction")
        }

        val requestCount = (result[2] as String).toInt()
        return requestCount <= limit
    }
}