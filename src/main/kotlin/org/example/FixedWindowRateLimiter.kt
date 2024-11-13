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

        val currentCount = jedis.get(key)?.toIntOrNull() ?: 0
        val isAllowed = currentCount < limit

        if (isAllowed) {
            // Transaction will also pipeline
            jedis.multi().run {
                incr(key)
                expire(key, windowDurationSeconds, ExpiryOption.NX) // Set expire only if expiration was not set before
                exec()
            }
        }
        return isAllowed
    }
}