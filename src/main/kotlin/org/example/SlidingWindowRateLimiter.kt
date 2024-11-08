package org.example

import redis.clients.jedis.Jedis

class SlidingWindowRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val windowSize: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val windowStartTime = currentTime - windowSize * 1000
        val key = "rate_limit:$clientId"

        // Remove timestamps outside the sliding window
        // Removes all elements in the sorted set stored at
        //      key with a score between min and max (inclusive).
        jedis.zremrangeByScore(key, 0.0, windowStartTime.toDouble())

        // Check the current count within the window
        val requestCount = jedis.zcard(key)

        return if (requestCount >= limit) {
            // If the count exceeds the limit, deny the request
            false
        } else {
            // Otherwise, allow the request and record the current timestamp
            jedis.zadd(key, currentTime.toDouble(), currentTime.toString())
            // Set expiration to automatically clear keys after window size
            jedis.expire(key, windowSize)
            true
        }
    }
}