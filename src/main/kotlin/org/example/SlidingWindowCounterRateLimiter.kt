package org.example

import redis.clients.jedis.Jedis

class SlidingWindowCounterRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val windowSize: Long,
    private val subWindowSize: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val subWindowSizeMillis = subWindowSize * 1000
        val numSubWindows = (windowSize * 1000) / subWindowSizeMillis

        val keyPrefix = "rate_limit:$clientId"

        // Calculate the current sub-window index based on the time
        val currentSubWindow = currentTime / subWindowSizeMillis
        val currentKey = "$keyPrefix:$currentSubWindow"

        // Increment the count for the current sub-window
        val result = jedis.multi().run {
            incr(currentKey)
            expire(currentKey, windowSize)
            exec()
        }

        if (result.isEmpty()) {
            throw IllegalStateException("Empty result from Redis transaction")
        }

        // Sum the counts from all sub-windows within the sliding window
        val totalCount = (0 until numSubWindows).sumOf { i ->
            val key = "$keyPrefix:${currentSubWindow - i}"
            jedis.get(key)?.toIntOrNull() ?: 0
        }

        // Check if the total count is within the limit
        return totalCount <= limit
    }
}