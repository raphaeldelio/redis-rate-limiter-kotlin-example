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

        val key = "rate_limit:$clientId"

        // Calculate the current sub-window index based on the time
        val currentSubWindow = currentTime / subWindowSizeMillis

        // Increment the count for the current sub-window in the hash
        val result = jedis.multi().run {
            hincrBy(key, currentSubWindow.toString(), 1)

            // Set a TTL for the hash field to ensure expired sub-windows are cleaned up
            hexpire(key, windowSize, *Array(1000) { currentSubWindow.toString() })

            // Sum the counts for all active sub-windows
            val subWindowsToCheck = (currentSubWindow downTo (currentSubWindow - numSubWindows + 1))
            subWindowsToCheck.forEach { subWindow ->
                hget(key, subWindow.toString()) ?: 0
            }

            exec()
        }

        if (result.isEmpty()) {
            throw IllegalStateException("Empty result from Redis pipeline")
        }

        val totalCount = result.takeLast(result.size - 2).map {
            (it as String?)?.toIntOrNull() ?: 0
        }.sum()

        // Check if the total count is within the limit
        return totalCount <= limit
    }
}