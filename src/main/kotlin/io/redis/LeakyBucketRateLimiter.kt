package io.redis

import redis.clients.jedis.Jedis

class LeakyBucketRateLimiter(
    private val jedis: Jedis,
    private val capacity: Int,
    private val leakRate: Double // Number of requests "leaked" per second
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val keyCount = "rate_limit:$clientId:count"
        val keyLastLeak = "rate_limit:$clientId:lastLeak"

        val result = jedis.multi().run {
            // Retrieve the last leak time and current request count in a transaction
            get(keyLastLeak)
            get(keyCount)
            exec()
        }

        val lastLeakTime = (result[0] as String?)?.toLongOrNull() ?: currentTime
        val requestCount = (result[1] as String?)?.toIntOrNull() ?: 0

        // Calculate time since the last leak and requests to remove
        val timeElapsedMs = currentTime - lastLeakTime
        val timeElapsedSecs = timeElapsedMs / 1000.0
        val requestsToLeak = (timeElapsedSecs * leakRate).toInt()
        val updatedRequestCount = maxOf(0, requestCount - requestsToLeak) // Ensure count is non-negative

        // Check if the request can be allowed
        val isAllowed = updatedRequestCount < capacity

        jedis.multi().run {
            // Update request count and last leak time
            set(keyCount, if (isAllowed) (updatedRequestCount + 1).toString() else updatedRequestCount.toString())
            set(keyLastLeak, currentTime.toString())
            exec()
        }

        return isAllowed
    }
}