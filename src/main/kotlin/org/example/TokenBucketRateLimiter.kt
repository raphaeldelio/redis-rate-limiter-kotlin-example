package org.example

import redis.clients.jedis.Jedis

class TokenBucketRateLimiter(
    private val jedis: Jedis,
    private val limit: Int,
    private val refillRate: Double
) {

    fun isAllowed(clientId: String): Boolean {
        val currentTime = System.currentTimeMillis()
        val keyCount = "rate_limit:$clientId:count"
        val keyLastRefill = "rate_limit:$clientId:lastRefill"
        val bucketCapacity = limit

        val result = jedis.multi().run {
            // Retrieve the last refill time and current token count in a transaction
            get(keyLastRefill)
            get(keyCount)
            exec()
        }

        val lastRefillTime = (result[0] as String?)?.toLongOrNull() ?: currentTime
        val tokenCount = (result[1] as String?)?.toIntOrNull() ?: bucketCapacity

        // Calculate time since last refill and tokens to add
        val timeElapsedMs = currentTime - lastRefillTime
        val timeElapsedSecs = timeElapsedMs / 1000.0
        val tokensToAdd = (timeElapsedSecs * refillRate).toInt()
        val newTokenCount = minOf(bucketCapacity, tokenCount + tokensToAdd) // Making sure we don't exceed bucket's capacity

        return jedis.multi().run {
            // Update token count and last refill time if tokens were refilled
            set(keyCount, newTokenCount.toString())
            set(keyLastRefill, currentTime.toString())

            val allowed = if (newTokenCount > 0) {
                decr(keyCount)
                true
            } else {
                false
            }

            exec()
            allowed
        }
    }
}