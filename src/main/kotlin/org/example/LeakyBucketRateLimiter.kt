package org.example

import redis.clients.jedis.Jedis

class LeakyBucketRateLimiter(
    private val jedis: Jedis,
    private val capacity: Int,
    private val leakRate: Long
) {

    fun isAllowed(clientId: String): Boolean {
        val keyCount = "rate_limit:$clientId:count"
        val requestCount = (jedis.get(keyCount)?.toIntOrNull() ?: 0)
        val isAllowed = requestCount < capacity
        if (isAllowed) jedis.incr(keyCount)
        return isAllowed
    }

    fun leak(clientId: String) {
        val keyCount = "rate_limit:$clientId:count"
        val requestCount = (jedis.get(keyCount)?.toIntOrNull() ?: 0)
        jedis.setex(keyCount, leakRate, (requestCount - 1).toString());
    }
}