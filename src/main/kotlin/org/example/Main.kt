package org.example

import redis.clients.jedis.Jedis

fun main() {
    val jedis = Jedis("localhost", 6379)
    val rateLimiter = SlidingWindowRateLimiter(jedis, limit = 100, windowSize = 60)

    val clientId = "client_123"
    if (rateLimiter.isAllowed(clientId)) {
        println("Request allowed.")
    } else {
        println("Rate limit exceeded. Try again later.")
    }
    jedis.close()
}