package org.example

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class SlidingWindowRateLimiterTest {

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: SlidingWindowRateLimiter
    private val limit = 5        // Allow up to 5 requests
    private val windowSize = 5L // Sliding window of 60 seconds
    private val clientId = "client_test"

    @BeforeEach
    fun setup() {
        jedis = Jedis("localhost", 6379)
        rateLimiter = SlidingWindowRateLimiter(jedis, limit, windowSize)

        // Clear any previous data for this client in Redis
        jedis.del("rate_limit:$clientId")
    }

    @Test
    fun `should allow requests within limit`() {
        for (i in 1..limit) {
            assertTrue(rateLimiter.isAllowed(clientId), "Request $i should be allowed")
        }
    }

    @Test
    fun `should deny requests once limit is exceeded`() {
        // First, allow requests up to the limit
        for (i in 1..limit) {
            assertTrue(rateLimiter.isAllowed(clientId), "Request $i should be allowed")
        }

        // Now, attempt one more request which should exceed the limit
        assertFalse(rateLimiter.isAllowed(clientId), "Request beyond limit should be denied")
    }

    @Test
    fun `should allow requests again after sliding window resets`() {
        // Allow requests up to the limit
        for (i in 1..limit) {
            assertTrue(rateLimiter.isAllowed(clientId), "Request $i should be allowed")
        }

        // Exceed the limit
        assertFalse(rateLimiter.isAllowed(clientId), "Request beyond limit should be denied")

        // Wait for the sliding window period to expire
        Thread.sleep((windowSize + 1) * 1000) // Sleep for slightly more than window size

        // After the window resets, requests should be allowed again
        assertTrue(rateLimiter.isAllowed(clientId), "Request after window reset should be allowed")
    }

    @Test
    fun `should handle multiple clients independently`() {
        val clientId2 = "client_test_2"

        // Send requests from client 1 up to the limit
        for (i in 1..limit) {
            assertTrue(rateLimiter.isAllowed(clientId), "Client 1 request $i should be allowed")
        }

        // Exceed the limit for client 1
        assertFalse(rateLimiter.isAllowed(clientId), "Client 1 request beyond limit should be denied")

        // Requests from client 2 should still be allowed since it’s a separate key
        for (i in 1..limit) {
            assertTrue(rateLimiter.isAllowed(clientId2), "Client 2 request $i should be allowed")
        }
    }
}