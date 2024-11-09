package org.example

import com.redis.testcontainers.RedisContainer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class SlidingWindowRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: SlidingWindowRateLimiter

    @BeforeEach
    fun setup() {
        jedis = Jedis(redisContainer.host, redisContainer.firstMappedPort)
        jedis.flushAll()
    }

    @AfterEach
    fun tearDown() {
        jedis.close()
    }

    @Test
    fun `should allow requests within limit`() {
        rateLimiter = SlidingWindowRateLimiter(jedis, 5, 10)
        for (i in 1..5) {
            assertTrue(rateLimiter.isAllowed("client-1"), "Request $i should be allowed")
        }
    }

   @Test
    fun `should deny requests once limit is exceeded`() {
        // First, allow requests up to the limit
       rateLimiter = SlidingWindowRateLimiter(jedis, 5, 60)
       for (i in 1..5) {
            assertTrue(rateLimiter.isAllowed("client-1"), "Request $i should be allowed")
        }

        // Now, attempt one more request which should exceed the limit
        assertFalse(rateLimiter.isAllowed("client-1"), "Request beyond limit should be denied")
    }

    @Test
    fun `should allow requests again after sliding window resets`() {
        val limit = 5
        val clientId = "client-1"
        val windowSize = 1L
        rateLimiter = SlidingWindowRateLimiter(jedis, limit, windowSize)


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
        val limit = 5
        val clientId = "client-1"
        val clientId2 = "client-2"
        val windowSize = 10L
        rateLimiter = SlidingWindowRateLimiter(jedis, limit, windowSize)

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