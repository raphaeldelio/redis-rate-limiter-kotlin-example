package io.redis

import com.redis.testcontainers.RedisContainer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class SlidingWindowCounterRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: SlidingWindowCounterRateLimiter

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
        rateLimiter = SlidingWindowCounterRateLimiter(jedis, 5, 10, 1)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }
    }

    @Test
    fun `should deny requests once limit is exceeded`() {
        // First, allow requests up to the limit
        rateLimiter = SlidingWindowCounterRateLimiter(jedis, 5, 60, 1)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Now, attempt one more request which should exceed the limit
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()
    }

    @Test
    fun `should allow requests again after sliding window resets`() {
        val limit = 5
        val clientId = "client-1"
        val windowSize = 2L
        val subWindowSize = 1L
        rateLimiter = SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize)


        // Allow requests up to the limit
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait for the sliding window period to expire
        Thread.sleep((windowSize + 1) * 1000) // Sleep for slightly more than window size

        // After the window resets, requests should be allowed again
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request after window reset should be allowed")
            .isTrue()
    }

    @Test
    fun `should handle multiple clients independently`() {
        val limit = 5
        val clientId = "client-1"
        val clientId2 = "client-2"
        val windowSize = 10L
        val subWindowSize = 1L
        rateLimiter = SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize)

        // Send requests from client 1 up to the limit
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Client 1 request $i should be allowed")
                .isTrue()
        }

        // Exceed the limit for client 1
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Client 1 request beyond limit should be denied")
            .isFalse()

        // Requests from client 2 should still be allowed since it’s a separate key
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId2))
                .withFailMessage("Client 2 request $i should be allowed")
                .isTrue()
        }
    }

    @Test
    fun `should allow requests again gradually in sliding window`() {
        val limit = 3
        val windowSize = 4L
        val subWindowSize = 1L
        val clientId = "client-1"
        rateLimiter = SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize)

        // Make `limit` number of requests within the window
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
            Thread.sleep(1000)
        }

        // Exceed the limit immediately
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait for 2 seconds, enough for the oldest request to fall out of the window in a sliding approach
        Thread.sleep(2000)

        // If using a sliding window, the next request should now be allowed
        // In a fixed window, this would still be denied until the entire window resets
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request should be allowed in a sliding window")
            .isTrue()
    }
}