package org.example

import com.redis.testcontainers.RedisContainer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis
import java.time.LocalDateTime

class SlidingWindowLogHashAlternativeRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: SlidingWindowLogRateLimiter

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
        rateLimiter = SlidingWindowLogRateLimiter(jedis, 5, 10)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowedHashAlternative("client-1"))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }
    }

    @Test
    fun `should deny requests once limit is exceeded`() {
        // First, allow requests up to the limit
        rateLimiter = SlidingWindowLogRateLimiter(jedis, 5, 60)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowedHashAlternative("client-1"))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Now, attempt one more request which should exceed the limit
        assertThat(rateLimiter.isAllowedHashAlternative("client-1"))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()
    }

    @Test
    fun `should allow requests again after sliding window resets`() {
        val limit = 5
        val clientId = "client-1"
        val windowSize = 1L
        rateLimiter = SlidingWindowLogRateLimiter(jedis, limit, windowSize)


        // Allow requests up to the limit
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowedHashAlternative(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait for the sliding window period to expire
        Thread.sleep((windowSize + 1) * 1000) // Sleep for slightly more than window size

        // After the window resets, requests should be allowed again
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("Request after window reset should be allowed")
            .isTrue()
    }

    @Test
    fun `should handle multiple clients independently`() {
        val limit = 5
        val clientId = "client-1"
        val clientId2 = "client-2"
        val windowSize = 10L
        rateLimiter = SlidingWindowLogRateLimiter(jedis, limit, windowSize)

        // Send requests from client 1 up to the limit
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowedHashAlternative(clientId))
                .withFailMessage("Client 1 request $i should be allowed")
                .isTrue()
        }

        // Exceed the limit for client 1
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("Client 1 request beyond limit should be denied")
            .isFalse()

        // Requests from client 2 should still be allowed since it’s a separate key
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowedHashAlternative(clientId2))
                .withFailMessage("Client 2 request $i should be allowed")
                .isTrue()
        }
    }

    @Test
    fun `should allow requests again gradually in sliding window`() {
        val limit = 3
        val windowSize = 4L
        val clientId = "client-1"
        rateLimiter = SlidingWindowLogRateLimiter(jedis, limit, windowSize)

        // Make `limit` number of requests within the window
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowedHashAlternative(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
            Thread.sleep(1000)
        }

        // Exceed the limit immediately
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait for 2 seconds, enough for the oldest request to fall out of the window in a sliding approach
        Thread.sleep(2000)

        // If using a sliding window, the next request should now be allowed
        // In a fixed window, this would still be denied until the entire window resets
        println(LocalDateTime.now())
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("Request should be allowed in a sliding window")
            .isTrue()
    }

    @Test
    fun `test rate limit - denied requests are not counted`() {
        val limit = 3
        val windowSize = 4L
        val clientId = "client-1"
        rateLimiter = SlidingWindowLogRateLimiter(jedis, limit, windowSize)

        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowedHashAlternative(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Submit one more request, which should be denied
        assertThat(rateLimiter.isAllowedHashAlternative(clientId))
            .withFailMessage("This request should be denied")
            .isFalse()

        // Verify the number of entries in Redis does not include the denied request
        val key = "rate_limit:$clientId"
        val requestCount = jedis.hlen(key) // Get the count of requests in the sorted set
        assertThat(limit.toLong())
            .withFailMessage("The count ($requestCount) should be equal to the limit ($limit), not counting the denied request")
            .isEqualTo(requestCount)
    }
}