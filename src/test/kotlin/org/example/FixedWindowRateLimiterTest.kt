package org.example

import com.redis.testcontainers.RedisContainer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class FixedWindowRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: FixedWindowRateLimiter

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
        rateLimiter = FixedWindowRateLimiter(jedis, 5, 10)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }
    }

   @Test
    fun `should deny requests once limit is exceeded`() {
       // First, allow requests up to the limit
       rateLimiter = FixedWindowRateLimiter(jedis, 5, 60)
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
    fun `should allow requests again after fixed window resets`() {
        val limit = 5
        val clientId = "client-1"
        val windowSize = 1L
        rateLimiter = FixedWindowRateLimiter(jedis, limit, windowSize)


        // Allow requests up to the limit
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()        }

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
        rateLimiter = FixedWindowRateLimiter(jedis, limit, windowSize)

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
        val windowSize = 5L
        val clientId = "client-1"
        rateLimiter = FixedWindowRateLimiter(jedis, limit, windowSize)

        // Make `limit` number of requests within the window
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within limit")
                .isTrue()
            Thread.sleep(1000) // Spread requests 1 second apart
        }

        // Exceed the limit immediately
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait for 1 second, enough for the oldest request to fall out of the window in a sliding approach
        Thread.sleep(1000)

        // If using a sliding window, the next request should now be allowed
        // In a fixed window, this would still be denied until the entire window resets
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request should still be denied in a fixed window")
            .isFalse()
    }

    @Test
    fun `should deny additional requests until fixed window resets`() {
        val limit = 3
        val windowSize = 5L
        val clientId = "client-1"
        rateLimiter = FixedWindowRateLimiter(jedis, limit, windowSize)

        // Make `limit` number of requests within the window
        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within limit")
                .isTrue()
        }

        // Exceed the limit within the same window
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond limit should be denied")
            .isFalse()

        // Wait just a bit (less than the window size)
        Thread.sleep(2500) // 2.5 seconds, which is half of the window

        // Additional request should still be denied since the fixed window hasn’t fully reset
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request should still be denied within the same fixed window")
            .isFalse()

        // Wait for the remaining window period to expire
        Thread.sleep(2500) // Wait for the remaining 2.5 seconds

        // After the fixed window resets, requests should be allowed again
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request should be allowed after fixed window reset")
            .isTrue()
    }

    @Test
    fun `test rate limit - denied requests are not counted`() {
        val limit = 3
        val windowSize = 5L
        val clientId = "client-1"
        rateLimiter = FixedWindowRateLimiter(jedis, limit, windowSize)

        for (i in 1..limit) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Submit one more request, which should be denied
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("This request should be denied")
            .isFalse()

        // Verify the number of entries in Redis does not include the denied request
        val key = "rate_limit:$clientId"
        val requestCount = jedis.get(key).toInt() // Get the count of requests in the sorted set
        assertThat(limit)
            .withFailMessage("The count ($requestCount) should be equal to the limit ($limit), not counting the denied request")
            .isEqualTo(requestCount)
    }
}