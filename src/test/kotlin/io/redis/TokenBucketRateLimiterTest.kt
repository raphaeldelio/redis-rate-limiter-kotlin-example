package io.redis

import com.redis.testcontainers.RedisContainer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class TokenBucketRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: TokenBucketRateLimiter

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
    fun `should allow requests within bucket capacity`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 5, 1.0)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }
    }

    @Test
    fun `should deny requests once bucket is empty`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 5, 1.0)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Now, attempt one more request, which should exceed the bucket capacity
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()
    }

    @Test
    fun `should allow requests again after tokens are refilled`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 5, 1.0)  // 5 tokens in bucket, refilled at 1 token per second

        // Use up all tokens
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait for the bucket to refill one token
        Thread.sleep(1000)

        // After refill, one request should be allowed
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request after token refill should be allowed")
            .isTrue()
    }

    @Test
    fun `should handle multiple clients independently`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 5, 1.0)  // Each client gets 5 tokens, refilled at 1 token per second
        val clientId1 = "client-1"
        val clientId2 = "client-2"

        // Use up all tokens for client 1
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request $i should be allowed")
                .isTrue()
        }

        // Exceed the limit for client 1
        assertThat(rateLimiter.isAllowed(clientId1))
            .withFailMessage("Client 1 request beyond bucket capacity should be denied")
            .isFalse()

        // Client 2 should still be allowed as it has its own bucket
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed(clientId2))
                .withFailMessage("Client 2 request $i should be allowed")
                .isTrue()
        }
    }

    @Test
    fun `should allow bursts up to bucket capacity`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 10, 2.0)  // 10 tokens max, refills at 2 tokens per second
        val clientId = "client-1"

        // Allow requests up to bucket capacity in a burst
        for (i in 1..10) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Burst request $i should be allowed up to bucket capacity")
                .isTrue()
        }

        // After bursting to capacity, further requests should be denied until refill
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity in burst should be denied")
            .isFalse()
    }

    @Test
    fun `should refill tokens gradually and allow requests over time`() {
        rateLimiter = TokenBucketRateLimiter(jedis, 5, 1.0)  // 5 tokens max, refills at 1 token per second
        val clientId = "client-1"

        // Use up all tokens in a burst
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait for 2 seconds to refill 2 tokens
        Thread.sleep(2000)

        // Two requests should now be allowed after gradual refill
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request after partial refill should be allowed")
            .isTrue()
        
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Second request after partial refill should be allowed")
            .isTrue()

        // Further requests should be denied until more tokens are refilled
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond available tokens should be denied")
            .isFalse()
    }

    @Test
    fun `should refill tokens up to capacity without exceeding it`() {
        val capacity = 3
        val refillRatePerSecond = 2.0  // 2 token per second
        val clientId = "client-1"
        rateLimiter = TokenBucketRateLimiter(jedis, capacity, refillRatePerSecond)

        // Use up all tokens to empty the bucket
        for (i in 1..capacity) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within initial bucket capacity")
                .isTrue()
        }

        // Verify that further requests are denied as the bucket is empty
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait enough time to refill tokens potentially over capacity
        Thread.sleep(3000)

        // Verify that bucket has refilled only up to the original capacity (3 tokens max)
        for (i in 1..capacity) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed as bucket refills up to capacity")
                .isTrue()
        }

        // Ensure additional requests are denied beyond the bucket's capacity
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()
    }

    @Test
    fun `test rate limit - denied requests are not counted`() {
        val capacity = 3
        val refillRatePerSecond = 0.5  // 1 token every two seconds
        val clientId = "client-1"
        rateLimiter = TokenBucketRateLimiter(jedis, capacity, refillRatePerSecond)

        for (i in 1..capacity) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Submit one more request, which should be denied
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("This request should be denied")
            .isFalse()

        // Verify the number of entries in Redis does not include the denied request
        val key = "rate_limit:$clientId:count"
        val requestCount = jedis.get(key).toInt() // Get the count of requests in the sorted set
        assertThat(requestCount)
            .withFailMessage("The count ($requestCount) should be equal to zero, not counting the denied request (negative number)")
            .isEqualTo(0)
    }
}