package org.example

import com.redis.testcontainers.RedisContainer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class LeakyBucketRateLimiterTest {

    companion object {
        private val redisContainer = RedisContainer("redis:latest").apply {
            withExposedPorts(6379)
            start()
        }
    }

    private lateinit var jedis: Jedis
    private lateinit var rateLimiter: LeakyBucketRateLimiter

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
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1.0)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }
    }

    @Test
    fun `should deny requests once bucket is full`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1.0)
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
    fun `should allow requests again after leakage`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1.0)
        // Fill in the bucket
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait for the bucket to leak one request
        Thread.sleep(1000)

        // After leakage, one request should be allowed
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request after leakage should be allowed")
            .isTrue()
    }

    @Test
    fun `should maintain independent buckets for multiple clients`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1.0)
        val clientId1 = "client-1"
        val clientId2 = "client-2"

        // Fill the bucket for client 1
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
        rateLimiter = LeakyBucketRateLimiter(jedis, 10, 2.0)  // 10 tokens max, refills at 2 tokens per second
        val clientId = "client-1"

        // Allow requests up to bucket capacity in a burst
        for (i in 1..10) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Burst request $i should be allowed up to bucket capacity")
                .isTrue()
        }

        // After bursting to capacity, further requests should be denied until leakage
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity in burst should be denied")
            .isFalse()
    }

    @Test
    fun `should leak requests gradually and allow requests over time`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1.0)  // 5 tokens max, refills at 1 token per second
        val clientId = "client-1"

        // Fill the bucket in burst
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait for 2 seconds to leak 2 requests
        Thread.sleep(2000)

        // Two requests should now be allowed after gradual leakage
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request after partial refill should be allowed")
            .isTrue()

        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Second request after partial refill should be allowed")
            .isTrue()

        // Further requests should be denied until more requests are leaked
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond available tokens should be denied")
            .isFalse()
    }

    @Test
    fun `should fill up to capacity without overflow`() {
        val capacity = 3
        val refillRatePerSecond = 2.0  // 2 token per second
        val clientId = "client-1"
        rateLimiter = LeakyBucketRateLimiter(jedis, capacity, refillRatePerSecond)

        // Use up all tokens to fill the bucket
        for (i in 1..capacity) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within initial bucket capacity")
                .isTrue()
        }

        // Verify that further requests are denied as the bucket is full
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Wait enough time to leak requests potentially over capacity
        Thread.sleep(3000)

        // Verify that bucket has leaked
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
    fun `should not count denied requests`() {
        val capacity = 3
        val leakRatePerSecond = 1.0 // 1 request leaks every second
        val clientId = "client-1"
        rateLimiter = LeakyBucketRateLimiter(jedis, capacity, leakRatePerSecond)

        // Allow requests up to the bucket's capacity
        for (i in 1..capacity) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed")
                .isTrue()
        }

        // Submit one more request, which should be denied as the bucket is full
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("This request should be denied")
            .isFalse()

        // Verify the number of requests currently held in the bucket matches the capacity
        val key = "rate_limit:$clientId:count"
        val updatedRequestCount = jedis.get(key)?.toIntOrNull() ?: 0
        assertThat(updatedRequestCount)
            .withFailMessage("The count ($updatedRequestCount) should reflect the leaked requests")
            .isEqualTo(capacity)
    }
}