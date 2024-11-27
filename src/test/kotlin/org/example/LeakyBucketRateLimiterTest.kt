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
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1)
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }
    }

    @Test
    fun `should deny requests once bucket is full`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1)
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
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1)

        // Fill the bucket
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Trigger the leak processor
        rateLimiter.leak("client-1")

        // After leakage, one request should be allowed
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request after leakage should be allowed")
            .isTrue()
    }

    @Test
    fun `should maintain independent buckets for multiple clients`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1)
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
    fun `should leak requests when triggered`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 5, 1)
        val clientId = "client-1"

        // Fill the bucket
        for (i in 1..5) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request $i should be allowed within bucket capacity")
                .isTrue()
        }

        // Exceed the limit
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse()

        // Trigger the leak processor
        rateLimiter.leak(clientId)

        // Verify that a single request can now be added
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request after manual leak should be allowed")
            .isTrue()

        // Ensure further requests still honor the remaining capacity
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond capacity after partial leak should be denied")
            .isFalse()
    }

    @Test
    fun `should correctly handle zero capacity buckets`() {
        rateLimiter = LeakyBucketRateLimiter(jedis, 0, 1)
        val clientId = "client-1"

        // Any request should be denied for a zero-capacity bucket
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request for zero-capacity bucket should always be denied")
            .isFalse()
    }
}