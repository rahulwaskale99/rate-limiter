package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimiter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real (non-deterministic-time) concurrency tests. The invariant being
 * verified: under heavy contention the limiters never over-admit and never
 * lose admitted permits — i.e. the CAS loops are linearizable.
 */
class ConcurrencyStressTest {

    private static final int THREADS = 1000;
    private static final int LIMIT = 100;

    @Test
    void tokenBucketNeverOverAdmitsUnderContention() throws Exception {
        RateLimiter limiter = new TokenBucketRateLimiter(LIMIT, 0); // fixed pool, no refill
        assertExactlyLimitAdmitted(limiter);
    }

    @Test
    void leakyBucketNeverOverAdmitsUnderContention() throws Exception {
        RateLimiter limiter = new LeakyBucketRateLimiter(LIMIT, 0); // fixed pool, no drain
        assertExactlyLimitAdmitted(limiter);
    }

    @Test
    void slidingWindowNeverOverAdmitsUnderContention() throws Exception {
        RateLimiter limiter = new SlidingWindowRateLimiter(LIMIT, Duration.ofHours(1));
        assertExactlyLimitAdmitted(limiter);
    }

    @Test
    void fixedWindowNeverOverAdmitsUnderContention() throws Exception {
        RateLimiter limiter = new FixedWindowRateLimiter(LIMIT, Duration.ofHours(1));
        assertExactlyLimitAdmitted(limiter);
    }

    /**
     * Fires {@value #THREADS} virtual threads at the limiter simultaneously and
     * asserts exactly {@value #LIMIT} of the {@value #THREADS} attempts are
     * admitted. Fewer would mean lost updates; more would mean over-admission.
     */
    private void assertExactlyLimitAdmitted(RateLimiter limiter) throws Exception {
        AtomicLong accepted = new AtomicLong();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch start = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < THREADS; i++) {
                executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    if (limiter.tryAcquire()) {
                        accepted.incrementAndGet();
                    }
                    return null;
                });
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS), "all threads must line up");
            start.countDown(); // release them all at once for maximum contention
        }

        assertEquals(LIMIT, accepted.get(),
                "exactly " + LIMIT + " permits must be admitted under contention");
    }
}
