package com.ratelimiter.impl;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic tests: the limiter reads time only through the injected
 * supplier, so tests advance a fake clock instead of sleeping.
 */
class TokenBucketRateLimiterTest {

    private final AtomicLong fakeNanos = new AtomicLong();
    private final TokenBucketRateLimiter limiter =
            new TokenBucketRateLimiter(10, 5, fakeNanos::get);

    @Test
    void allowsBurstUpToCapacityThenRejects() {
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire(), "acquisition " + i + " should succeed");
        }
        assertFalse(limiter.tryAcquire(), "bucket is exhausted");
        assertEquals(0.0, limiter.availablePermits());
    }

    @Test
    void refillsContinuouslyWithElapsedTime() {
        exhaustBucket();
        // 200ms at 5 permits/sec = exactly 1 permit
        fakeNanos.addAndGet(200_000_000L);
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    void partialRefillIsNotEnoughForAFullPermit() {
        exhaustBucket();
        fakeNanos.addAndGet(100_000_000L); // 0.5 permits accrued
        assertFalse(limiter.tryAcquire(), "0.5 permits cannot cover a 1-permit acquire");
        fakeNanos.addAndGet(100_000_000L); // now 1.0
        assertTrue(limiter.tryAcquire());
    }

    @Test
    void multiPermitAcquireIsAtomic() {
        assertTrue(limiter.tryAcquire(3));
        assertTrue(limiter.tryAcquire(3));
        assertTrue(limiter.tryAcquire(3));
        assertFalse(limiter.tryAcquire(3), "only 1 permit left, all-or-nothing must fail");
        assertTrue(limiter.tryAcquire(), "single permit still available");
    }

    @Test
    void requestForMoreThanCapacityAlwaysFails() {
        assertFalse(limiter.tryAcquire(11));
    }

    @Test
    void retryAfterEstimateReflectsRefillRate() {
        exhaustBucket();
        var result = limiter.tryAcquireWithDetail();
        assertFalse(result.allowed());
        // 1 missing permit at 5 permits/sec = 200ms
        assertEquals(200, result.retryAfterMillis());
    }

    @Test
    void refillNeverExceedsCapacity() {
        exhaustBucket();
        fakeNanos.addAndGet(6_000_000_000L); // 6s at 5/sec would be 30 permits
        assertEquals(10.0, limiter.availablePermits(), "capped at bucket capacity");
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(0, 5));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(10, -1));
        assertThrows(IllegalArgumentException.class, () -> limiter.tryAcquire(0));
    }

    /** Burns all 10 permits at t=0. */
    private void exhaustBucket() {
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire());
        }
    }
}
