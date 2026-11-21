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
class LeakyBucketRateLimiterTest {

    private final AtomicLong fakeNanos = new AtomicLong();
    private final LeakyBucketRateLimiter limiter =
            new LeakyBucketRateLimiter(10, 5, fakeNanos::get);

    @Test
    void admitsBurstUpToCapacityThenRejects() {
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire(), "fill " + i + " should succeed");
        }
        assertFalse(limiter.tryAcquire(), "bucket is full");
        assertEquals(0.0, limiter.availablePermits());
    }

    @Test
    void drainsContinuouslyWithElapsedTime() {
        fillBucket();
        // 200ms at 5 permits/sec drain = exactly one slot freed
        fakeNanos.addAndGet(200_000_000L);
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }

    @Test
    void partialDrainIsNotEnoughForAFullPermit() {
        fillBucket();
        fakeNanos.addAndGet(100_000_000L); // level 9.5
        assertFalse(limiter.tryAcquire(), "0.5 of freed space cannot cover a 1-permit acquire");
        fakeNanos.addAndGet(100_000_000L); // level 9.0
        assertTrue(limiter.tryAcquire());
    }

    @Test
    void multiPermitAcquireIsAtomic() {
        assertTrue(limiter.tryAcquire(3));
        assertTrue(limiter.tryAcquire(3));
        assertTrue(limiter.tryAcquire(3)); // level 9
        assertFalse(limiter.tryAcquire(3), "only 1 slot of space left");
        assertTrue(limiter.tryAcquire(), "single slot still available");
    }

    @Test
    void requestForMoreThanCapacityAlwaysFails() {
        assertFalse(limiter.tryAcquire(11));
    }

    @Test
    void retryAfterEstimateReflectsDrainRate() {
        fillBucket();
        var result = limiter.tryAcquireWithDetail();
        assertFalse(result.allowed());
        // 1 missing slot of space at 5 slots/sec drain = 200ms
        assertEquals(200, result.retryAfterMillis());
    }

    @Test
    void drainNeverGoesNegative() {
        fillBucket();
        fakeNanos.addAndGet(6_000_000_000L); // would drain 30 permits
        assertEquals(10.0, limiter.availablePermits(), "floored at an empty bucket");
    }

    @Test
    void rejectedAttemptsDoNotRaiseTheLevel() {
        fillBucket();
        for (int i = 0; i < 100; i++) {
            assertFalse(limiter.tryAcquire());
        }
        fakeNanos.addAndGet(200_000_000L);
        assertTrue(limiter.tryAcquire(), "rejections must not consume drain capacity");
    }

    @Test
    void zeroDrainRateBehavesAsFixedPool() {
        LeakyBucketRateLimiter fixed = new LeakyBucketRateLimiter(5, 0, fakeNanos::get);
        for (int i = 0; i < 5; i++) {
            assertTrue(fixed.tryAcquire());
        }
        var result = fixed.tryAcquireWithDetail();
        assertFalse(result.allowed());
        assertEquals(-1, result.retryAfterMillis(), "no drain scheduled, no retry estimate");
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new LeakyBucketRateLimiter(0, 5));
        assertThrows(IllegalArgumentException.class, () -> new LeakyBucketRateLimiter(10, -1));
        assertThrows(IllegalArgumentException.class, () -> limiter.tryAcquire(0));
    }

    /** Fills the bucket to capacity at t=0. */
    private void fillBucket() {
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.tryAcquire());
        }
    }
}
