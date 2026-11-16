package com.ratelimiter.impl;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedWindowRateLimiterTest {

    private final AtomicLong fakeNanos = new AtomicLong();
    private final FixedWindowRateLimiter limiter =
            new FixedWindowRateLimiter(5, Duration.ofSeconds(1), fakeNanos::get);

    @Test
    void admitsUpToMaxEventsPerWindow() {
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire());
        }
        assertFalse(limiter.tryAcquire());
        assertEquals(0.0, limiter.availablePermits());
    }

    @Test
    void counterResetsWhenWindowRollsOver() {
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire());
        }
        assertFalse(limiter.tryAcquire());
        fakeNanos.addAndGet(1_000_000_000L); // next window
        assertTrue(limiter.tryAcquire());
        assertEquals(4.0, limiter.availablePermits());
    }

    @Test
    void unusedCapacityDoesNotCarryOver() {
        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        fakeNanos.addAndGet(1_000_000_000L);
        // New window starts from zero, not from the unused 3 permits.
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.tryAcquire());
        }
        assertFalse(limiter.tryAcquire());
    }

    @Test
    void retryAfterPointsAtNextWindow() {
        for (int i = 0; i < 5; i++) {
            limiter.tryAcquire();
        }
        fakeNanos.addAndGet(400_000_000L); // t=400ms
        var result = limiter.tryAcquireWithDetail();
        assertFalse(result.allowed());
        assertEquals(600, result.retryAfterMillis()); // window ends at t=1000ms
    }

    @Test
    void boundaryExactlyAtWindowLengthRollsOver() {
        assertTrue(limiter.tryAcquire());
        fakeNanos.addAndGet(1_000_000_000L);
        var result = limiter.tryAcquireWithDetail();
        assertTrue(result.allowed());
        assertEquals(4.0, result.remainingPermits());
    }
}
