package com.ratelimiter.impl;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingWindowRateLimiterTest {

    private final AtomicLong fakeNanos = new AtomicLong();
    private final SlidingWindowRateLimiter limiter =
            new SlidingWindowRateLimiter(3, Duration.ofSeconds(1), fakeNanos::get);

    @Test
    void admitsUpToMaxEventsThenRejects() {
        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
        assertEquals(0.0, limiter.availablePermits());
    }

    @Test
    void oldEventsExpireAsTimeAdvances() {
        takeAll();
        fakeNanos.addAndGet(600_000_000L); // 600ms: still inside the 1s window
        assertFalse(limiter.tryAcquire());
        fakeNanos.addAndGet(600_000_000L); // 1.2s total: all three events expired
        assertTrue(limiter.tryAcquire());
        assertEquals(2.0, limiter.availablePermits());
    }

    @Test
    void windowSlidesContinuouslyRatherThanResettingInSteps() {
        takeAll();                       // t=0: three events
        fakeNanos.addAndGet(400_000_000L);
        assertFalse(limiter.tryAcquire()); // t=400ms
        fakeNanos.addAndGet(700_000_000L); // t=1100ms: the t=0 events dropped off
        assertTrue(limiter.tryAcquire());
        fakeNanos.addAndGet(100_000_000L); // t=1200ms: the t=400ms rejection never consumed a slot
        assertTrue(limiter.tryAcquire());
    }

    @Test
    void multiPermitAcquireIsAtomic() {
        assertTrue(limiter.tryAcquire(2));
        assertFalse(limiter.tryAcquire(2), "only 1 slot left");
        assertTrue(limiter.tryAcquire());
        assertEquals(0.0, limiter.availablePermits());
    }

    @Test
    void rejectedAttemptsDoNotConsumeSlots() {
        takeAll();
        for (int i = 0; i < 100; i++) {
            assertFalse(limiter.tryAcquire());
        }
        fakeNanos.addAndGet(1_500_000_000L);
        assertEquals(3.0, limiter.availablePermits());
    }

    @Test
    void retryAfterPointsAtTheOldestEventExpiring() {
        takeAll();
        fakeNanos.addAndGet(300_000_000L); // t=300ms
        var result = limiter.tryAcquireWithDetail();
        assertFalse(result.allowed());
        // oldest event at t=0 expires at t=1000ms -> ~700ms from now
        assertEquals(700, result.retryAfterMillis());
    }

    private void takeAll() {
        for (int i = 0; i < 3; i++) {
            assertTrue(limiter.tryAcquire());
        }
    }
}
