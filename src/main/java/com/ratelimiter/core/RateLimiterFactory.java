package com.ratelimiter.core;

import com.ratelimiter.impl.FixedWindowRateLimiter;
import com.ratelimiter.impl.LeakyBucketRateLimiter;
import com.ratelimiter.impl.SlidingWindowRateLimiter;
import com.ratelimiter.impl.TokenBucketRateLimiter;

import java.time.Duration;

/**
 * Convenience factory for the built-in limiter implementations.
 */
public final class RateLimiterFactory {

    private RateLimiterFactory() {
    }

    /**
     * Token bucket: allows bursts up to {@code capacity}, then refills smoothly
     * at {@code refillPerSecond} permits/second. The usual choice for API
     * throttling.
     */
    public static RateLimiter tokenBucket(double capacity, double refillPerSecond) {
        return new TokenBucketRateLimiter(capacity, refillPerSecond);
    }

    /**
     * Leaky bucket: absorbs bursts up to {@code capacity} and drains at a
     * constant {@code leakPerSecond} permits/second, so the downstream sees a
     * smooth outflow. Use when the consumer cannot tolerate bursts.
     */
    public static RateLimiter leakyBucket(double capacity, double leakPerSecond) {
        return new LeakyBucketRateLimiter(capacity, leakPerSecond);
    }

    /**
     * Sliding window log: at most {@code maxEvents} events in any window of
     * {@code window} duration, computed exactly by remembering event timestamps.
     * No burst boundary effects, higher bookkeeping cost.
     */
    public static RateLimiter slidingWindow(int maxEvents, Duration window) {
        return new SlidingWindowRateLimiter(maxEvents, window);
    }

    /**
     * Fixed window: at most {@code maxEvents} events per aligned window of
     * {@code window} duration. Cheapest, but allows up to 2x {@code maxEvents}
     * across a window boundary.
     */
    public static RateLimiter fixedWindow(int maxEvents, Duration window) {
        return new FixedWindowRateLimiter(maxEvents, window);
    }
}
