package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.LongSupplier;

/**
 * Sliding window "log" limiter: remembers the exact timestamp of each accepted
 * event and admits a new event only if fewer than {@code maxEvents} accepted
 * events fall inside the trailing {@code window}. Unlike the fixed window,
 * there are no burst boundaries — any window of that duration seen by callers
 * contains at most {@code maxEvents} events.
 *
 * <p><strong>Concurrency design.</strong> Unlike {@link TokenBucketRateLimiter},
 * this implementation guards its deque with a {@code synchronized} block. The
 * window accounting needs a consistent read-evict-count-append sequence over
 * multiple entries, which a single CAS cell cannot express. The critical
 * section is tiny (a few deque operations), so under contention this behaves
 * like a short lock, not a bottleneck — a deliberate trade of throughput for
 * exactness, and the reason the token bucket remains the default choice.
 *
 * <p>Cost is O(window events) per acquire in the worst case (eviction scan),
 * so very large windows with very high limits favor the token bucket.
 *
 * <p>The {@code nanoTimeSupplier} constructor parameter exists purely for
 * deterministic testing.
 */
public final class SlidingWindowRateLimiter implements RateLimiter {

    private static final double NANOS_PER_MILLI = 1_000_000.0;

    private final int maxEvents;
    private final long windowNanos;
    private final LongSupplier nanoTimeSupplier;
    private final ArrayDeque<Long> acceptedTimestamps;

    public SlidingWindowRateLimiter(int maxEvents, Duration window) {
        this(maxEvents, window, System::nanoTime);
    }

    public SlidingWindowRateLimiter(int maxEvents, Duration window, LongSupplier nanoTimeSupplier) {
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be > 0, got " + maxEvents);
        }
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be a positive Duration");
        }
        this.maxEvents = maxEvents;
        this.windowNanos = window.toNanos();
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.acceptedTimestamps = new ArrayDeque<>(maxEvents);
    }

    @Override
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be > 0, got " + permits);
        }
        if (permits > maxEvents) {
            return false; // can never fit inside one window
        }
        long now = nanoTimeSupplier.getAsLong();
        synchronized (acceptedTimestamps) {
            evictExpired(now);
            if (acceptedTimestamps.size() + permits <= maxEvents) {
                for (int i = 0; i < permits; i++) {
                    acceptedTimestamps.addLast(now);
                }
                return true;
            }
            return false;
        }
    }

    @Override
    public RateLimitResult tryAcquireWithDetail() {
        long now = nanoTimeSupplier.getAsLong();
        synchronized (acceptedTimestamps) {
            evictExpired(now);
            int used = acceptedTimestamps.size();
            if (used < maxEvents) {
                acceptedTimestamps.addLast(now);
                return RateLimitResult.allowed(maxEvents - used - 1);
            }
            // The oldest accepted event determines when a slot frees up.
            long oldest = acceptedTimestamps.peekFirst();
            long retryAfter = Math.max(1, (long) Math.ceil((oldest + windowNanos - now) / NANOS_PER_MILLI));
            return RateLimitResult.rejected(maxEvents - used, retryAfter);
        }
    }

    @Override
    public double availablePermits() {
        long now = nanoTimeSupplier.getAsLong();
        synchronized (acceptedTimestamps) {
            evictExpired(now);
            return maxEvents - acceptedTimestamps.size();
        }
    }

    /** Must be called while holding the monitor. */
    private void evictExpired(long now) {
        long cutoff = now - windowNanos;
        Iterator<Long> it = acceptedTimestamps.iterator();
        while (it.hasNext() && it.next() <= cutoff) {
            it.remove();
        }
    }
}
