package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Fixed window limiter: at most {@code maxEvents} events per
 * {@code window} duration. The first window starts when the limiter is
 * created; each later window starts lazily on the first acquire after the
 * previous window expires. This is the cheapest of the four algorithms
 * (two fields of state, one CAS), but it allows a burst of {@code maxEvents}
 * at the end of one window followed immediately by {@code maxEvents} more at
 * the start of the next — up to 2x the limit across a boundary.
 *
 * <p><strong>Concurrency design.</strong> A single {@link AtomicReference} to
 * an immutable {@code WindowState(startNanos, count)} cell, updated with the
 * same CAS retry loop as the token bucket. Window rollover happens lazily on
 * the first acquire after the boundary: whichever thread wins that CAS resets
 * the counter for the new window.
 *
 * <p>The {@code nanoTimeSupplier} constructor parameter exists purely for
 * deterministic testing.
 */
public final class FixedWindowRateLimiter implements RateLimiter {

    private static final double NANOS_PER_MILLI = 1_000_000.0;

    private final int maxEvents;
    private final long windowNanos;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicReference<WindowState> state;

    private record WindowState(long windowStartNanos, int count) {
    }

    public FixedWindowRateLimiter(int maxEvents, Duration window) {
        this(maxEvents, window, System::nanoTime);
    }

    public FixedWindowRateLimiter(int maxEvents, Duration window, LongSupplier nanoTimeSupplier) {
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be > 0, got " + maxEvents);
        }
        if (window == null || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be a positive Duration");
        }
        this.maxEvents = maxEvents;
        this.windowNanos = window.toNanos();
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.state = new AtomicReference<>(new WindowState(nanoTimeSupplier.getAsLong(), 0));
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
        while (true) {
            WindowState current = state.get();
            if (now - current.windowStartNanos() >= windowNanos) {
                // Window rolled over: start a fresh window that consumes this acquire.
                WindowState rolled = new WindowState(now, permits);
                if (state.compareAndSet(current, rolled)) {
                    return true;
                }
            } else if (current.count() + permits <= maxEvents) {
                WindowState updated = new WindowState(current.windowStartNanos(), current.count() + permits);
                if (state.compareAndSet(current, updated)) {
                    return true;
                }
            } else if (state.compareAndSet(current, current)) {
                // Rejected; window unchanged. CAS against itself just checks we
                // observed a stable window boundary.
                return false;
            }
        }
    }

    @Override
    public RateLimitResult tryAcquireWithDetail() {
        long now = nanoTimeSupplier.getAsLong();
        while (true) {
            WindowState current = state.get();
            if (now - current.windowStartNanos() >= windowNanos) {
                if (state.compareAndSet(current, new WindowState(now, 1))) {
                    return RateLimitResult.allowed(maxEvents - 1);
                }
            } else if (current.count() < maxEvents) {
                if (state.compareAndSet(current, new WindowState(current.windowStartNanos(), current.count() + 1))) {
                    return RateLimitResult.allowed(maxEvents - current.count() - 1);
                }
            } else {
                long nextWindowNanos = current.windowStartNanos() + windowNanos - now;
                long retryAfter = Math.max(1, (long) Math.ceil(nextWindowNanos / NANOS_PER_MILLI));
                if (state.compareAndSet(current, current)) {
                    return RateLimitResult.rejected(0, retryAfter);
                }
            }
        }
    }

    @Override
    public double availablePermits() {
        WindowState current = state.get();
        long now = nanoTimeSupplier.getAsLong();
        if (now - current.windowStartNanos() >= windowNanos) {
            return maxEvents; // window will roll over on the next acquire
        }
        return maxEvents - current.count();
    }
}
