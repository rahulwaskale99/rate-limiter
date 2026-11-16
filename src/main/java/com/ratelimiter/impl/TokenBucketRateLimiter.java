package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Lock-free token bucket.
 *
 * <p>The bucket holds up to {@code capacity} permits and refills continuously
 * at {@code refillPerSecond} permits per second. Bursts up to the capacity are
 * allowed, after which callers are throttled to the refill rate.
 *
 * <p><strong>Concurrency design.</strong> State is kept in an
 * {@link AtomicReference} to a small immutable record ({@code tokens,
 * lastUpdateNanos}). Every acquire runs a CAS retry loop: read the snapshot,
 * refill it using the elapsed time, then {@code compareAndSet} the new snapshot.
 * No lock is ever taken, so threads never block each other; under contention
 * they simply retry the CAS. Refill is lazy — time is only converted into
 * permits when somebody asks — which keeps idle limiters completely free.
 *
 * <p>Even a rejected acquire commits the refill progress, so elapsed time is
 * never double-credited by two successive callers.
 *
 * <p>The {@code nanoTimeSupplier} constructor parameter exists purely for
 * deterministic testing: tests inject a controllable clock instead of
 * {@link System#nanoTime}.
 */
public final class TokenBucketRateLimiter implements RateLimiter {

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double MILLIS_PER_SECOND = 1_000.0;

    private final double capacity;
    private final double refillPerSecond;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicReference<Snapshot> state;

    /** Immutable state cell, replaced atomically via CAS. */
    private record Snapshot(double tokens, long lastUpdateNanos) {
    }

    public TokenBucketRateLimiter(double capacity, double refillPerSecond) {
        this(capacity, refillPerSecond, System::nanoTime);
    }

    public TokenBucketRateLimiter(double capacity, double refillPerSecond, LongSupplier nanoTimeSupplier) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        if (refillPerSecond < 0) {
            throw new IllegalArgumentException("refillPerSecond must be >= 0, got " + refillPerSecond);
        }
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.state = new AtomicReference<>(new Snapshot(capacity, nanoTimeSupplier.getAsLong()));
    }

    @Override
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be > 0, got " + permits);
        }
        if (permits > capacity) {
            return false; // never satisfiable, whatever the refill
        }
        long now = nanoTimeSupplier.getAsLong();
        while (true) {
            Snapshot current = state.get();
            double tokens = refill(current, now);
            if (tokens >= permits) {
                if (state.compareAndSet(current, new Snapshot(tokens - permits, now))) {
                    return true;
                }
            } else if (state.compareAndSet(current, new Snapshot(tokens, now))) {
                // Commit refill progress even on rejection: the next caller must
                // not re-credit the same elapsed time.
                return false;
            }
            // CAS failed = another thread won the race; retry with fresh state.
        }
    }

    @Override
    public RateLimitResult tryAcquireWithDetail() {
        long now = nanoTimeSupplier.getAsLong();
        while (true) {
            Snapshot current = state.get();
            double tokens = refill(current, now);
            if (tokens >= 1) {
                if (state.compareAndSet(current, new Snapshot(tokens - 1, now))) {
                    return RateLimitResult.allowed(tokens - 1);
                }
            } else {
                long retryAfter = retryAfterMillis(tokens, 1);
                if (state.compareAndSet(current, new Snapshot(tokens, now))) {
                    return RateLimitResult.rejected(tokens, retryAfter);
                }
            }
        }
    }

    @Override
    public double availablePermits() {
        Snapshot current = state.get();
        return refill(current, nanoTimeSupplier.getAsLong());
    }

    /** Converts the elapsed time since the last update into permits, capped at capacity. */
    private double refill(Snapshot snapshot, long nowNanos) {
        if (refillPerSecond == 0) {
            return snapshot.tokens();
        }
        double elapsedSeconds = (nowNanos - snapshot.lastUpdateNanos()) / NANOS_PER_SECOND;
        double tokens = snapshot.tokens() + elapsedSeconds * refillPerSecond;
        return Math.min(capacity, tokens);
    }

    private long retryAfterMillis(double availableTokens, double permits) {
        if (refillPerSecond == 0) {
            return -1; // fixed pool, no refill scheduled
        }
        double missingPermits = permits - availableTokens;
        double secondsUntilAvailable = missingPermits / refillPerSecond;
        return (long) Math.ceil(secondsUntilAvailable * MILLIS_PER_SECOND);
    }
}
