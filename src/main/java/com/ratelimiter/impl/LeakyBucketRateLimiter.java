package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Leaky bucket (smooth-output variant).
 *
 * <p>Accepted requests pour into a virtual bucket; the bucket drains at a
 * constant {@code leakPerSecond} rate. Acceptance is bounded by the bucket
 * {@code capacity}, so bursts are absorbed, while the <em>effective</em>
 * outflow the downstream system experiences is smoothed to the leak rate.
 * This is the shape you want when the consumer cannot tolerate bursts at all
 * — for example a partner API that meters requests per second strictly.
 *
 * <p><strong>How it differs from the token bucket.</strong> Both are CAS-based
 * and O(1), and both admit an initial burst up to capacity. The difference is
 * what time does: in the token bucket, idle time accumulates as credit and a
 * later burst can run at full speed; in the leaky bucket, time only lowers
 * the queue level, so the caller is throttled to a steady drip regardless of
 * how long they waited. Token bucket = generous to well-behaved callers;
 * leaky bucket = protective of a fixed-throughput downstream.
 *
 * <p><strong>Concurrency design.</strong> Identical pattern to
 * {@link TokenBucketRateLimiter}: an {@link AtomicReference} to an immutable
 * {@code (level, lastDrainNanos)} cell, CAS retry loop on every acquire, lazy
 * time-to-level conversion, and rejected acquires still commit the drain
 * progress so elapsed time is never double-counted.
 *
 * <p>The {@code nanoTimeSupplier} constructor parameter exists purely for
 * deterministic testing.
 */
public final class LeakyBucketRateLimiter implements RateLimiter {

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double MILLIS_PER_SECOND = 1_000.0;

    private final double capacity;
    private final double leakPerSecond;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicReference<Snapshot> state;

    /** Immutable state cell, replaced atomically via CAS. */
    private record Snapshot(double level, long lastDrainNanos) {
    }

    public LeakyBucketRateLimiter(double capacity, double leakPerSecond) {
        this(capacity, leakPerSecond, System::nanoTime);
    }

    public LeakyBucketRateLimiter(double capacity, double leakPerSecond, LongSupplier nanoTimeSupplier) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        if (leakPerSecond < 0) {
            throw new IllegalArgumentException("leakPerSecond must be >= 0, got " + leakPerSecond);
        }
        this.capacity = capacity;
        this.leakPerSecond = leakPerSecond;
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.state = new AtomicReference<>(new Snapshot(0, nanoTimeSupplier.getAsLong()));
    }

    @Override
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be > 0, got " + permits);
        }
        if (permits > capacity) {
            return false; // never satisfiable, whatever the drain
        }
        long now = nanoTimeSupplier.getAsLong();
        while (true) {
            Snapshot current = state.get();
            double level = drain(current, now);
            if (level + permits <= capacity) {
                if (state.compareAndSet(current, new Snapshot(level + permits, now))) {
                    return true;
                }
            } else if (state.compareAndSet(current, new Snapshot(level, now))) {
                // Commit the drain progress even on rejection: the next caller
                // must not re-credit the same elapsed time.
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
            double level = drain(current, now);
            if (level + 1 <= capacity) {
                if (state.compareAndSet(current, new Snapshot(level + 1, now))) {
                    return RateLimitResult.allowed(capacity - level - 1);
                }
            } else {
                long retryAfter = retryAfterMillis(level, 1);
                if (state.compareAndSet(current, new Snapshot(level, now))) {
                    return RateLimitResult.rejected(capacity - level, retryAfter);
                }
            }
        }
    }

    @Override
    public double availablePermits() {
        Snapshot current = state.get();
        return capacity - drain(current, nanoTimeSupplier.getAsLong());
    }

    /** Converts the elapsed time since the last drain into freed space, floored at zero. */
    private double drain(Snapshot snapshot, long nowNanos) {
        if (leakPerSecond == 0) {
            return snapshot.level();
        }
        double elapsedSeconds = (nowNanos - snapshot.lastDrainNanos()) / NANOS_PER_SECOND;
        return Math.max(0, snapshot.level() - elapsedSeconds * leakPerSecond);
    }

    private long retryAfterMillis(double level, double permits) {
        if (leakPerSecond == 0) {
            return -1; // fixed pool, no drain scheduled
        }
        double missingSpace = level + permits - capacity;
        double secondsUntilSpace = missingSpace / leakPerSecond;
        return (long) Math.ceil(secondsUntilSpace * MILLIS_PER_SECOND);
    }
}
