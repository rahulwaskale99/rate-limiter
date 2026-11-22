package com.ratelimiter.impl;

import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Leaky-bucket-style admission limiter using a virtual backlog.
 *
 * <p>Each accepted request adds to the backlog, and elapsed time drains it at
 * the configured {@code leakPerSecond} rate. A request is admitted only when
 * the resulting backlog stays within {@code capacity}. When the backlog has
 * drained to zero, the full capacity is available again, so an idle limiter
 * can admit a burst up to that capacity.
 *
 * <p>This implementation only makes an allow/reject decision. It does not
 * enqueue accepted work or schedule its execution, so it does not guarantee
 * smooth downstream dispatch. Use a queue and scheduler when actual traffic
 * shaping is required. With the same capacity and rate, this virtual-backlog
 * representation has the same admission envelope as a token bucket; it
 * expresses state as queued work rather than available credit.
 *
 * <p><strong>Concurrency design.</strong> State is kept in an
 * {@link AtomicReference} to an immutable {@code (level, lastDrainNanos)}
 * cell. Each acquire uses a CAS retry loop, lazy time-to-level conversion,
 * and rejected acquires still commit drain progress so elapsed time is never
 * double-counted.
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
