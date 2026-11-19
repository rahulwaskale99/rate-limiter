package com.ratelimiter.core;

/**
 * Contract for all rate limiter implementations.
 *
 * <p>Implementations must be thread-safe and must not wait for a permit:
 * each acquire call returns an allow/reject decision instead of waiting for
 * capacity to become available. An implementation may still use a short
 * internal synchronization mechanism, so this contract does not promise
 * lock-free execution. Callers that want to wait can use
 * {@link RateLimitResult#retryAfterMillis()} to back off before retrying.
 */
public interface RateLimiter {

    /**
     * Tries to acquire a single permit without waiting for capacity.
     *
     * @return true if the permit was granted
     */
    default boolean tryAcquire() {
        return tryAcquire(1);
    }

    /**
     * Tries to acquire {@code permits} permits atomically — either all of them
     * are granted or none are.
     *
     * @param permits number of permits to acquire, must be &gt; 0
     * @return true if all permits were granted
     * @throws IllegalArgumentException if {@code permits <= 0}
     */
    boolean tryAcquire(int permits);

    /**
     * Same as {@link #tryAcquire()} but reports how many permits remain and,
     * when rejected, an estimate of how long until the next permit becomes
     * available. Useful for setting {@code Retry-After} headers on HTTP APIs.
     */
    RateLimitResult tryAcquireWithDetail();

    /**
     * Best-effort estimate of the permits currently available. The value may be
     * slightly stale the moment it is read because other threads are acquiring
     * concurrently.
     */
    double availablePermits();
}
