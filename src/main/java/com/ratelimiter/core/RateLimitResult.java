package com.ratelimiter.core;

/**
 * Outcome of a rate limit attempt, with enough detail to make a sensible
 * rejection response (e.g. an HTTP 429 with a Retry-After header).
 *
 * @param allowed           whether the permits were granted
 * @param remainingPermits  permits still available at the moment of the decision
 * @param retryAfterMillis  when rejected: estimate of milliseconds until one
 *                          permit is available; {@code -1} if the limiter can
 *                          never recover (zero refill rate); {@code 0} if allowed
 */
public record RateLimitResult(boolean allowed, double remainingPermits, long retryAfterMillis) {

    public static RateLimitResult allowed(double remainingPermits) {
        return new RateLimitResult(true, remainingPermits, 0);
    }

    public static RateLimitResult rejected(double remainingPermits, long retryAfterMillis) {
        return new RateLimitResult(false, remainingPermits, retryAfterMillis);
    }
}
