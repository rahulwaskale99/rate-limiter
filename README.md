# rate-limiter

A small Java 21 library for deciding whether a request should be allowed through.

It has three familiar algorithms: token bucket, sliding window log, and fixed window. There are no runtime dependencies. I built it to get hands-on with concurrency, time-based state, and the trade-offs behind each approach.

## Pick an algorithm

| Algorithm | What it does | Useful when |
|---|---|---|
| Token bucket | Keeps a bucket of permits and refills it over time. Allows short bursts. | You want to absorb bursts but still enforce a steady rate. |
| Sliding window log | Tracks accepted event times and checks the exact trailing window. | The limit must hold across every point in the window. |
| Fixed window | Counts permits inside aligned time windows. | You want a simple, low-memory counter and can accept boundary bursts. |

A fixed window can allow close to twice the configured limit around a boundary. The sliding window avoids that, but has to keep timestamps for accepted events. The token bucket sits somewhere in between: bursts are allowed, but only up to its capacity.

## Use it

```java
import com.ratelimiter.core.RateLimitResult;
import com.ratelimiter.core.RateLimiter;
import com.ratelimiter.core.RateLimiterFactory;

RateLimiter limiter = RateLimiterFactory.tokenBucket(100, 10);
RateLimitResult result = limiter.tryAcquireWithDetail();

if (result.allowed()) {
    handleRequest();
} else {
    // Use this value when building an HTTP 429 response.
    long retryAfterMillis = result.retryAfterMillis();
}
```

If you only need a yes/no decision, use `tryAcquire()`. You can also acquire more than one permit atomically with `tryAcquire(int permits)`.

## A few implementation details

- The token bucket stores its state in an immutable snapshot and updates it with a compare-and-set loop. Contending threads retry if another thread wins the update.
- The fixed window also uses atomic state updates. The sliding window uses a short synchronized section to manage its event timestamps; it is not lock-free.
- Time is supplied through a `LongSupplier`. Tests can move a fake clock forward instead of sleeping and hoping the timing is right.
- Calls do not sleep or queue while waiting for a permit; they return an allow/reject decision. That is different from lock-free execution: the sliding-window implementation can briefly wait to enter its internal monitor. Rejected calls include retry information in `RateLimitResult`.

The code is intentionally small so the algorithm and its concurrency behaviour are easy to follow.

## Run it

Requires JDK 21 and Maven.

```bash
mvn test
mvn package
```

Run the demo:

```bash
mvn -q compile exec:java -Dexec.mainClass=com.ratelimiter.demo.RateLimiterDemo
```

## What is not included yet

This is an in-memory library. It does not coordinate limits across multiple application instances, and it does not provide a Spring Boot integration or per-user registry.

Some useful next steps would be a Redis-backed implementation, a Spring Boot starter, weighted permits, and benchmarks under contention. Those are ideas for future work, not features in the current version.
