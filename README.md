# rate-limiter

A zero-dependency Java 21 library implementing classic rate-limiting algorithms with genuinely correct concurrency — lock-free CAS loops on the hot path, a real concurrency stress test in the suite, and deterministic unit tests via an injected clock.

Built as a focused exercise in production-grade concurrent Java: every design decision (why CAS over `synchronized`, when `synchronized` is the honest choice, how to make time-based logic testable) is documented in the code.

## Algorithms

### Token bucket
```
 permits
   10 ┤ ██████████  ← burst allowed up to capacity
      │      ░░░░░ ← refills continuously at N permits/sec
    0 └──────────────→ time
```
Allows bursts up to `capacity`, then throttles to the steady refill rate. The default choice for API throttling.

### Sliding window log
```
        ┌───── window (1s) ─────┐
 events │ ● ● ●          → rejected
        └──●───────────────────┘
          ↑
     event expires → slot frees
```
Exact counting: at most N events in *any* trailing window, no boundary bursts. Costs a timestamp per accepted event.

### Fixed window
```
 |── window 1 ──|── window 2 ──|
      ● ● ● ●     ● ● ● ●
                    ↑ can burst 2x across the boundary
```
Cheapest (two fields, one CAS), but allows up to 2x the limit across a window boundary. Included as the baseline.

## When to use which

| | Token bucket | Sliding window | Fixed window |
|---|---|---|---|
| Burst tolerance | configurable | none | none (but 2x boundary leak) |
| Memory | O(1) | O(events in window) | O(1) |
| Locking | lock-free CAS | short synchronized section | lock-free CAS |
| Best for | APIs, throttling | strict quotas | coarse counters |

## Usage

```java
RateLimiter limiter = RateLimiterFactory.tokenBucket(100, 10); // burst 100, refill 10/sec

if (limiter.tryAcquire()) {
    handleRequest();
} else {
    RateLimitResult detail = limiter.tryAcquireWithDetail();
    respondWith429(detail.retryAfterMillis()); // e.g. Retry-After header
}
```

Run the live demo (50 virtual threads hammering one bucket):

```
mvn -q compile exec:java -Dexec.mainClass=com.ratelimiter.demo.RateLimiterDemo
```

or after packaging:

```
java -cp target/classes com.ratelimiter.demo.RateLimiterDemo
```

## Testability

All limiters accept a `LongSupplier nanoTimeSupplier` so tests control time instead of sleeping:

```java
AtomicLong fakeNanos = new AtomicLong();
var limiter = new TokenBucketRateLimiter(10, 5, fakeNanos::get);
limiter.tryAcquire();
fakeNanos.addAndGet(200_000_000L); // advance 200ms deterministically
```

`ConcurrencyStressTest` is the opposite: real virtual threads, real clock — 1000 threads race for 100 permits and the suite asserts *exactly* 100 are admitted (no over-admission, no lost updates).

## Building

```
mvn test        # run the suite
mvn package     # build the jar
```

Requires JDK 21 (virtual threads).

## Roadmap

- [ ] Leaky bucket (smooth-output variant) implementation
- [ ] Composite limiters: AND / OR combinators over multiple limiters
- [ ] Weighted permits (cost per request, not just count)
- [ ] Keyed registry: one limiter per API key / user id with weak cleanup
- [ ] Warm-up limiter (gradual ramp from cold start, like Guava's SmoothWarmingUp)
- [ ] Micrometer metrics hook (accepted/rejected counters per limiter)
- [ ] Redis-backed distributed token bucket (Lua script, single round trip)
- [ ] Spring Boot starter: `@RateLimit` annotation on controller methods
- [ ] JMH benchmark module: CAS loop vs synchronized under contention
- [ ] Documentation site / architecture decision records
