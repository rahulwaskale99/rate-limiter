package com.ratelimiter.demo;

import com.ratelimiter.core.RateLimiter;
import com.ratelimiter.core.RateLimiterFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Runnable demo: 50 virtual threads hammer a token bucket (capacity 10,
 * refill 5 permits/second) for 3 seconds. Expected outcome is roughly
 * 10 (initial burst) + ~15 (refill) accepted calls and everything else
 * rejected — a live demonstration that the bucket enforces the configured
 * rate under real concurrency.
 */
public final class RateLimiterDemo {

    private static final int THREADS = 50;
    private static final long DURATION_SECONDS = 3;

    public static void main(String[] args) throws InterruptedException {
        RateLimiter limiter = RateLimiterFactory.tokenBucket(10, 5);
        LongAdder accepted = new LongAdder();
        LongAdder rejected = new LongAdder();
        LongAdder attempts = new LongAdder();

        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < THREADS; i++) {
                executor.submit(() -> {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DURATION_SECONDS);
                    while (System.nanoTime() < deadline) {
                        attempts.increment();
                        if (limiter.tryAcquire()) {
                            accepted.increment();
                        } else {
                            rejected.increment();
                            // In a real service: back off, return 429, etc.
                        }
                    }
                });
            }
        } // executor.close() waits for all virtual threads to finish

        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("threads=%d, wall time=%.1fs%n", THREADS, seconds);
        System.out.printf("attempts=%d, accepted=%d, rejected=%d%n",
                attempts.sum(), accepted.sum(), rejected.sum());
        System.out.printf("observed throughput=%.2f permits/sec (configured refill=5.0)%n",
                accepted.sum() / seconds);
    }

    private RateLimiterDemo() {
    }
}
