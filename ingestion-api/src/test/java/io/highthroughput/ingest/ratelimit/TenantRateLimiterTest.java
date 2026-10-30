package io.highthroughput.ingest.ratelimit;

import io.github.bucket4j.Bucket;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.TimeMeter;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.highthroughput.ingest.ratelimit.RateLimitExceededException.Limit;
import io.highthroughput.ingest.ratelimit.RateLimitProperties.Plan;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisConnectionException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** The bucket configuration of a plan, on a manually advanced clock. */
class TenantRateLimiterTest {

    /** 100 events/s, burst 500, 1,000 events per day. */
    private static final Plan PLAN = new Plan(100, 500, 1000);

    private final ManualTime time = new ManualTime();

    @Test
    void dailyQuotaRefillsAtUtcMidnightNotTwentyFourHoursLater() {
        time.set(Instant.parse("2026-03-10T22:00:00Z"));
        Bucket bucket = newBucket();

        assertThat(bucket.tryConsume(500)).isTrue();
        time.advance(Duration.ofSeconds(5)); // the burst refills
        assertThat(bucket.tryConsume(500)).isTrue();
        time.advance(Duration.ofSeconds(5));

        ConsumptionProbe exhausted = bucket.tryConsumeAndReturnRemaining(1);
        assertThat(exhausted.isConsumed()).isFalse();
        assertThat(Duration.ofNanos(exhausted.getNanosToWaitForRefill()))
                .isCloseTo(Duration.ofHours(2).minusSeconds(10), Duration.ofMillis(1));
        assertThat(TenantRateLimiter.limitFor(PLAN, exhausted.getNanosToWaitForRefill())).isEqualTo(Limit.QUOTA);

        time.set(Instant.parse("2026-03-10T23:59:59Z"));
        assertThat(bucket.tryConsume(1)).isFalse();
        time.set(Instant.parse("2026-03-11T00:00:00Z"));
        assertThat(bucket.tryConsume(500)).isTrue();
    }

    @Test
    void fallbackShareDividesThePlanButKeepsRoomForOneFullBatch() {
        Plan free = new Plan(200, 1000, 1_000_000);

        assertThat(free.perInstance(1)).isEqualTo(free);
        assertThat(free.perInstance(2)).isEqualTo(new Plan(100, 500, 500_000));
        assertThat(free.perInstance(4)).isEqualTo(new Plan(50, 500, 250_000));
    }

    @Test
    void waitForTheBurstToRefillIsClassifiedAsRate() {
        time.set(Instant.parse("2026-03-10T12:00:00Z"));
        Bucket bucket = newBucket();
        assertThat(bucket.tryConsume(500)).isTrue();

        ConsumptionProbe probe = bucket.tryConsumeAndReturnRemaining(200);

        assertThat(probe.isConsumed()).isFalse();
        // 200 tokens at 100 per second.
        assertThat(Duration.ofNanos(probe.getNanosToWaitForRefill())).isCloseTo(Duration.ofSeconds(2),
                Duration.ofMillis(1));
        assertThat(TenantRateLimiter.limitFor(PLAN, probe.getNanosToWaitForRefill())).isEqualTo(Limit.RATE);
    }

    @Test
    void rejectedBatchTakesNoTokens() {
        time.set(Instant.parse("2026-03-10T12:00:00Z"));
        Bucket bucket = newBucket();
        assertThat(bucket.tryConsume(400)).isTrue();

        assertThat(bucket.tryConsume(200)).isFalse();

        assertThat(bucket.tryConsume(100)).isTrue();
    }

    @Test
    void valkeyFailuresAreTimedByOutcome() {
        CallNotPermittedException breakerOpen = CallNotPermittedException
                .createCallNotPermittedException(CircuitBreaker.ofDefaults("test"));

        assertThat(TenantRateLimiter.outcome(breakerOpen)).isEqualTo("breaker_open");
        assertThat(TenantRateLimiter.outcome(new RuntimeException(new RedisCommandTimeoutException("slow"))))
                .isEqualTo("timeout");
        assertThat(TenantRateLimiter.outcome(new RedisConnectionException("refused"))).isEqualTo("error");
    }

    private Bucket newBucket() {
        return TenantRateLimiter.localBucket(TenantRateLimiter.configuration(PLAN, time.instant()), time);
    }

    private static final class ManualTime implements TimeMeter {

        private long epochNanos;

        void set(Instant instant) {
            epochNanos = instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
        }

        void advance(Duration duration) {
            epochNanos += duration.toNanos();
        }

        Instant instant() {
            return Instant.ofEpochSecond(0, epochNanos);
        }

        @Override
        public long currentTimeNanos() {
            return epochNanos;
        }

        @Override
        public boolean isWallClockBased() {
            return true;
        }
    }
}
