package io.highthroughput.ingest.ratelimit;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.local.LocalBucketBuilder;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.highthroughput.ingest.ratelimit.RateLimitExceededException.Limit;
import io.highthroughput.ingest.ratelimit.RateLimitProperties.Plan;
import io.lettuce.core.RedisCommandTimeoutException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Per-tenant limits (ADR-0014): one token bucket per tenant, one token per
 * event, with two limits — events per second
 * with a burst, and a daily quota that refills at 00:00 UTC. A batch is
 * admitted or rejected as a whole.
 * <p>
 * The buckets live in Valkey so all API instances share them. If Valkey fails
 * or the circuit breaker is open, each
 * instance limits with its own in-memory buckets instead, holding
 * {@code 1 / rate-limit.expected-instances} of the plan
 * (ADR-0004). Ingestion stays available, but the combined limit is only right
 * if that count is right and the load is
 * balanced evenly; at worst it becomes <i>instances × local limit</i> until
 * Valkey is back. The gauge
 * {@code ratelimit.fallback.active} shows when that is the case. The timer
 * {@code ratelimit.valkey.duration} times every Valkey operation by outcome:
 * success, timeout, error, or breaker_open (no call made).
 */
public class TenantRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TenantRateLimiter.class);

    static final String SUCCESS = "success";
    static final String TIMEOUT = "timeout";
    static final String ERROR = "error";
    static final String BREAKER_OPEN = "breaker_open";

    private final RateLimitProperties properties;
    private final ValkeyBuckets valkey;
    private final Clock clock;
    private final CircuitBreaker circuitBreaker;
    private final Map<String, Timer> valkeyTimers;
    private final ConcurrentMap<String, Bucket> localBuckets = new ConcurrentHashMap<>();

    public TenantRateLimiter(RateLimitProperties properties, ValkeyBuckets valkey, Clock clock,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.valkey = valkey;
        this.clock = clock;
        this.circuitBreaker = CircuitBreaker.of("valkey", CircuitBreakerConfig.custom()
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build());
        circuitBreaker.getEventPublisher().onStateTransition(event -> log.warn(
                "Valkey circuit breaker {}; tenant limits are {}", event.getStateTransition(),
                event.getStateTransition().getToState() == CircuitBreaker.State.CLOSED
                        ? "shared again"
                        : "enforced per instance"));
        Gauge.builder("ratelimit.fallback.active", circuitBreaker,
                breaker -> breaker.getState() == CircuitBreaker.State.CLOSED ? 0 : 1)
                .description("1 while tenant limits are enforced per instance because Valkey is unavailable")
                .register(meterRegistry);
        this.valkeyTimers = Map.of(
                SUCCESS, valkeyTimer(meterRegistry, SUCCESS),
                TIMEOUT, valkeyTimer(meterRegistry, TIMEOUT),
                ERROR, valkeyTimer(meterRegistry, ERROR),
                BREAKER_OPEN, valkeyTimer(meterRegistry, BREAKER_OPEN));
    }

    private static Timer valkeyTimer(MeterRegistry meterRegistry, String outcome) {
        return Timer.builder("ratelimit.valkey.duration")
                .description("Rate-limit operations on Valkey, including the circuit breaker")
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    /**
     * Takes one token per event, or throws {@link RateLimitExceededException} and
     * takes none.
     */
    public void acquire(String tenantId, int events) {
        Plan plan = properties.planFor(tenantId);
        Plan applied = plan;
        ConsumptionProbe probe;
        long start = System.nanoTime();
        try {
            probe = circuitBreaker.executeSupplier(() -> valkey
                    .bucket(tenantId, () -> configuration(plan, clock.instant()))
                    .tryConsumeAndReturnRemaining(events));
            valkeyTimers.get(SUCCESS).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            // Valkey failed, timed out, or the breaker is open.
            valkeyTimers.get(outcome(e)).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            log.debug("Limiting tenant {} locally: {}", tenantId, e.toString());
            applied = plan.perInstance(properties.expectedInstances());
            probe = localBucketFor(tenantId, applied).tryConsumeAndReturnRemaining(events);
        }
        if (!probe.isConsumed()) {
            long nanosToWait = probe.getNanosToWaitForRefill();
            throw new RateLimitExceededException(limitFor(applied, nanosToWait), retryAfter(nanosToWait));
        }
    }

    static String outcome(Throwable failure) {
        if (failure instanceof CallNotPermittedException) {
            return BREAKER_OPEN;
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof RedisCommandTimeoutException || cause instanceof TimeoutException) {
                return TIMEOUT;
            }
        }
        return ERROR;
    }

    private Bucket localBucketFor(String tenantId, Plan plan) {
        return localBuckets.computeIfAbsent(tenantId,
                id -> localBucket(configuration(plan, clock.instant()), TimeMeter.SYSTEM_MILLISECONDS));
    }

    static Bucket localBucket(BucketConfiguration configuration, TimeMeter timeMeter) {
        LocalBucketBuilder builder = Bucket.builder().withCustomTimePrecision(timeMeter);
        for (Bandwidth bandwidth : configuration.getBandwidths()) {
            builder.addLimit(bandwidth);
        }
        return builder.build();
    }

    static BucketConfiguration configuration(Plan plan, Instant now) {
        Instant nextUtcMidnight = LocalDate.ofInstant(now, ZoneOffset.UTC).plusDays(1)
                .atStartOfDay(ZoneOffset.UTC).toInstant();
        return BucketConfiguration.builder()
                .addLimit(Bandwidth.builder()
                        .capacity(plan.burst())
                        .refillGreedy(plan.eventsPerSecond(), Duration.ofSeconds(1))
                        .id("rate")
                        .build())
                // Refills completely once a day at 00:00 UTC (a calendar day, not a rolling 24
                // hours).
                .addLimit(Bandwidth.builder()
                        .capacity(plan.dailyQuota())
                        .refillIntervallyAligned(plan.dailyQuota(), Duration.ofDays(1), nextUtcMidnight)
                        .id("quota")
                        .build())
                .build();
    }

    /**
     * The rate limit alone never needs longer than refilling the whole burst; a
     * longer wait means the quota.
     */
    static Limit limitFor(Plan plan, long nanosToWait) {
        return nanosToWait > plan.burstRefillTime().toNanos() ? Limit.QUOTA : Limit.RATE;
    }

    private static Duration retryAfter(long nanosToWait) {
        return Duration.ofSeconds(Math.max(1, Math.ceilDiv(nanosToWait, 1_000_000_000L)));
    }
}
