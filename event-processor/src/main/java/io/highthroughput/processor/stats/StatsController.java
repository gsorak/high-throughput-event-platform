package io.highthroughput.processor.stats;

import io.highthroughput.contract.EventTypes;
import io.highthroughput.contract.TenantIds;
import io.highthroughput.processor.aggregation.EventStatsRepository;
import io.highthroughput.processor.aggregation.EventStatsRepository.MinuteBucket;
import io.highthroughput.processor.aggregation.EventStatsRepository.Totals;
import io.highthroughput.processor.config.ProcessorProperties;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * {@code GET /v1/stats?from=&to=[&type=]}: per-minute buckets whose start lies
 * in {@code [from, to)}, plus totals.
 *
 * <p>
 * Runs in the processor's JVM and shares its connection pool with the consumers
 * (ADR-0006), hence the hard limits
 * on range and bucket count. The tenant comes only from the verified bearer
 * token
 * (see {@code SecurityConfiguration}), so a tenant can read only its own
 * statistics.
 */
@RestController
public class StatsController {

    private final EventStatsRepository repository;
    private final ProcessorProperties.Stats limits;

    public StatsController(EventStatsRepository repository, ProcessorProperties properties) {
        this.repository = repository;
        this.limits = properties.stats();
    }

    @GetMapping("/v1/stats")
    public StatsResponse stats(@AuthenticationPrincipal Jwt token,
            @RequestParam Instant from,
            @RequestParam Instant to,
            @RequestParam(required = false) String type) {
        String tenantId = token.getClaimAsString(TenantIds.JWT_CLAIM);
        validate(from, to, type);

        // One extra row tells whether the result was cut off, without a separate COUNT
        // query.
        List<MinuteBucket> buckets = repository.findBuckets(tenantId, from, to, type, limits.maxBuckets() + 1);
        boolean truncated = buckets.size() > limits.maxBuckets();
        if (truncated) {
            buckets = buckets.subList(0, limits.maxBuckets());
        }
        Totals totals = repository.totals(tenantId, from, to, type);

        return new StatsResponse(tenantId, from, to, type, totals.count(), totals.valueSum(),
                buckets.stream().map(StatsResponse.Bucket::from).toList(), truncated);
    }

    private void validate(Instant from, Instant to, String type) {
        if (!from.isBefore(to)) {
            throw new InvalidStatsQueryException("'from' must be before 'to'");
        }
        if (Duration.between(from, to).compareTo(limits.maxRange()) > 0) {
            throw new InvalidStatsQueryException("The range must not exceed " + limits.maxRange());
        }
        if (type != null && !EventTypes.isValid(type)) {
            throw new InvalidStatsQueryException("'type' must match " + EventTypes.PATTERN + " (max "
                    + EventTypes.MAX_LENGTH + " characters)");
        }
    }
}
