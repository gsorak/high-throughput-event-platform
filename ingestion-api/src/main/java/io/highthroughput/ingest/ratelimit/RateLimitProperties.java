package io.highthroughput.ingest.ratelimit;

import io.highthroughput.ingest.web.IngestEventsRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Settings under {@code rate-limit.*} (ADR-0014). Plans are server-side
 * configuration: a tenant's plan is looked up
 * here, never taken from the request or the token.
 *
 * @param valkeyUri         Valkey that holds the shared token buckets
 * @param timeout           connect and command timeout for Valkey; a slower
 *                          call counts as a failure and the request is
 *                          limited locally instead
 * @param defaultPlan       plan for tenants not listed in {@code tenants} and
 *                          matching no {@code tenantPatterns}
 * @param plans             plans by name
 * @param tenants           tenant id → plan name; takes precedence over
 *                          {@code tenantPatterns}
 * @param tenantPatterns    ordered rules for tenants not listed in
 *                          {@code tenants}; the first whose regular
 *                          expression matches the whole tenant id wins
 * @param expectedInstances API instances that share the limits; while Valkey is
 *                          unavailable each instance enforces
 *                          this share of the plan on its own (ADR-0004)
 */
@Validated
@ConfigurationProperties("rate-limit")
public record RateLimitProperties(
        @NotBlank String valkeyUri,
        @NotNull Duration timeout,
        @NotBlank String defaultPlan,
        @NotEmpty Map<String, @Valid Plan> plans,
        Map<String, String> tenants,
        List<@Valid TenantPattern> tenantPatterns,
        @DefaultValue("1") @Min(1) int expectedInstances) {

    public RateLimitProperties {
        tenants = tenants == null ? Map.of() : tenants;
        tenantPatterns = tenantPatterns == null ? List.of() : List.copyOf(tenantPatterns);
        if (plans == null || !plans.containsKey(defaultPlan)) {
            throw new IllegalArgumentException(
                    "rate-limit.default-plan '" + defaultPlan + "' is not in rate-limit.plans");
        }
        tenants.forEach((tenant, plan) -> {
            if (!plans.containsKey(plan)) {
                throw new IllegalArgumentException(
                        "rate-limit.tenants." + tenant + " refers to unknown plan '" + plan + "'");
            }
        });
        for (TenantPattern rule : tenantPatterns) {
            if (!plans.containsKey(rule.plan())) {
                throw new IllegalArgumentException(
                        "rate-limit.tenant-patterns '" + rule.pattern() + "' refers to unknown plan '" + rule.plan()
                                + "'");
            }
        }
    }

    public Plan planFor(String tenantId) {
        String plan = tenants.get(tenantId);
        if (plan == null) {
            plan = tenantPatterns.stream().filter(rule -> rule.matches(tenantId)).findFirst()
                    .map(TenantPattern::plan).orElse(defaultPlan);
        }
        return plans.get(plan);
    }

    /**
     * @param pattern regular expression that must match the whole tenant id
     * @param plan    plan name
     */
    public record TenantPattern(@NotBlank String pattern, @NotBlank String plan) {

        private static final Map<String, Pattern> COMPILED = new ConcurrentHashMap<>();

        public TenantPattern {
            if (pattern != null) {
                compiled(pattern); // fails at startup on an invalid expression
            }
        }

        boolean matches(String tenantId) {
            return compiled(pattern).matcher(tenantId).matches();
        }

        private static Pattern compiled(String pattern) {
            return COMPILED.computeIfAbsent(pattern, Pattern::compile);
        }
    }

    /**
     * Burst and daily quota must each fit one full batch; otherwise a maximum-size
     * batch could never be accepted.
     *
     * @param eventsPerSecond sustained rate
     * @param burst           events that may arrive at once after a quiet period
     * @param dailyQuota      events per UTC day
     */
    public record Plan(
            @Min(1) long eventsPerSecond,
            @Min(IngestEventsRequest.MAX_EVENTS) long burst,
            @Min(IngestEventsRequest.MAX_EVENTS) long dailyQuota) {

        /**
         * The longest wait the rate limit alone can cause: refilling an empty burst.
         */
        Duration burstRefillTime() {
            return Duration.ofNanos(Math.ceilDiv(burst * 1_000_000_000L, eventsPerSecond));
        }

        /**
         * One instance's share of this plan, but burst and quota never below one full
         * batch.
         */
        Plan perInstance(int instances) {
            return new Plan(Math.max(1, eventsPerSecond / instances),
                    Math.max(IngestEventsRequest.MAX_EVENTS, burst / instances),
                    Math.max(IngestEventsRequest.MAX_EVENTS, dailyQuota / instances));
        }
    }
}
