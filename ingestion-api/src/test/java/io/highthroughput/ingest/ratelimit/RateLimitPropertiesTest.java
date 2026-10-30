package io.highthroughput.ingest.ratelimit;

import io.highthroughput.ingest.ratelimit.RateLimitProperties.Plan;
import io.highthroughput.ingest.ratelimit.RateLimitProperties.TenantPattern;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan resolution: exact tenant mapping, then the first matching pattern, then
 * the default plan.
 */
class RateLimitPropertiesTest {

    private static final Plan FREE = new Plan(200, 1000, 1_000_000);
    private static final Plan PRO = new Plan(5000, 10_000, 100_000_000);
    private static final Plan SMALL = new Plan(100, 500, 1000);

    private static RateLimitProperties properties(Map<String, String> tenants, List<TenantPattern> patterns) {
        return new RateLimitProperties("redis://localhost:6379", Duration.ofMillis(100), "free",
                Map.of("free", FREE, "pro", PRO, "small", SMALL), tenants, patterns, 1);
    }

    @Test
    void exactMappingWinsOverPatternsAndPatternsOverTheDefault() {
        RateLimitProperties properties = properties(Map.of("demo-0000000000000000-b", "pro"), List.of(
                new TenantPattern("demo-[0-9a-f]{16}-b", "small"),
                new TenantPattern("demo-.*", "pro")));

        assertThat(properties.planFor("demo-0000000000000000-b")).isEqualTo(PRO);
        assertThat(properties.planFor("demo-0123456789abcdef-b")).isEqualTo(SMALL);
        assertThat(properties.planFor("demo-0123456789abcdef-a")).isEqualTo(PRO);
        assertThat(properties.planFor("acme")).isEqualTo(FREE);
    }

    @Test
    void aPatternMustMatchTheWholeTenantId() {
        RateLimitProperties properties = properties(Map.of(),
                List.of(new TenantPattern("demo-[0-9a-f]{16}-b", "small")));

        assertThat(properties.planFor("xdemo-0123456789abcdef-b")).isEqualTo(FREE);
        assertThat(properties.planFor("demo-0123456789abcdef-bx")).isEqualTo(FREE);
    }

    @Test
    void withoutPatternsTheExistingBehaviourIsUnchanged() {
        RateLimitProperties properties = properties(Map.of("acme", "pro"), null);

        assertThat(properties.tenantPatterns()).isEmpty();
        assertThat(properties.planFor("acme")).isEqualTo(PRO);
        assertThat(properties.planFor("demo-0123456789abcdef-b")).isEqualTo(FREE);
    }

    @Test
    void unknownPlansAndInvalidExpressionsFailAtStartup() {
        assertThatThrownBy(() -> properties(Map.of(), List.of(new TenantPattern("demo-.*", "gold"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown plan 'gold'");
        assertThatThrownBy(() -> new TenantPattern("demo-[", "small"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
