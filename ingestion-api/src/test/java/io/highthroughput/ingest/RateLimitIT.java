package io.highthroughput.ingest;

import io.highthroughput.contract.Topics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-tenant limits through the API, with buckets in a real Valkey (ADR-0014).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 10 events/s, burst 500: a refill is too slow to matter within a test.
        "rate-limit.plans.slow.events-per-second=10",
        "rate-limit.plans.slow.burst=500",
        "rate-limit.plans.slow.daily-quota=1000000",
        // Fast refill, but only 600 events per day.
        "rate-limit.plans.capped.events-per-second=100000",
        "rate-limit.plans.capped.burst=500",
        "rate-limit.plans.capped.daily-quota=600",
        "rate-limit.tenants.slow-tenant=slow",
        "rate-limit.tenants.capped-tenant=capped",
        "rate-limit.tenants.pro-tenant=pro" })
@Import(TestcontainersConfiguration.class)
class RateLimitIT {

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Test
    void aBatchIsAdmittedOrRejectedAsAWhole() throws Exception {
        assertThat(post("slow-tenant", 300).statusCode()).isEqualTo(202);

        // About 200 tokens are left: 300 do not fit, and none of them is taken or
        // written.
        HttpResponse<String> rejected = post("slow-tenant", 300);
        assertThat(rejected.statusCode()).isEqualTo(429);
        assertThat(rejected.headers().firstValue("Content-Type")).hasValue("application/problem+json");
        assertThat(rejected.body()).contains("\"limit\":\"rate\"");
        // About 100 missing tokens at 10 per second.
        assertThat(retryAfterSeconds(rejected)).isBetween(1L, 10L);

        assertThat(post("slow-tenant", 200).statusCode()).isEqualTo(202);
        assertThat(eventsOf("slow-tenant")).isEqualTo(500);
    }

    @Test
    void exhaustedDailyQuotaIsRejectedUntilUtcMidnight() throws Exception {
        assertThat(post("capped-tenant", 500).statusCode()).isEqualTo(202);

        HttpResponse<String> rejected = post("capped-tenant", 200);

        assertThat(rejected.statusCode()).isEqualTo(429);
        assertThat(rejected.body()).contains("\"limit\":\"quota\"")
                .contains("exceeds this tenant's remaining daily event quota", "No event of the batch was accepted");
        Instant nextMidnight = LocalDate.now(ZoneOffset.UTC).plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        long secondsToMidnight = Duration.between(Instant.now(), nextMidnight).toSeconds();
        assertThat(retryAfterSeconds(rejected)).isBetween(secondsToMidnight - 5, secondsToMidnight + 5);
        // The remaining 100 still fit.
        assertThat(post("capped-tenant", 100).statusCode()).isEqualTo(202);
    }

    @Test
    void anExhaustedTenantDoesNotAffectOthers() throws Exception {
        // Unlisted, so the default free plan: burst 1,000.
        assertThat(post("free-tenant", 500).statusCode()).isEqualTo(202);
        assertThat(post("free-tenant", 500).statusCode()).isEqualTo(202);
        assertThat(post("free-tenant", 500).statusCode()).isEqualTo(429);

        assertThat(post("pro-tenant", 500).statusCode()).isEqualTo(202);
        assertThat(post("pro-tenant", 500).statusCode()).isEqualTo(202);
        assertThat(post("pro-tenant", 500).statusCode()).isEqualTo(202);
    }

    private HttpResponse<String> post(String tenant, int events) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(tenant))
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(events))))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private long eventsOf(String tenant) {
        return TopicContents.readAll(kafka.getBootstrapServers(), Topics.EVENTS).stream()
                .filter(record -> tenant.equals(record.key()))
                .count();
    }

    private static long retryAfterSeconds(HttpResponse<?> response) {
        return Long.parseLong(response.headers().firstValue("Retry-After").orElseThrow());
    }
}
