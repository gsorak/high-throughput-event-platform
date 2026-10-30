package io.highthroughput.ingest;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two API instances and one Valkey (ADR-0014). While Valkey is up, the
 * instances share each tenant's bucket, so
 * together they stay within one plan. Without Valkey each instance limits on
 * its own (ADR-0004): no request fails.
 * Both instances keep the default {@code expected-instances} of 1, a configured
 * count that is too low, so the combined
 * limit grows to the ADR-0004 bound: two instances × the full plan.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SharedRateLimitIT {

    private static final long EVENTS_PER_SECOND = 50;
    private static final long BURST = 500;
    private static final int BATCH = 50;
    private static final Duration RUN = Duration.ofSeconds(2);

    private static final KafkaContainer kafka = new KafkaContainer(TestcontainersConfiguration.KAFKA_IMAGE);
    private static final GenericContainer<?> valkey = new GenericContainer<>(TestcontainersConfiguration.VALKEY_IMAGE)
            .withExposedPorts(TestcontainersConfiguration.VALKEY_PORT);
    private static final List<ConfigurableApplicationContext> instances = new ArrayList<>();

    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void startTwoInstances() {
        Startables.deepStart(kafka, valkey).join();
        for (int i = 0; i < 2; i++) {
            // Command-line arguments, which take precedence over application.yaml.
            instances.add(new SpringApplicationBuilder(IngestionApiApplication.class).run(
                    "--server.port=0",
                    "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                    "--rate-limit.valkey-uri=" + TestcontainersConfiguration.valkeyUri(valkey),
                    "--security.jwt.public-key-location=" + TestJwt.publicKeyLocation(),
                    "--rate-limit.default-plan=shared",
                    "--rate-limit.plans.shared.events-per-second=" + EVENTS_PER_SECOND,
                    "--rate-limit.plans.shared.burst=" + BURST,
                    "--rate-limit.plans.shared.daily-quota=100000000"));
        }
    }

    @AfterAll
    static void stop() {
        instances.forEach(ConfigurableApplicationContext::close);
        valkey.stop();
        kafka.stop();
    }

    @Test
    @Order(1)
    void instancesTogetherStayWithinOneTenantLimit() throws Exception {
        Result result = sendToBothInstances("shared-tenant");

        assertThat(result.serverErrors()).isZero();
        assertThat(result.acceptedEvents())
                .isGreaterThanOrEqualTo(BURST)
                .isLessThanOrEqualTo(planLimit(result.elapsed()));
        assertThat(fallbackGauges()).containsOnly(0.0);
        assertThat(valkeyCalls("success")).allMatch(count -> count > 0);
    }

    @Test
    @Order(2)
    void withoutValkeyEachInstanceLimitsOnItsOwn() throws Exception {
        valkey.stop();

        Result result = sendToBothInstances("fallback-tenant");

        assertThat(result.serverErrors()).isZero();
        assertThat(result.acceptedEvents())
                .isGreaterThan(planLimit(result.elapsed()))
                .isLessThanOrEqualTo(2 * planLimit(result.elapsed()));
        assertThat(fallbackGauges()).containsOnly(1.0);
        assertThat(valkeyCalls("timeout", "error", "breaker_open")).allMatch(count -> count > 0);
    }

    /** Sends batches to the two instances in turn for {@link #RUN}. */
    private Result sendToBothInstances(String tenant) throws Exception {
        String authorization = TestJwt.bearer(tenant);
        long accepted = 0;
        int serverErrors = 0;
        long start = System.nanoTime();
        for (int i = 0; System.nanoTime() - start < RUN.toNanos(); i++) {
            int status = post(instances.get(i % 2), authorization).statusCode();
            if (status == 202) {
                accepted += BATCH;
            } else if (status >= 500) {
                serverErrors++;
            } else {
                assertThat(status).isEqualTo(429);
            }
        }
        return new Result(accepted, serverErrors, Duration.ofNanos(System.nanoTime() - start));
    }

    /**
     * Most that one bucket can admit in {@code elapsed}: a full burst plus the
     * refill, plus one batch of slack.
     */
    private static long planLimit(Duration elapsed) {
        return BURST + EVENTS_PER_SECOND * elapsed.toMillis() / 1000 + BATCH;
    }

    private HttpResponse<String> post(ConfigurableApplicationContext instance, String authorization) throws Exception {
        String port = instance.getEnvironment().getProperty("local.server.port");
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorization)
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(BATCH))))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static List<Double> fallbackGauges() {
        return instances.stream()
                .map(instance -> instance.getBean(MeterRegistry.class).get("ratelimit.fallback.active").gauge().value())
                .toList();
    }

    /** Per instance: Valkey operations timed with any of these outcomes. */
    private static List<Long> valkeyCalls(String... outcomes) {
        return instances.stream()
                .map(instance -> instance.getBean(MeterRegistry.class))
                .map(registry -> Arrays.stream(outcomes)
                        .mapToLong(outcome -> registry.get("ratelimit.valkey.duration").tag("outcome", outcome)
                                .timer().count())
                        .sum())
                .toList();
    }

    private record Result(long acceptedEvents, int serverErrors, Duration elapsed) {
    }
}
