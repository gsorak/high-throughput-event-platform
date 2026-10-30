package io.highthroughput.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka is unreachable while the application starts. {@code ProducerWarmUp}
 * gives up after
 * {@code ingestion.kafka-warm-up-timeout}, the application starts anyway, and
 * requests fail fast with 503 +
 * Retry-After. Without metadata the first {@code send} blocks for
 * {@code max.block.ms}; the publisher then stops
 * sending, so the answer time does not grow with the batch size.
 * {@link KafkaUnavailableIT} covers an outage after
 * startup.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // No Kafka broker listens on port 1.
        "spring.kafka.bootstrap-servers=localhost:1",
        "ingestion.kafka-warm-up-timeout=1s",
        // Nor Valkey: requests are rate-limited locally.
        "rate-limit.valkey-uri=redis://localhost:1" })
class KafkaUnreachableAtStartupIT {

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void jwtKey(DynamicPropertyRegistry registry) {
        registry.add("security.jwt.public-key-location", TestJwt::publicKeyLocation);
    }

    @Test
    void startsAndAnswers503WithinThePublishTimeout() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(TestBatches.uniqueTenant()))
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(100))))
                .build();

        long start = System.nanoTime();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).hasValue("1");
        // One max.block.ms (500 ms) for the first event instead of one per event; the
        // rest is slack for a slow machine.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(4));
    }
}
