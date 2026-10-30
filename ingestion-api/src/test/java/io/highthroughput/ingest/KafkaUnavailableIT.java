package io.highthroughput.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With the broker gone, the API must fail fast with 503 + Retry-After instead
 * of holding the request for the Kafka
 * client's default timeouts (minutes). The bound comes from max.block.ms,
 * delivery.timeout.ms and
 * ingestion.publish-timeout in application.yaml.
 *
 * <p>
 * {@link KafkaUnreachableAtStartupIT} covers a broker that is already down when
 * the application starts.
 *
 * <p>
 * {@code @DirtiesContext}: this test stops the Kafka container, so the context
 * must not be reused by other tests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@DirtiesContext
class KafkaUnavailableIT {

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Test
    void brokerOutageAnswers503WithinThePublishTimeout() throws Exception {
        // Warm up: the producer now has cached metadata and an open connection, like a
        // running service would.
        assertThat(post().statusCode()).isEqualTo(202);

        kafka.stop();

        long start = System.nanoTime();
        HttpResponse<String> response = post();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.headers().firstValue("Retry-After")).hasValue("1");
        // publish-timeout is 2.5 s; the rest is slack for a slow CI machine.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(4));
    }

    private HttpResponse<String> post() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer("outage-test"))
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(2))))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
