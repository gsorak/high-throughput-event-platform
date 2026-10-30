package io.highthroughput.ingest;

import io.highthroughput.ingest.publish.EventPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Admission control (ADR-0004): with every permit held by a request that waits
 * for Kafka, the next request is
 * rejected at once instead of queueing.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "ingestion.max-in-flight=2")
@Import(TestcontainersConfiguration.class)
class OverloadIT {

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int port;

    /**
     * Stands in for a slow Kafka: each publish blocks until the test releases it.
     */
    @MockitoBean
    EventPublisher publisher;

    @Test
    void requestBeyondMaxInFlightIsRejectedImmediately() throws Exception {
        CountDownLatch publishing = new CountDownLatch(2);
        CountDownLatch kafkaAcknowledges = new CountDownLatch(1);
        doAnswer(invocation -> {
            publishing.countDown();
            kafkaAcknowledges.await(30, TimeUnit.SECONDS);
            return null;
        }).when(publisher).publish(any());

        CompletableFuture<HttpResponse<String>> first = http.sendAsync(request(), HttpResponse.BodyHandlers.ofString());
        CompletableFuture<HttpResponse<String>> second = http.sendAsync(request(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        HttpResponse<String> rejected = http.send(request(), HttpResponse.BodyHandlers.ofString());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(rejected.statusCode()).isEqualTo(503);
        assertThat(rejected.headers().firstValue("Retry-After")).hasValue("1");
        assertThat(rejected.body()).contains("\"title\":\"Server busy\"");
        assertThat(elapsed).isLessThan(Duration.ofSeconds(1));

        kafkaAcknowledges.countDown();
        assertThat(first.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(202);
        assertThat(second.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(202);
        // Permits are returned: the next request is admitted again.
        assertThat(http.send(request(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(202);
    }

    private HttpRequest request() {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(TestBatches.uniqueTenant()))
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(1))))
                .build();
    }
}
