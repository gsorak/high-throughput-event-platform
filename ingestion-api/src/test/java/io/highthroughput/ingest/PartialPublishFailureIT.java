package io.highthroughput.ingest;

import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * The ambiguous outcomes from docs/architecture/delivery-guarantees.md, made
 * concrete: a batch is partly written, the
 * client gets 503, retries the whole batch with the same eventIds, and the
 * topic ends up with duplicates.
 *
 * <p>
 * The failure is injected into the 3rd {@code send} of a 5-event batch; all
 * other sends reach the real broker.
 * These tests pin Stage 1 behavior on purpose. The duplicates are expected; the
 * stable eventIds are what make them
 * removable later. Deduplication (Stage 3) happens in the processor, so these
 * tests will stay as they are: the topic
 * keeps containing duplicates.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class PartialPublishFailureIT {

    private final HttpClient http = HttpClient.newHttpClient();
    private final EventEnvelopeCodec codec = new EventEnvelopeCodec();

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    /**
     * A Mockito spy wraps the real KafkaTemplate bean: calls go through unless a
     * test stubs them. The type must match
     * the bean definition exactly, and Spring Boot declares its template as
     * {@code KafkaTemplate<?, ?>}.
     */
    @MockitoSpyBean
    KafkaTemplate<?, ?> kafkaTemplate;

    @Test
    void brokerSideFailureAnswers503AndTheRetryDuplicatesTheOtherEvents() throws Exception {
        String tenant = TestBatches.uniqueTenant();
        List<UUID> ids = TestBatches.randomIds(5);
        String body = TestBatches.withEventIds(ids);
        failThirdSend(PartialPublishFailureIT::failsAfterBeingSent);

        HttpResponse<String> first = post(tenant, body);

        assertThat(first.statusCode()).isEqualTo(503);
        assertThat(first.headers().firstValue("Retry-After")).hasValue("1");
        // The other four were acknowledged and cannot be taken back: the outcome is
        // partial, not "nothing happened".
        assertThat(eventIdCounts(tenant)).containsOnlyKeys(ids.get(0), ids.get(1), ids.get(3), ids.get(4));

        HttpResponse<String> retry = post(tenant, body);

        assertThat(retry.statusCode()).isEqualTo(202);
        assertThat(eventIdCounts(tenant)).as("same eventIds; only the failed event is present once")
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        ids.get(0), 2L, ids.get(1), 2L, ids.get(2), 1L, ids.get(3), 2L, ids.get(4), 2L));
    }

    @Test
    void immediateRejectionStopsTheBatchAndTheRetryDuplicatesTheEventsSentBeforeIt() throws Exception {
        String tenant = TestBatches.uniqueTenant();
        List<UUID> ids = TestBatches.randomIds(5);
        String body = TestBatches.withEventIds(ids);
        failThirdSend(PartialPublishFailureIT::rejectedImmediately);

        HttpResponse<String> first = post(tenant, body);

        assertThat(first.statusCode()).isEqualTo(503);
        // Events 4 and 5 were never sent: once one send is rejected the batch cannot
        // succeed.
        assertThat(eventIdCounts(tenant)).containsOnlyKeys(ids.get(0), ids.get(1));

        HttpResponse<String> retry = post(tenant, body);

        assertThat(retry.statusCode()).isEqualTo(202);
        assertThat(eventIdCounts(tenant))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        ids.get(0), 2L, ids.get(1), 2L, ids.get(2), 1L, ids.get(3), 1L, ids.get(4), 1L));
    }

    /**
     * The 3rd send of the next request uses {@code failure}; every other send goes
     * to the real broker.
     */
    @SuppressWarnings("unchecked")
    private void failThirdSend(Supplier<CompletableFuture<?>> failure) {
        AtomicInteger sends = new AtomicInteger();
        doAnswer(invocation -> sends.incrementAndGet() == 3 ? failure.get() : invocation.callRealMethod())
                .when(kafkaTemplate).send(any(ProducerRecord.class));
    }

    /**
     * Like a broker error response: the future fails a little later, while the
     * other records are in flight.
     */
    private static CompletableFuture<?> failsAfterBeingSent() {
        CompletableFuture<?> future = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS)
                .execute(() -> future.completeExceptionally(new KafkaException("simulated broker error")));
        return future;
    }

    /**
     * Like {@code send()} failing at once, e.g. no topic metadata within
     * max.block.ms.
     */
    private static CompletableFuture<?> rejectedImmediately() {
        return CompletableFuture.failedFuture(new KafkaException("simulated producer rejection"));
    }

    private Map<UUID, Long> eventIdCounts(String tenant) {
        List<ConsumerRecord<String, byte[]>> records = TopicContents.readAll(kafka.getBootstrapServers(),
                Topics.EVENTS);
        return records.stream()
                .filter(record -> tenant.equals(record.key()))
                .map(record -> codec.decode(record.value()).eventId())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private HttpResponse<String> post(String tenant, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(tenant))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
