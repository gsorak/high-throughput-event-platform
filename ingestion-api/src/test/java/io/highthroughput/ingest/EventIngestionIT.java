package io.highthroughput.ingest;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Full HTTP → Kafka path against a real broker: the application listens on a
 * random port and is called over HTTP
 * with the JDK client; the topic is read back with a plain Kafka consumer.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class EventIngestionIT {

    private final HttpClient http = HttpClient.newHttpClient();
    private final EventEnvelopeCodec codec = new EventEnvelopeCodec();

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    KafkaTemplate<String, byte[]> kafkaTemplate;

    @Test
    void producerHasTheDeclaredTopicMetadataBeforeTheFirstRequest() {
        // ProducerWarmUp fetched the metadata during startup. Had it run before
        // KafkaAdmin created the declared topic
        // (3 partitions), the test broker would have auto-created it with 1 partition,
        // and the producer would keep
        // that cached view until its next metadata refresh (5 min).
        assertThat(kafkaTemplate.partitionsFor(Topics.EVENTS)).hasSize(TestcontainersConfiguration.PARTITIONS);
    }

    @Test
    void acceptedEventsAreInKafkaKeyedByTenant() throws Exception {
        String tenant = TestBatches.uniqueTenant();
        List<UUID> ids = TestBatches.randomIds(3);
        Instant before = Instant.now();

        HttpResponse<String> response = post(tenant, TestBatches.withEventIds(ids));

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.body()).isEqualTo("{\"accepted\":3}");

        List<ConsumerRecord<String, byte[]>> records = recordsOf(tenant);
        assertThat(records).hasSize(3);
        assertThat(records).allSatisfy(record -> assertThat(record.key()).isEqualTo(tenant));
        // One key → one partition: all events of a tenant land in the same partition
        // (ADR-0003).
        assertThat(records).extracting(ConsumerRecord::partition).containsOnly(records.getFirst().partition());

        List<EventEnvelope> envelopes = records.stream().map(r -> codec.decode(r.value())).toList();
        assertThat(envelopes).extracting(EventEnvelope::eventId).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(envelopes).allSatisfy(envelope -> {
            assertThat(envelope.schemaVersion()).isEqualTo(EventEnvelope.CURRENT_SCHEMA_VERSION);
            assertThat(envelope.tenantId()).isEqualTo(tenant);
            assertThat(envelope.type()).isEqualTo("api.call");
            assertThat(envelope.value()).isEqualTo(1.0);
            assertThat(envelope.receivedAt()).isCloseTo(before, within(10, ChronoUnit.SECONDS));
        });
    }

    @Test
    void rejectedBatchWritesNothing() throws Exception {
        String tenant = TestBatches.uniqueTenant();
        String body = """
                {"events": [
                  {"eventId": "%s", "type": "api.call", "occurredAt": "%s"},
                  {"eventId": "%s", "type": "INVALID", "occurredAt": "%s"}
                ]}""".formatted(UUID.randomUUID(), Instant.now(), UUID.randomUUID(), Instant.now());

        HttpResponse<String> response = post(tenant, body);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().firstValue("Content-Type")).hasValue("application/problem+json");
        assertThat(recordsOf(tenant)).isEmpty();
    }

    /**
     * A chunked upload has no Content-Length, so the limit can only be enforced
     * while reading. This needs a real
     * server: MockMvc always knows the body length.
     */
    @Test
    void oversizedChunkedBodyIsRejected() throws Exception {
        byte[] body = new byte[2 * 1024 * 1024];
        Arrays.fill(body, (byte) ' ');
        HttpRequest request = HttpRequest.newBuilder(eventsUri())
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(TestBatches.uniqueTenant()))
                // An InputStream publisher has unknown length, so the client sends
                // Transfer-Encoding: chunked.
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(body)))
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(413);
    }

    private HttpResponse<String> post(String tenant, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(eventsUri())
                .header("Content-Type", "application/json")
                .header("Authorization", TestJwt.bearer(tenant))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI eventsUri() {
        return URI.create("http://localhost:" + port + "/v1/events");
    }

    private List<ConsumerRecord<String, byte[]>> recordsOf(String tenant) {
        return TopicContents.readAll(kafka.getBootstrapServers(), Topics.EVENTS).stream()
                .filter(record -> tenant.equals(record.key()))
                .toList();
    }
}
