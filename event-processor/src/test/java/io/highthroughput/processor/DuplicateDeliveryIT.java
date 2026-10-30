package io.highthroughput.processor;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.Topics;
import io.highthroughput.processor.aggregation.LedgerRetentionJob;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static io.highthroughput.processor.TestEventProducer.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The deduplication ledger (ADR-0005): every way the same event can reach the
 * processor twice is counted once,
 * within the 21-day window. See docs/architecture/delivery-guarantees.md.
 *
 * <p>
 * Most tests send a marker event behind the duplicates, with the same key and
 * so in the same partition. Once the
 * marker is aggregated, every copy before it has been handled, whatever the
 * outcome; the count can then be checked
 * without waiting for a timeout.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(ProcessorTestcontainersConfiguration.class)
class DuplicateDeliveryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Autowired
    LedgerRetentionJob retentionJob;

    @Value("${spring.kafka.consumer.group-id}")
    String groupId;

    private final Instant minute = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(10, ChronoUnit.MINUTES);
    private final Instant from = minute;
    private final Instant to = minute.plus(1, ChronoUnit.MINUTES);

    private TestEventProducer producer;
    private StatsClient stats;

    @BeforeEach
    void setUp() {
        producer = new TestEventProducer(kafka.getBootstrapServers());
        stats = new StatsClient(port);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void sameEventDeliveredTwiceIsCountedOnce() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        EventEnvelope event = event(tenant, "api.call", minute.plusSeconds(1), 1.0);

        producer.send(event);
        producer.send(event); // identical envelope, identical eventId
        sendMarkerAndAwait(tenant);

        assertThat(count(tenant, "api.call")).isEqualTo(1);
        assertThat(stats.get(tenant, from, to, "api.call").get("totalValueSum").asDouble()).isEqualTo(1.0);
    }

    /**
     * A client retries a whole batch after a 503 although Kafka already had part of
     * it (PartialPublishFailureIT in
     * ingestion-api): the processor sees a, b, then a, b, c.
     */
    @Test
    void retriedBatchIsCountedOnce() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        EventEnvelope a = event(tenant, "api.call", minute.plusSeconds(1), null);
        EventEnvelope b = event(tenant, "api.call", minute.plusSeconds(2), null);
        EventEnvelope c = event(tenant, "api.call", minute.plusSeconds(3), null);

        producer.send(a);
        producer.send(b);
        producer.send(a);
        producer.send(b);
        producer.send(c);
        sendMarkerAndAwait(tenant);

        assertThat(count(tenant, "api.call")).isEqualTo(3);
    }

    /**
     * The ledger key is (tenantId, eventId): one tenant's event ID never suppresses
     * another tenant's event.
     */
    @Test
    void sameEventIdUnderTwoTenantsIsCountedForBoth() throws Exception {
        String tenantA = EventAggregationIT.uniqueTenant();
        String tenantB = EventAggregationIT.uniqueTenant();
        EventEnvelope a = event(tenantA, "api.call", minute.plusSeconds(1), null);
        EventEnvelope b = new EventEnvelope(a.schemaVersion(), tenantB, a.eventId(), a.type(), a.occurredAt(),
                a.receivedAt(), a.sourceId(), a.value(), a.attributes());

        producer.send(a);
        producer.send(b);
        sendMarkerAndAwait(tenantA);
        sendMarkerAndAwait(tenantB);

        assertThat(count(tenantA, "api.call")).isEqualTo(1);
        assertThat(count(tenantB, "api.call")).isEqualTo(1);
    }

    /**
     * Simulates a crash after the database commits but before the Kafka offset
     * commit: the consumers stop, the
     * group's offset is moved back to the first event, and on restart every event
     * is delivered again.
     */
    @Test
    void redeliveryAfterRestartIsCountedOnce() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        int events = 200;
        RecordMetadata first = producer.send(event(tenant, "api.call", minute.plusSeconds(1), null));
        for (int i = 1; i < events; i++) {
            producer.send(event(tenant, "api.call", minute.plusSeconds(1 + i % 50), null));
        }
        await().atMost(TIMEOUT).until(() -> count(tenant, "api.call") == events);
        double duplicatesBefore = meterRegistry.get("processor.events.duplicate").counter().count();

        MessageListenerContainer container = listeners.getListenerContainer("event-aggregation");
        container.stop();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()))) {
            TopicPartition partition = new TopicPartition(Topics.EVENTS, first.partition());
            // Possible only once the stopped consumers have left the group.
            await().atMost(TIMEOUT).ignoreExceptions().untilAsserted(() -> admin
                    .alterConsumerGroupOffsets(groupId, Map.of(partition, new OffsetAndMetadata(first.offset())))
                    .all().get());
        } finally {
            container.start();
        }
        sendMarkerAndAwait(tenant);

        assertThat(count(tenant, "api.call")).isEqualTo(events);
        assertThat(meterRegistry.get("processor.events.duplicate").counter().count())
                .isEqualTo(duplicatesBefore + events);
    }

    /**
     * The documented boundary of the 21-day window: a copy arriving while the
     * ledger row exists is skipped; once the
     * retention job deleted the row, it is counted again.
     */
    @Test
    void duplicatesAreSkippedOnlyWhileTheLedgerRowIsRetained() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        EventEnvelope recent = event(tenant, "recent.copy", minute.plusSeconds(1), null);
        EventEnvelope expired = event(tenant, "expired.copy", minute.plusSeconds(1), null);
        Instant now = Instant.now();
        insertLedgerRow(recent, now.minus(Duration.ofDays(20)));
        insertLedgerRow(expired, now.minus(Duration.ofDays(22)));

        assertThat(retentionJob.deleteExpired()).isGreaterThanOrEqualTo(1);
        assertThat(ledgerRows(recent)).isEqualTo(1);
        assertThat(ledgerRows(expired)).isZero();

        producer.send(recent);
        producer.send(expired);
        sendMarkerAndAwait(tenant);

        assertThat(count(tenant, "recent.copy")).isZero();
        assertThat(count(tenant, "expired.copy")).isEqualTo(1);
    }

    private void sendMarkerAndAwait(String tenant) throws Exception {
        producer.send(event(tenant, "test.marker", minute.plusSeconds(59), null));
        await().atMost(TIMEOUT).until(() -> count(tenant, "test.marker") == 1);
    }

    private long count(String tenant, String type) {
        return stats.get(tenant, from, to, type).get("totalCount").asLong();
    }

    private void insertLedgerRow(EventEnvelope event, Instant processedAt) {
        jdbc.sql("INSERT INTO processed_events (tenant_id, event_id, processed_at) VALUES (?, ?, ?)")
                .params(event.tenantId(), event.eventId(), OffsetDateTime.ofInstant(processedAt, ZoneOffset.UTC))
                .update();
    }

    private long ledgerRows(EventEnvelope event) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE tenant_id = ? AND event_id = ?")
                .params(event.tenantId(), event.eventId())
                .query(Long.class).single();
    }
}
