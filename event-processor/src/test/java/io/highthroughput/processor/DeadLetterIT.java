package io.highthroughput.processor;

import io.highthroughput.contract.EnvelopeDecodingException;
import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.contract.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static io.highthroughput.processor.TestEventProducer.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Failure handling in the processor (KafkaErrorHandlingConfiguration):
 * permanent failures go to
 * {@code events.v1.DLT} without blocking the partition, a failed dead-letter
 * publish keeps the offset uncommitted,
 * and transient database failures are retried until they succeed, never
 * dead-lettered.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(ProcessorTestcontainersConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
class DeadLetterIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    DataSource dataSource;

    @Autowired
    MeterRegistry meterRegistry;

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
    void malformedRecordGoesToTheDeadLetterTopicWithoutBlockingThePartition() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        double before = deadLettered("malformed");
        byte[] malformed = "{not json".getBytes(StandardCharsets.UTF_8);

        // Same key → same partition, so the valid event is behind the malformed one.
        RecordMetadata sent = producer.sendRaw(tenant, malformed);
        producer.send(event(tenant, "api.call", minute.plusSeconds(1), null));

        await().atMost(TIMEOUT).until(() -> stats.totalCount(tenant, from, to) == 1);
        assertThat(deadLettered("malformed")).isEqualTo(before + 1);
        assertThat(deadLetters(tenant)).singleElement().satisfies(record -> {
            assertThat(record.value()).isEqualTo(malformed);
            assertThat(record.partition()).isEqualTo(sent.partition());
            assertThat(header(record, "kafka_dlt-original-topic")).isEqualTo(Topics.EVENTS);
            assertThat(ByteBuffer.wrap(record.headers().lastHeader("kafka_dlt-original-offset").value()).getLong())
                    .isEqualTo(sent.offset());
            assertThat(header(record, "kafka_dlt-exception-cause-fqcn"))
                    .isEqualTo(EnvelopeDecodingException.class.getName());
        });
    }

    /**
     * Two values near the double maximum sum to infinity, which PostgreSQL rejects
     * (SQLSTATE 22003). The API bounds
     * values, so this can only come from a producer that bypasses it. The ledger
     * row of the failed event is rolled
     * back with the upsert, so the event would be counted if it is redriven after a
     * fix.
     */
    @Test
    void unprocessableRecordGoesToTheDeadLetterTopicAndLeavesNoLedgerRow() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        double before = deadLettered("unprocessable");
        EventEnvelope first = event(tenant, "api.call", minute.plusSeconds(1), 1e308);
        EventEnvelope overflowing = event(tenant, "api.call", minute.plusSeconds(2), 1e308);

        producer.send(first);
        producer.send(overflowing);
        producer.send(event(tenant, "login", minute.plusSeconds(3), null));

        await().atMost(TIMEOUT).until(() -> stats.totalCount(tenant, from, to) == 2);
        assertThat(deadLettered("unprocessable")).isEqualTo(before + 1);
        assertThat(deadLetters(tenant)).singleElement()
                .satisfies(record -> assertThat(new EventEnvelopeCodec().decode(record.value()).eventId())
                        .isEqualTo(overflowing.eventId()));
        assertThat(ledgerRows(first)).isEqualTo(1);
        assertThat(ledgerRows(overflowing)).isZero();
    }

    /**
     * While the dead-letter topic does not exist, the malformed record can be
     * neither processed nor dead-lettered.
     * The publish fails (after delivery.timeout.ms, 15 s), the offset is not
     * committed and the event behind it waits;
     * once the topic is back, both complete.
     */
    @Test
    void failedDeadLetterPublishLeavesTheOffsetUncommitted(CapturedOutput output) throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafka.getBootstrapServers()))) {
            RecordMetadata malformed;
            TopicPartition partition;
            try {
                admin.deleteTopics(List.of(Topics.EVENTS_DEAD_LETTER)).all().get();
                await().atMost(TIMEOUT)
                        .until(() -> !admin.listTopics().names().get().contains(Topics.EVENTS_DEAD_LETTER));

                malformed = producer.sendRaw(tenant, "{not json".getBytes(StandardCharsets.UTF_8));
                producer.send(event(tenant, "api.call", minute.plusSeconds(1), null));
                partition = new TopicPartition(Topics.EVENTS, malformed.partition());

                await().atMost(Duration.ofSeconds(40)).until(() -> output.getAll()
                        .contains("Dead-letter publication to " + Topics.EVENTS_DEAD_LETTER));
                // After the failed publish: still nothing committed past the record, nothing
                // behind it processed.
                await().during(Duration.ofSeconds(5)).atMost(Duration.ofSeconds(8))
                        .until(() -> stats.totalCount(tenant, from, to) == 0
                                && committedOffset(admin, partition) <= malformed.offset());
            } finally {
                // Deletion completes asynchronously; creating too early fails with
                // TopicExistsException.
                await().atMost(TIMEOUT).ignoreExceptions().untilAsserted(() -> admin
                        .createTopics(List.of(new NewTopic(Topics.EVENTS_DEAD_LETTER, 3, (short) 1))).all().get());
            }

            await().atMost(Duration.ofSeconds(60)).until(() -> stats.totalCount(tenant, from, to) == 1);
            await().atMost(TIMEOUT).until(() -> committedOffset(admin, partition) > malformed.offset());
            assertThat(deadLetters(tenant)).hasSize(1);
        }
    }

    /**
     * A table lock held by another transaction makes every insert into the ledger
     * wait until the 5 s query timeout
     * cancels it (SQLSTATE 57014, transient). The record is retried, not
     * dead-lettered, and counted once the lock is
     * released.
     */
    @Test
    void transientDatabaseFailureIsRetriedUntilItSucceeds() throws Exception {
        String tenant = EventAggregationIT.uniqueTenant();
        double before = deadLetteredTotal();
        long attemptsBefore = meterRegistry.get("processor.upsert.duration").timer().count();
        double transientFailuresBefore = transientFailures();

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("LOCK TABLE processed_events IN ACCESS EXCLUSIVE MODE");
            try {
                producer.send(event(tenant, "api.call", minute.plusSeconds(1), null));
                producer.send(event(tenant, "api.call", minute.plusSeconds(2), null));

                await().during(Duration.ofSeconds(12)).atMost(Duration.ofSeconds(15))
                        .until(() -> stats.totalCount(tenant, from, to) == 0);
            } finally {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        }

        await().atMost(TIMEOUT).until(() -> stats.totalCount(tenant, from, to) == 2);
        // More transactions than events: at least one attempt was cancelled and
        // retried.
        assertThat(meterRegistry.get("processor.upsert.duration").timer().count() - attemptsBefore)
                .isGreaterThan(2);
        assertThat(transientFailures()).isGreaterThan(transientFailuresBefore);
        assertThat(deadLetteredTotal()).isEqualTo(before);
        assertThat(deadLetters(tenant)).isEmpty();
    }

    private double transientFailures() {
        return meterRegistry.get("processor.delivery.failures").tag("class", "transient").counter().count();
    }

    private double deadLettered(String reason) {
        return meterRegistry.get("processor.events.dead_lettered").tag("reason", reason).counter().count();
    }

    private double deadLetteredTotal() {
        return meterRegistry.get("processor.events.dead_lettered").counters().stream()
                .mapToDouble(counter -> counter.count()).sum();
    }

    private List<ConsumerRecord<String, byte[]>> deadLetters(String tenant) {
        return TopicContents.readAll(kafka.getBootstrapServers(), Topics.EVENTS_DEAD_LETTER).stream()
                .filter(record -> tenant.equals(record.key()))
                .toList();
    }

    private long committedOffset(Admin admin, TopicPartition partition) throws Exception {
        OffsetAndMetadata committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                .get(partition);
        return committed == null ? 0 : committed.offset();
    }

    private long ledgerRows(EventEnvelope event) {
        return jdbc.sql("SELECT count(*) FROM processed_events WHERE tenant_id = ? AND event_id = ?")
                .params(event.tenantId(), event.eventId())
                .query(Long.class).single();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
