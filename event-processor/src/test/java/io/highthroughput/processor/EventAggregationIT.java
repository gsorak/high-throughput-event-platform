package io.highthroughput.processor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static io.highthroughput.processor.TestEventProducer.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Kafka → listener → PostgreSQL → {@code GET /v1/stats}, with real containers.
 * Processing is asynchronous, so the
 * assertions poll with Awaitility until the expected state appears (or fail
 * after the timeout).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(ProcessorTestcontainersConfiguration.class)
class EventAggregationIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    JdbcClient jdbc;

    private TestEventProducer producer;
    private StatsClient stats;

    /**
     * A whole minute in the recent past, so all test events fall inside the 7-day
     * window and a known bucket.
     */
    private final Instant minute = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(10, ChronoUnit.MINUTES);
    private final Instant from = minute.minus(1, ChronoUnit.MINUTES);
    private final Instant to = minute.plus(5, ChronoUnit.MINUTES);

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
    void flywayCreatedTheSchema() {
        String version = jdbc.sql("SELECT max(version) FROM flyway_schema_history WHERE success")
                .query(String.class).single();
        assertThat(version).isEqualTo("2");
    }

    @Test
    void eventsAreAggregatedPerMinuteAndType() throws Exception {
        String tenant = uniqueTenant();
        producer.send(event(tenant, "api.call", minute.plusSeconds(5), 1.5));
        producer.send(event(tenant, "api.call", minute.plusSeconds(40), 2.5));
        producer.send(event(tenant, "login", minute.plusSeconds(50), null));
        producer.send(event(tenant, "api.call", minute.plusSeconds(65), null));

        await().atMost(TIMEOUT).until(() -> stats.totalCount(tenant, from, to) == 4);

        JsonNode body = stats.get(tenant, from, to, null);
        assertThat(body.get("totalValueSum").asDouble()).isEqualTo(4.0);
        assertThat(body.get("truncated").asBoolean()).isFalse();
        JsonNode buckets = body.get("buckets");
        assertThat(buckets).hasSize(3);
        assertBucket(buckets.get(0), minute, "api.call", 2, 4.0);
        assertBucket(buckets.get(1), minute, "login", 1, 0.0);
        assertBucket(buckets.get(2), minute.plus(1, ChronoUnit.MINUTES), "api.call", 1, 0.0);

        JsonNode logins = stats.get(tenant, from, to, "login");
        assertThat(logins.get("totalCount").asLong()).isEqualTo(1);
        assertThat(logins.get("buckets")).hasSize(1);
    }

    @Test
    void tenantsSeeOnlyTheirOwnAggregates() throws Exception {
        String tenantA = uniqueTenant();
        String tenantB = uniqueTenant();
        producer.send(event(tenantA, "api.call", minute.plusSeconds(1), 10.0));
        producer.send(event(tenantA, "api.call", minute.plusSeconds(2), 10.0));
        producer.send(event(tenantB, "api.call", minute.plusSeconds(3), 1.0));

        await().atMost(TIMEOUT)
                .until(() -> stats.totalCount(tenantA, from, to) == 2 && stats.totalCount(tenantB, from, to) == 1);

        assertThat(stats.get(tenantB, from, to, null).get("totalValueSum").asDouble()).isEqualTo(1.0);
        assertThat(stats.totalCount(uniqueTenant(), from, to)).isZero();
    }

    private static void assertBucket(JsonNode bucket, Instant start, String type, long count, double valueSum) {
        assertThat(Instant.parse(bucket.get("bucketStart").asString())).isEqualTo(start);
        assertThat(bucket.get("type").asString()).isEqualTo(type);
        assertThat(bucket.get("count").asLong()).isEqualTo(count);
        assertThat(bucket.get("valueSum").asDouble()).isEqualTo(valueSum);
    }

    static String uniqueTenant() {
        return "t-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
