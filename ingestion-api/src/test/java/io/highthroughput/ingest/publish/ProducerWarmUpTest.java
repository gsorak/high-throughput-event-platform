package io.highthroughput.ingest.publish;

import io.highthroughput.ingest.config.IngestionProperties;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The three outcomes of the startup metadata fetch. A mocked
 * {@link KafkaTemplate} stands in for the broker, because
 * the interesting cases (slow, unreachable, misconfigured) are hard to produce
 * reliably with a real one.
 */
class ProducerWarmUpTest {

    private static final String TOPIC = "events.v1";
    private static final List<PartitionInfo> PARTITIONS = List.of(
            new PartitionInfo(TOPIC, 0, Node.noNode(), new Node[0], new Node[0]));

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, byte[]> kafkaTemplate = mock(KafkaTemplate.class);

    @Test
    void retriesUntilTheTopicMetadataIsAvailable() {
        when(kafkaTemplate.partitionsFor(TOPIC))
                .thenThrow(new TimeoutException("Topic events.v1 not present in metadata after 500 ms."))
                .thenThrow(new TimeoutException("Topic events.v1 not present in metadata after 500 ms."))
                .thenReturn(PARTITIONS);

        warmUp(Duration.ofSeconds(10)).start();

        verify(kafkaTemplate, times(3)).partitionsFor(TOPIC);
    }

    @Test
    void startsAnywayWhenKafkaStaysUnreachable() {
        when(kafkaTemplate.partitionsFor(TOPIC)).thenThrow(new TimeoutException("not present in metadata"));

        // Zero timeout: one attempt, then give up. The application starts and answers
        // 503 until Kafka is back.
        assertThatCode(() -> warmUp(Duration.ZERO).start()).doesNotThrowAnyException();
        verify(kafkaTemplate, times(1)).partitionsFor(TOPIC);
    }

    @Test
    void failsTheStartupOnMisconfiguration() {
        when(kafkaTemplate.partitionsFor(TOPIC)).thenThrow(new TopicAuthorizationException(Set.of(TOPIC)));

        assertThatThrownBy(() -> warmUp(Duration.ofSeconds(10)).start())
                .isInstanceOf(TopicAuthorizationException.class);
    }

    private ProducerWarmUp warmUp(Duration timeout) {
        IngestionProperties properties = new IngestionProperties(TOPIC, Duration.ofMillis(2500), Duration.ofSeconds(1),
                DataSize.ofMegabytes(1), Duration.ofDays(7), Duration.ofMinutes(5), timeout, 256);
        return new ProducerWarmUp(kafkaTemplate, properties);
    }
}
