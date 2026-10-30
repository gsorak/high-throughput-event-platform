package io.highthroughput.ingest.publish;

import io.highthroughput.ingest.config.IngestionProperties;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Loads the topic's metadata into the Kafka producer before the API accepts
 * requests.
 *
 * <p>
 * The producer is created lazily, on the first send, and must then ask the
 * broker which partitions the topic has and
 * which broker leads each. On a freshly started JVM that took ~0.9 s locally —
 * longer than {@code max.block.ms}
 * (500 ms) — so without this the first request after every start was answered
 * with 503 although Kafka was healthy.
 *
 * <p>
 * When it runs matters. Spring starts {@link SmartLifecycle} beans in ascending
 * phase order once all beans exist, and
 * the embedded web server is one of them (phase
 * {@code Integer.MAX_VALUE - 2048}). Phase 0 therefore runs
 * <em>before</em> the server accepts requests, and <em>after</em>
 * {@code KafkaAdmin} created any declared topics
 * (it does so while beans are created). Fetching metadata earlier would make a
 * broker with topic auto-creation enabled
 * create the topic with default settings.
 *
 * <p>
 * Each {@code partitionsFor} call blocks for at most
 * {@code max.block.ms}; it is repeated until
 * {@code ingestion.kafka-warm-up-timeout}.
 * If Kafka is still unreachable
 * the application starts anyway and answers 503 until it is — the same as a
 * broker outage at runtime. Other errors (e.g. not authorized for the topic)
 * are misconfiguration and fail the startup.
 */
@Component
class ProducerWarmUp implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ProducerWarmUp.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final String topic;
    private final Duration timeout;
    private volatile boolean running;

    ProducerWarmUp(KafkaTemplate<String, byte[]> kafkaTemplate, IngestionProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.topic();
        this.timeout = properties.kafkaWarmUpTimeout();
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void start() {
        fetchTopicMetadata();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void fetchTopicMetadata() {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        for (int attempt = 1;; attempt++) {
            try {
                List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(topic);
                log.info("Kafka producer ready: {} partitions of {} known after {} attempt(s)", partitions.size(),
                        topic, attempt);
                return;
            } catch (TimeoutException e) {
                // Broker unreachable or topic unknown: partitionsFor waited max.block.ms. Try
                // again until the deadline.
                if (System.nanoTime() - deadlineNanos >= 0) {
                    log.warn("Metadata for topic {} not available after {} (broker unreachable or topic missing);"
                            + " starting anyway. Publishing answers 503 until it is available ({})", topic, timeout,
                            e.toString());
                    return;
                }
            }
        }
    }
}
