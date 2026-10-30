package io.highthroughput.processor.aggregation;

import io.highthroughput.contract.EnvelopeDecodingException;
import io.highthroughput.contract.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

/**
 * Called by the error handler for a record that will not be retried (any more).
 * Publishes the record unchanged to
 * {@link Topics#EVENTS_DEAD_LETTER}, same partition number, with Spring Kafka's
 * {@code kafka_dlt-*} headers
 * (exception class and message, original topic, partition and offset), then
 * counts and logs it.
 *
 * <p>
 * If publishing fails or times out, this method throws. The error handler then
 * seeks back to the record instead of
 * committing its offset, and tries again: a record leaves its partition only
 * through the aggregate or through the
 * dead-letter topic.
 */
public class DeadLetterRecoverer implements ConsumerAwareRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRecoverer.class);

    private final DeadLetterPublishingRecoverer publisher;
    private final MeterRegistry meterRegistry;

    public DeadLetterRecoverer(KafkaOperations<?, ?> template, MeterRegistry meterRegistry) {
        this.publisher = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(Topics.EVENTS_DEAD_LETTER, record.partition()));
        // The default, stated because the guarantee above depends on it: a failed send
        // throws.
        this.publisher.setFailIfSendResultIsError(true);
        this.meterRegistry = meterRegistry;
        // Register up front so the series exists (at 0) before the first record; alerts
        // can then use increase().
        deadLetteredCounter("malformed");
        deadLetteredCounter("unprocessable");
        deadLetteredCounter("retries-exhausted");
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception exception) {
        String reason = hasCause(exception, EnvelopeDecodingException.class) ? "malformed"
                : hasCause(exception, DataIntegrityViolationException.class) ? "unprocessable"
                        : "retries-exhausted";
        publisher.accept(record, consumer, exception);
        deadLetteredCounter(reason).increment();
        // The payload is not logged: it may be large and contains tenant data.
        // topic-partition@offset locates it.
        log.warn("Dead-lettered record {}-{}@{} ({}): {}", record.topic(), record.partition(), record.offset(),
                reason, rootMessage(exception));
    }

    private Counter deadLetteredCounter(String reason) {
        return Counter.builder("processor.events.dead_lettered")
                .description("Records not aggregated and published to the dead-letter topic, by reason")
                .tag("reason", reason)
                .register(meterRegistry);
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> type) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static String rootMessage(Throwable exception) {
        Throwable root = exception;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
