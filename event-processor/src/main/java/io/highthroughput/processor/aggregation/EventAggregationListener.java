package io.highthroughput.processor.aggregation;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventEnvelopeCodec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;

/**
 * Consumes {@code events.v1} one record at a time and adds each event to its
 * per-minute aggregate, at most once per {@code (tenantId, eventId)}.
 *
 * <p>
 * Spring for Apache Kafka runs a poll loop per consumer thread
 * ({@code spring.kafka.listener.concurrency}) and
 * calls this method for every record. After all records of one poll were
 * handled, the container commits their offsets
 * ({@code ack-mode: batch}), always after the database transaction of each
 * record committed. A crash in between
 * redelivers records whose ledger rows already exist; they are skipped as
 * duplicates (ADR-0005,
 * delivery-guarantees.md).
 *
 * <p>
 * Exceptions go to the error handler in
 * {@code KafkaErrorHandlingConfiguration}.
 */
@Component
public class EventAggregationListener {

        private final EventEnvelopeCodec codec;
        private final EventStatsRepository repository;
        private final Clock clock;

        private final Counter aggregated;
        private final Counter duplicates;
        private final Timer upsertDuration;
        private final Timer endToEndLatency;

        public EventAggregationListener(EventEnvelopeCodec codec, EventStatsRepository repository, Clock clock,
                        MeterRegistry meterRegistry) {
                this.codec = codec;
                this.repository = repository;
                this.clock = clock;
                this.aggregated = Counter.builder("processor.events.aggregated")
                                .description("Events added to the per-minute aggregates")
                                .register(meterRegistry);
                this.duplicates = Counter.builder("processor.events.duplicate")
                                .description("Deliveries skipped because the event was already counted (deduplication ledger)")
                                .register(meterRegistry);
                this.upsertDuration = Timer.builder("processor.upsert.duration")
                                .description("Time for one ledger insert plus aggregate upsert transaction, including waiting for a"
                                                + " pooled connection")
                                .register(meterRegistry);
                this.endToEndLatency = Timer.builder("processor.end.to.end.latency")
                                .description("From the API receiving the event (receivedAt) to its aggregate being updated."
                                                + " Compares clocks of two processes; meaningful when they share a host or synced clocks.")
                                .register(meterRegistry);
        }

        /**
         * The listener id names the container (and its threads); the consumer group
         * comes from configuration.
         */
        @KafkaListener(id = "event-aggregation", idIsGroup = false, topics = "${processor.topic}")
        public void onEvent(ConsumerRecord<String, byte[]> record) {
                EventEnvelope event = codec.decode(record.value());
                boolean counted = upsertDuration.record(() -> repository.incrementOnce(event));
                if (!counted) {
                        duplicates.increment();
                        return;
                }
                aggregated.increment();
                endToEndLatency.record(Duration.between(event.receivedAt(), clock.instant()));
        }
}
