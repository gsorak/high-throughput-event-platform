package io.highthroughput.ingest.publish;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.ingest.config.IngestionProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes a batch so that the caller knows whether <em>every</em> event was
 * acknowledged by Kafka.
 *
 * <p>
 * How it works:
 * <ol>
 * <li>Send every event without waiting. {@code KafkaTemplate.send} only appends
 * the record to the producer's
 * in-memory buffer and returns a future; a background I/O thread groups records
 * into requests
 * ({@code linger.ms}) and completes the futures when the broker answers.</li>
 * <li>Wait for all futures together, bounded by
 * {@code ingestion.publish-timeout}. So a batch costs roughly one
 * broker round trip, not one per event.</li>
 * </ol>
 * Any failure means "not all acknowledged" and becomes
 * {@link PublishFailedException}. Records that were acknowledged
 * stay in Kafka — there is no rollback without transactions (ADR-0002).
 *
 * <p>
 * Every wait is bounded by producer settings in {@code application.yaml}:
 * {@code max.block.ms} (how long
 * {@code send} may block when metadata is missing or the buffer is full) and
 * {@code delivery.timeout.ms} (when a
 * future must complete at the latest).
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final EventEnvelopeCodec codec;
    private final String topic;
    private final Duration publishTimeout;

    private final Timer acknowledgedTimer;
    private final Timer failedTimer;
    private final Counter acceptedEvents;

    public EventPublisher(KafkaTemplate<String, byte[]> kafkaTemplate, EventEnvelopeCodec codec,
            IngestionProperties properties, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
        this.topic = properties.topic();
        this.publishTimeout = properties.publishTimeout();
        this.acknowledgedTimer = publishTimer(meterRegistry, "acknowledged");
        this.failedTimer = publishTimer(meterRegistry, "failed");
        this.acceptedEvents = Counter.builder("ingestion.events.accepted")
                .description("Events in batches that Kafka fully acknowledged (answered 202)")
                .register(meterRegistry);
    }

    private static Timer publishTimer(MeterRegistry registry, String outcome) {
        return Timer.builder("ingestion.publish.duration")
                .description("Time from first send to the last acknowledgement of a batch")
                .tag("outcome", outcome)
                .register(registry);
    }

    /**
     * @throws PublishFailedException if not every event was acknowledged before the
     *                                deadline
     */
    public void publish(List<EventEnvelope> envelopes) {
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + publishTimeout.toNanos();
        try {
            List<CompletableFuture<?>> acknowledgements = sendAll(envelopes, deadlineNanos);
            awaitAll(acknowledgements, deadlineNanos);
        } catch (PublishFailedException e) {
            failedTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
            // No stack trace: during a broker outage this fires on every request.
            log.warn("Batch of {} events not fully acknowledged: {} ({})", envelopes.size(), e.getMessage(),
                    e.getCause() == null ? "no cause" : e.getCause().toString());
            throw e;
        }
        acknowledgedTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        acceptedEvents.increment(envelopes.size());
    }

    private List<CompletableFuture<?>> sendAll(List<EventEnvelope> envelopes, long deadlineNanos) {
        List<CompletableFuture<?>> acknowledgements = new ArrayList<>(envelopes.size());
        for (EventEnvelope envelope : envelopes) {
            if (System.nanoTime() - deadlineNanos >= 0) {
                throw new PublishFailedException("deadline reached after sending %d of %d events"
                        .formatted(acknowledgements.size(), envelopes.size()), null);
            }
            CompletableFuture<?> acknowledgement = send(envelope);
            if (acknowledgement.isCompletedExceptionally()) {
                // Failed before reaching the network: no metadata within max.block.ms, buffer
                // full, record too
                // large. The batch can no longer succeed, so stop instead of blocking on every
                // remaining event.
                throw new PublishFailedException("producer rejected event %d of %d; the rest were not sent"
                        .formatted(acknowledgements.size() + 1, envelopes.size()), acknowledgement.exceptionNow());
            }
            acknowledgements.add(acknowledgement);
        }
        return acknowledgements;
    }

    private CompletableFuture<?> send(EventEnvelope envelope) {
        try {
            return kafkaTemplate.send(new ProducerRecord<>(topic, envelope.tenantId(), codec.encode(envelope)));
        } catch (RuntimeException e) {
            // Most producer errors arrive as a failed future, but some are thrown directly.
            // Treat both the same way.
            return CompletableFuture.failedFuture(e);
        }
    }

    private static void awaitAll(List<CompletableFuture<?>> acknowledgements, long deadlineNanos) {
        try {
            CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new))
                    .get(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            long failed = acknowledgements.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            throw new PublishFailedException("%d of %d events were not acknowledged"
                    .formatted(failed, acknowledgements.size()), e.getCause());
        } catch (TimeoutException e) {
            long pending = acknowledgements.stream().filter(f -> !f.isDone()).count();
            throw new PublishFailedException("%d of %d events still unacknowledged at the deadline"
                    .formatted(pending, acknowledgements.size()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("interrupted while waiting for acknowledgements", e);
        }
    }
}
