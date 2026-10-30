package com.gsorak.eventplatform.service;

import com.gsorak.eventplatform.api.EventRequest;
import com.gsorak.eventplatform.kafka.EventPublisher;
import com.gsorak.eventplatform.ratelimit.DistributedRateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EventIngestionService {

    private final DistributedRateLimiter rateLimiter;
    private final EventPublisher eventPublisher;
    private final MeterRegistry meterRegistry;

    public EventIngestionService(
            DistributedRateLimiter rateLimiter,
            EventPublisher eventPublisher,
            MeterRegistry meterRegistry
    ) {
        this.rateLimiter = rateLimiter;
        this.eventPublisher = eventPublisher;
        this.meterRegistry = meterRegistry;
    }

    public CompletableFuture<IngestionResult> ingest(EventRequest event, String clientId) {
        if (!rateLimiter.tryAcquire(clientId)) {
            meterRegistry.counter("events.ingested", "outcome", "rate_limited").increment();
            return CompletableFuture.completedFuture(IngestionResult.RATE_LIMITED);
        }

        try {
            return eventPublisher.publish(event)
                    .thenApply(ignored -> {
                        meterRegistry.counter("events.ingested", "outcome", "accepted").increment();
                        return IngestionResult.ACCEPTED;
                    })
                    .exceptionally(error -> {
                        meterRegistry.counter("events.ingested", "outcome", "unavailable").increment();
                        return IngestionResult.UNAVAILABLE;
                    });
        } catch (RuntimeException exception) {
            meterRegistry.counter("events.ingested", "outcome", "unavailable").increment();
            return CompletableFuture.completedFuture(IngestionResult.UNAVAILABLE);
        }
    }
}
