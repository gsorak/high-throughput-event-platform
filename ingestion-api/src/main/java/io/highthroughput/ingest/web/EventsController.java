package io.highthroughput.ingest.web;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.TenantIds;
import io.highthroughput.ingest.publish.EventPublisher;
import io.highthroughput.ingest.ratelimit.TenantRateLimiter;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * {@code POST /v1/events}: validate the whole batch → take one rate-limit token
 * per event → publish every event → 202.
 * The tenant comes only from the verified bearer token (see
 * {@code SecurityConfiguration}). Every failure is an
 * exception mapped to RFC 9457 problem details by {@link ApiExceptionHandler}.
 */
@RestController
public class EventsController {

    private final BatchValidator batchValidator;
    private final TenantRateLimiter rateLimiter;
    private final EventPublisher publisher;
    private final Clock clock;

    public EventsController(BatchValidator batchValidator, TenantRateLimiter rateLimiter, EventPublisher publisher,
            Clock clock) {
        this.batchValidator = batchValidator;
        this.rateLimiter = rateLimiter;
        this.publisher = publisher;
        this.clock = clock;
    }

    @PostMapping(path = "/v1/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AcceptedResponse> ingest(
            @AuthenticationPrincipal Jwt token,
            @RequestBody IngestEventsRequest request) {

        String tenantId = token.getClaimAsString(TenantIds.JWT_CLAIM);
        Instant receivedAt = clock.instant();
        batchValidator.validate(request, receivedAt);
        // After validation, so an invalid batch does not use up the tenant's tokens.
        rateLimiter.acquire(tenantId, request.events().size());

        List<EventEnvelope> envelopes = request.events().stream()
                .map(event -> event.toEnvelope(tenantId, receivedAt))
                .toList();
        publisher.publish(envelopes);

        return ResponseEntity.accepted().body(new AcceptedResponse(envelopes.size()));
    }
}
