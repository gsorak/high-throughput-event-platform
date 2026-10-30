package io.highthroughput.ingest.web;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventTypes;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * One event as sent by the client. Field rules that need "now" (the
 * {@code occurredAt} window) or the whole batch
 * (duplicate ids) live in {@link BatchValidator}; everything static is declared
 * here.
 */
public record EventPayload(
        @NotNull UUID eventId,
        @NotNull @Size(max = EventTypes.MAX_LENGTH) @Pattern(regexp = EventTypes.PATTERN) String type,
        @NotNull Instant occurredAt,
        @Size(min = 1, max = 64) String sourceId,
        Double value,
        @Size(max = 16) Map<@NotNull @Size(min = 1, max = 64) String, @NotNull @Size(max = 256) String> attributes) {

    EventEnvelope toEnvelope(String tenantId, Instant receivedAt) {
        return new EventEnvelope(EventEnvelope.CURRENT_SCHEMA_VERSION, tenantId, eventId, type, occurredAt,
                receivedAt, sourceId, value, attributes);
    }
}
