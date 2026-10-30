package io.highthroughput.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The value of every record on {@link Topics#EVENTS}: one validated event plus
 * the metadata the edge adds.
 *
 * <p>
 * This record <em>is</em> the wire contract. Renaming a component changes the
 * JSON and breaks consumers that are
 * still running the old version, so changes must be additive (new optional
 * fields) or bump {@link #schemaVersion}.
 *
 * @param schemaVersion envelope format version; consumers reject versions they
 *                      do not know
 * @param tenantId      owner of the event, taken from the caller's identity
 *                      (never from the request body)
 * @param eventId       client-supplied idempotency key; identical across client
 *                      retries
 * @param type          event type, e.g. {@code api.call}
 * @param occurredAt    when the event happened, according to the client; used
 *                      for minute bucketing
 * @param receivedAt    when the ingestion API accepted the batch; used for
 *                      end-to-end latency
 * @param sourceId      optional client-side origin (service, device, ...);
 *                      {@code null} when absent
 * @param value         optional numeric payload summed by the aggregation;
 *                      {@code null} when absent
 * @param attributes    optional string pairs, never {@code null}
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        int schemaVersion,
        String tenantId,
        UUID eventId,
        String type,
        Instant occurredAt,
        Instant receivedAt,
        String sourceId,
        Double value,
        Map<String, String> attributes) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public EventEnvelope {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(receivedAt, "receivedAt");
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }
}
