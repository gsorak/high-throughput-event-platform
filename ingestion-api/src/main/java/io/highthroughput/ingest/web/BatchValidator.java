package io.highthroughput.ingest.web;

import io.highthroughput.ingest.config.IngestionProperties;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * All-or-nothing validation of a batch: either every event is valid, or the
 * request fails with every problem listed.
 *
 * <p>
 * Combines the declarative Bean Validation constraints on {@link EventPayload}
 * with the rules that need context:
 * the {@code occurredAt} window (needs "now") and duplicate {@code eventId}s
 * (needs the whole batch).
 */
@Component
public class BatchValidator {

    /**
     * Enough to fix a batch; prevents a 500-event garbage batch from producing a
     * multi-megabyte error response.
     */
    static final int MAX_REPORTED_VIOLATIONS = 50;

    /**
     * Keeps per-minute sums ({@code double precision}) far from overflow: summing
     * two values near
     * {@code 1e308} fails in PostgreSQL, and the processor would skip that event.
     */
    static final double MAX_ABS_VALUE = 1e12;

    private final Validator validator;
    private final IngestionProperties properties;

    public BatchValidator(Validator validator, IngestionProperties properties) {
        this.validator = validator;
        this.properties = properties;
    }

    /**
     * @param receivedAt the time the batch arrived; the {@code occurredAt} window
     *                   is relative to it
     * @throws InvalidBatchException if any rule is violated
     */
    public void validate(IngestEventsRequest request, Instant receivedAt) {
        List<FieldViolation> violations = new ArrayList<>();
        validator.validate(request).forEach(
                v -> violations.add(new FieldViolation(JsonPointers.from(v.getPropertyPath()), v.getMessage())));

        if (request.events() != null) {
            Instant oldestAllowed = receivedAt.minus(properties.maxEventAge());
            Instant newestAllowed = receivedAt.plus(properties.maxClockSkew());
            Set<UUID> seenIds = new HashSet<>();
            for (int i = 0; i < request.events().size(); i++) {
                EventPayload event = request.events().get(i);
                if (event == null) {
                    continue;
                }
                if (event.occurredAt() != null
                        && (event.occurredAt().isBefore(oldestAllowed) || event.occurredAt().isAfter(newestAllowed))) {
                    violations.add(new FieldViolation("/events/" + i + "/occurredAt",
                            "must be between %s and %s".formatted(oldestAllowed, newestAllowed)));
                }
                if (event.eventId() != null && !seenIds.add(event.eventId())) {
                    violations.add(new FieldViolation("/events/" + i + "/eventId",
                            "duplicates an eventId earlier in this batch"));
                }
                if (event.value() != null && !(Math.abs(event.value()) <= MAX_ABS_VALUE)) {
                    violations.add(new FieldViolation("/events/" + i + "/value",
                            "must be a finite number between -1e12 and 1e12"));
                }
            }
        }

        if (!violations.isEmpty()) {
            violations.sort(Comparator.comparing(FieldViolation::pointer));
            String message = violations.size() <= MAX_REPORTED_VIOLATIONS
                    ? "%d validation error(s); no events were accepted".formatted(violations.size())
                    : "%d validation errors (first %d listed); no events were accepted"
                            .formatted(violations.size(), MAX_REPORTED_VIOLATIONS);
            throw new InvalidBatchException(message,
                    violations.subList(0, Math.min(violations.size(), MAX_REPORTED_VIOLATIONS)));
        }
    }
}
