package io.highthroughput.ingest;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Builds request bodies for integration tests. */
final class TestBatches {

    private TestBatches() {
    }

    static String withEventIds(List<UUID> eventIds) {
        Instant occurredAt = Instant.now().minusSeconds(5);
        return eventIds.stream()
                .map(id -> """
                        {"eventId": "%s", "type": "api.call", "occurredAt": "%s", "value": 1.0}"""
                        .formatted(id, occurredAt))
                .collect(Collectors.joining(",", "{\"events\": [", "]}"));
    }

    static List<UUID> randomIds(int count) {
        return Stream.generate(UUID::randomUUID).limit(count).toList();
    }

    /** Unique per test, so tests sharing one topic can tell their records apart. */
    static String uniqueTenant() {
        return "t-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
