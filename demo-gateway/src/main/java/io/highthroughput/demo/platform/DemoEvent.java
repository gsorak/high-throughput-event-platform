package io.highthroughput.demo.platform;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

/** One event in the request body of {@code POST /v1/events}. */
public record DemoEvent(String eventId, String type, String occurredAt, String sourceId, double value) {

    public static DemoEvent of(UUID eventId, String type, Instant occurredAt, double value) {
        return new DemoEvent(eventId.toString(), type, occurredAt.truncatedTo(ChronoUnit.MILLIS).toString(), "demo-gateway",
                value);
    }
}
