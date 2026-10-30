package io.highthroughput.contract;

/**
 * Kafka topic names. Topics are created by infrastructure (see
 * {@code infra/local/compose.yaml}), never by the
 * applications, so a typo here fails loudly instead of silently creating a new
 * topic.
 */
public final class Topics {

    /**
     * All ingested events. Key = tenantId, value = JSON {@link EventEnvelope}. 12
     * partitions.
     */
    public static final String EVENTS = "events.v1";

    /**
     * Records of {@link #EVENTS} that event-processor could not aggregate,
     * unchanged, with exception headers. Same
     * partition count as {@link #EVENTS}: a record keeps its partition number.
     * 13-day retention plus daily segments: records are gone at most 14 days
     * after they were written (ADR-0005).
     */
    public static final String EVENTS_DEAD_LETTER = "events.v1.DLT";

    private Topics() {
    }
}
