package io.highthroughput.contract;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Converts {@link EventEnvelope} to and from the JSON bytes stored in Kafka.
 *
 * <p>
 * Why an explicit codec instead of Spring Kafka's JSON serializer: the wire
 * format must not depend on Spring's
 * type headers ({@code __TypeId__}) or on whichever {@code JsonMapper} the
 * application happens to configure. This class
 * owns its own mapper with every format-relevant setting written down, so
 * producer and consumer agree by construction
 * and a non-Spring consumer can read the topic.
 *
 * <p>
 * Thread-safe; create once and share.
 */
public final class EventEnvelopeCodec {

    private final JsonMapper mapper = JsonMapper.builder()
            // Instants as ISO-8601 strings ("2026-10-27T10:15:30.123Z"), not epoch numbers.
            // Jackson 3 already
            // defaults to this; stating it pins the format against future default changes.
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            // Tolerant reader: a newer producer may add optional fields; older consumers
            // must keep working.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            // Deterministic bytes for the same envelope (attributes are stored in an
            // unordered Map).
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();

    public byte[] encode(EventEnvelope envelope) {
        return mapper.writeValueAsBytes(envelope);
    }

    /**
     * @throws EnvelopeDecodingException if the bytes are not a valid envelope of a
     *                                   supported schema version
     */
    public EventEnvelope decode(byte[] bytes) {
        if (bytes == null) {
            throw new EnvelopeDecodingException("Record value is null");
        }
        EventEnvelope envelope;
        try {
            envelope = mapper.readValue(bytes, EventEnvelope.class);
        } catch (JacksonException e) {
            throw new EnvelopeDecodingException("Record value is not a valid event envelope: " + e.getOriginalMessage(),
                    e);
        }
        if (envelope.schemaVersion() != EventEnvelope.CURRENT_SCHEMA_VERSION) {
            throw new EnvelopeDecodingException("Unsupported envelope schemaVersion " + envelope.schemaVersion());
        }
        return envelope;
    }
}
