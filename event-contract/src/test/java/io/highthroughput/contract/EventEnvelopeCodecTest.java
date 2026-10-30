package io.highthroughput.contract;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventEnvelopeCodecTest {

    private final EventEnvelopeCodec codec = new EventEnvelopeCodec();

    private final EventEnvelope envelope = new EventEnvelope(
            EventEnvelope.CURRENT_SCHEMA_VERSION,
            "acme",
            UUID.fromString("01929f4e-7c1a-7b3e-9f00-5d2b6c1e8a10"),
            "api.call",
            Instant.parse("2026-10-27T10:15:30.123Z"),
            Instant.parse("2026-10-27T10:15:31.456789Z"),
            "checkout-service",
            42.5,
            Map.of("region", "eu", "endpoint", "/orders"));

    @Test
    void roundTripPreservesEveryField() {
        assertThat(codec.decode(codec.encode(envelope))).isEqualTo(envelope);
    }

    /**
     * Pins the exact wire format. If this test fails, the change affects every
     * consumer of the topic:
     * make it additive or bump the schema version.
     */
    @Test
    void wireFormatIsPinned() {
        String json = new String(codec.encode(envelope), StandardCharsets.UTF_8);

        assertThat(json).isEqualTo("""
                {"schemaVersion":1,"tenantId":"acme","eventId":"01929f4e-7c1a-7b3e-9f00-5d2b6c1e8a10",\
                "type":"api.call","occurredAt":"2026-10-27T10:15:30.123Z","receivedAt":"2026-10-27T10:15:31.456789Z",\
                "sourceId":"checkout-service","value":42.5,"attributes":{"endpoint":"/orders","region":"eu"}}""");
    }

    @Test
    void absentOptionalFieldsAreOmittedAndAttributesDefaultToEmpty() {
        var minimal = new EventEnvelope(1, "acme", envelope.eventId(), "api.call",
                envelope.occurredAt(), envelope.receivedAt(), null, null, null);

        String json = new String(codec.encode(minimal), StandardCharsets.UTF_8);

        assertThat(json).doesNotContain("sourceId", "value").contains("\"attributes\":{}");
        assertThat(codec.decode(json.getBytes(StandardCharsets.UTF_8))).isEqualTo(minimal);
    }

    @Test
    void unknownFieldsFromNewerProducersAreIgnored() {
        byte[] newer = """
                {"schemaVersion":1,"tenantId":"acme","eventId":"01929f4e-7c1a-7b3e-9f00-5d2b6c1e8a10",
                 "type":"api.call","occurredAt":"2026-10-27T10:15:30.123Z","receivedAt":"2026-10-27T10:15:31Z",
                 "someFutureField":{"nested":true}}""".getBytes(StandardCharsets.UTF_8);

        assertThat(codec.decode(newer).tenantId()).isEqualTo("acme");
    }

    @Test
    void invalidJsonIsADecodingError() {
        assertThatThrownBy(() -> codec.decode("not json".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(EnvelopeDecodingException.class);
    }

    @Test
    void missingRequiredFieldIsADecodingError() {
        byte[] noTenant = """
                {"schemaVersion":1,"eventId":"01929f4e-7c1a-7b3e-9f00-5d2b6c1e8a10","type":"api.call",
                 "occurredAt":"2026-10-27T10:15:30.123Z","receivedAt":"2026-10-27T10:15:31Z"}"""
                .getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> codec.decode(noTenant)).isInstanceOf(EnvelopeDecodingException.class);
    }

    @Test
    void unknownSchemaVersionIsADecodingError() {
        byte[] v2 = new String(codec.encode(envelope), StandardCharsets.UTF_8)
                .replace("\"schemaVersion\":1", "\"schemaVersion\":2")
                .getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> codec.decode(v2))
                .isInstanceOf(EnvelopeDecodingException.class)
                .hasMessageContaining("schemaVersion 2");
    }

    @Test
    void nullRecordValueIsADecodingError() {
        assertThatThrownBy(() -> codec.decode(null)).isInstanceOf(EnvelopeDecodingException.class);
    }
}
