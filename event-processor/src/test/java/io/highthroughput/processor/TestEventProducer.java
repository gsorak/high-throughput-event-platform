package io.highthroughput.processor;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Writes records to {@code events.v1} the way ingestion-api does (key =
 * tenantId, value = encoded envelope), or raw
 * bytes for malformed-record tests. Each send waits for the acknowledgement, so
 * records are in Kafka when it returns.
 */
final class TestEventProducer implements AutoCloseable {

    private final KafkaProducer<String, byte[]> producer;
    private final EventEnvelopeCodec codec = new EventEnvelopeCodec();

    TestEventProducer(String bootstrapServers) {
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    static EventEnvelope event(String tenantId, String type, Instant occurredAt, Double value) {
        return new EventEnvelope(EventEnvelope.CURRENT_SCHEMA_VERSION, tenantId, UUID.randomUUID(), type, occurredAt,
                Instant.now(), null, value, Map.of());
    }

    RecordMetadata send(EventEnvelope event) throws Exception {
        return sendRaw(event.tenantId(), codec.encode(event));
    }

    RecordMetadata sendRaw(String key, byte[] value) throws Exception {
        return producer.send(new ProducerRecord<>(Topics.EVENTS, key, value)).get(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        producer.close();
    }
}
