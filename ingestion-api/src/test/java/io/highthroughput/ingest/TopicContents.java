package io.highthroughput.ingest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads a topic from the beginning up to its current end, without a consumer
 * group.
 *
 * <p>
 * Because the API answers 202 only after Kafka acknowledged every record, the
 * end offsets taken after the HTTP
 * call already include those records. Reading up to them is deterministic: no
 * "poll until it probably arrived".
 */
final class TopicContents {

    private TopicContents() {
    }

    static List<ConsumerRecord<String, byte[]>> readAll(String bootstrapServers, String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (var consumer = new KafkaConsumer<String, byte[]>(config)) {
            // assign() instead of subscribe(): read every partition directly, no group
            // coordination needed.
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < endOffsets.get(tp))) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Timed out reading " + topic + " up to " + endOffsets);
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }
}
