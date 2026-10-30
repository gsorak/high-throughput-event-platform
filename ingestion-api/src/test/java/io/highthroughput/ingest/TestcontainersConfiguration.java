package io.highthroughput.ingest;

import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Starts a real single-node Kafka (KRaft mode) and a Valkey in Docker for
 * integration tests, and points
 * {@code security.jwt.public-key-location} at {@link TestJwt}'s key.
 *
 * <p>
 * {@code @ServiceConnection} makes Spring Boot read the Kafka container's
 * mapped address and use it as
 * {@code spring.kafka.bootstrap-servers}; Valkey's address is registered
 * explicitly. The containers are beans, so
 * they start with the application context and stop when the context is closed.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    /**
     * Same images as infra/local/compose.yaml, so tests and local runs use one
     * version of each.
     */
    public static final String KAFKA_IMAGE = "apache/kafka:4.3.2";
    public static final String VALKEY_IMAGE = "valkey/valkey:9.0.1";
    public static final int VALKEY_PORT = 6379;

    public static final int PARTITIONS = 3;

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(KAFKA_IMAGE);
    }

    @Bean
    GenericContainer<?> valkeyContainer() {
        return new GenericContainer<>(VALKEY_IMAGE).withExposedPorts(VALKEY_PORT);
    }

    @Bean
    DynamicPropertyRegistrar testProperties(GenericContainer<?> valkeyContainer) {
        return registry -> {
            registry.add("rate-limit.valkey-uri", () -> valkeyUri(valkeyContainer));
            registry.add("security.jwt.public-key-location", TestJwt::publicKeyLocation);
        };
    }

    public static String valkeyUri(GenericContainer<?> valkey) {
        return "redis://" + valkey.getHost() + ":" + valkey.getMappedPort(VALKEY_PORT);
    }

    /**
     * Spring's KafkaAdmin creates every NewTopic bean at startup. Several
     * partitions, so keying is observable.
     */
    @Bean
    NewTopic eventsTopic() {
        return TopicBuilder.name(Topics.EVENTS).partitions(PARTITIONS).replicas(1).build();
    }
}
