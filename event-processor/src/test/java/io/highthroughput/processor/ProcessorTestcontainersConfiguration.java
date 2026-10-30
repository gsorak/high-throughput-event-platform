package io.highthroughput.processor;

import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real Kafka and PostgreSQL in Docker, same images as infra/local/compose.yaml.
 * {@code @ServiceConnection} points
 * {@code spring.kafka.*} and {@code spring.datasource.*} at the containers;
 * Flyway then migrates the empty database
 * at startup exactly as in production. Stats requests are authenticated with
 * {@link TestJwt}'s key.
 */
@TestConfiguration(proxyBeanMethods = false)
public class ProcessorTestcontainersConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // As in compose: a missing topic is an error, not silently created
        // (DeadLetterIT relies on this).
        return new KafkaContainer("apache/kafka:4.3.2").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer("postgres:17.11");
    }

    @Bean
    NewTopic eventsTopic() {
        return TopicBuilder.name(Topics.EVENTS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(Topics.EVENTS_DEAD_LETTER).partitions(3).replicas(1).build();
    }

    @Bean
    DynamicPropertyRegistrar jwtPublicKey() {
        return registry -> registry.add("security.jwt.public-key-location", TestJwt::publicKeyLocation);
    }
}
