package com.gsorak.eventplatform.kafka;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaConfiguration {

    @Bean
    NewTopic eventTopic(
            @Value("${app.kafka.topic:events.v1}") String topic,
            @Value("${app.kafka.partitions:6}") int partitions,
            @Value("${app.kafka.replication-factor:1}") int replicas
    ) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    NewTopic deadLetterTopic(
            @Value("${app.kafka.topic:events.v1}") String topic,
            @Value("${app.kafka.partitions:6}") int partitions,
            @Value("${app.kafka.replication-factor:1}") int replicas
    ) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(replicas).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic:events.v1}") String topic
    ) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(topic + ".DLT", record.partition())
        );
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
    }
}
