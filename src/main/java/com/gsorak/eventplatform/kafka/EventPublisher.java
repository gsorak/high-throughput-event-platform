package com.gsorak.eventplatform.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gsorak.eventplatform.api.EventRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public EventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topic:events.v1}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public CompletableFuture<Void> publish(EventRequest event) {
        try {
            String value = objectMapper.writeValueAsString(event);
            return kafkaTemplate.send(topic, event.eventId().toString(), value).thenApply(result -> null);
        } catch (JsonProcessingException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}
