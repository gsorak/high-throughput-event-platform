package com.gsorak.eventplatform.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gsorak.eventplatform.api.EventRequest;
import com.gsorak.eventplatform.persistence.EventPersistenceService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class EventConsumer {

    private final ObjectMapper objectMapper;
    private final EventPersistenceService persistenceService;

    public EventConsumer(ObjectMapper objectMapper, EventPersistenceService persistenceService) {
        this.objectMapper = objectMapper;
        this.persistenceService = persistenceService;
    }

    @KafkaListener(topics = "${app.kafka.topic:events.v1}")
    public void consume(String message) throws IOException {
        EventRequest event = objectMapper.readValue(message, EventRequest.class);
        persistenceService.persist(event);
    }
}
