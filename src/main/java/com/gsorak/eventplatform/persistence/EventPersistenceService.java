package com.gsorak.eventplatform.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gsorak.eventplatform.api.EventRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

@Service
public class EventPersistenceService {

    private static final String INSERT_EVENT = """
            INSERT INTO events (event_id, event_type, occurred_at, payload)
            VALUES (?, ?, ?, CAST(? AS jsonb))
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public EventPersistenceService(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void persist(EventRequest event) {
        try {
            jdbcTemplate.update(
                    INSERT_EVENT,
                    event.eventId(),
                    event.type(),
                    Timestamp.from(event.occurredAt()),
                    objectMapper.writeValueAsString(event.payload())
            );
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Event payload could not be serialized", exception);
        }
    }
}
