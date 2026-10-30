package io.highthroughput.ingest.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Settings under {@code ingestion.*}. All are required and set explicitly in
 * {@code application.yaml}, where each
 * value is explained; the application fails at startup if one is missing.
 *
 * @param topic              Kafka topic for accepted events
 * @param publishTimeout     deadline for Kafka to acknowledge a whole batch
 *                           before the API answers 503
 * @param retryAfter         value of the {@code Retry-After} header on 503
 *                           responses
 * @param maxRequestBodySize larger request bodies are rejected with 413 before
 *                           they are parsed
 * @param maxEventAge        oldest accepted {@code occurredAt}, relative to the
 *                           time the batch is received
 * @param maxClockSkew       how far in the future {@code occurredAt} may be
 *                           (client clocks drift)
 * @param kafkaWarmUpTimeout how long startup waits for the topic's metadata
 *                           before starting without it
 * @param maxInFlight        ingestion requests processed at once per instance;
 *                           more are rejected with 503 (admission control)
 */
@Validated
@ConfigurationProperties("ingestion")
public record IngestionProperties(
                @NotBlank String topic,
                @NotNull Duration publishTimeout,
                @NotNull Duration retryAfter,
                @NotNull DataSize maxRequestBodySize,
                @NotNull Duration maxEventAge,
                @NotNull Duration maxClockSkew,
                @NotNull Duration kafkaWarmUpTimeout,
                @Min(1) int maxInFlight) {
}
