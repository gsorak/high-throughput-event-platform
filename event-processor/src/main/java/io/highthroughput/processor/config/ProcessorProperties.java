package io.highthroughput.processor.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Settings under {@code processor.*}, all set explicitly and explained in
 * {@code application.yaml}.
 *
 * @param topic Kafka topic to consume
 * @param stats limits for {@code GET /v1/stats}
 * @param retry back-off between attempts when processing a record fails with a
 *              retryable error
 * @param dedup deduplication ledger settings
 */
@Validated
@ConfigurationProperties("processor")
public record ProcessorProperties(@NotBlank String topic, @Valid @NotNull Stats stats, @Valid @NotNull Retry retry,
        @Valid @NotNull Dedup dedup) {

    /**
     * @param maxRange   longest allowed {@code to - from}
     * @param maxBuckets most buckets returned; more sets {@code truncated: true}
     */
    public record Stats(@NotNull Duration maxRange, @Min(1) int maxBuckets) {
    }

    /**
     * Exponential back-off: {@code initialInterval}, then × {@code multiplier},
     * capped at {@code maxInterval}. Transient failures retry without limit,
     * unclassified ones a bounded number of
     * times ({@code KafkaErrorHandlingConfiguration}).
     */
    public record Retry(@NotNull Duration initialInterval, @DecimalMin("1.0") double multiplier,
            @NotNull Duration maxInterval) {
    }

    /**
     * @param retention how long a counted event's ledger row is kept; a copy
     *                  arriving later is counted again
     */
    public record Dedup(@NotNull Duration retention) {
    }
}
