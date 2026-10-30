package io.highthroughput.processor.aggregation;

import io.highthroughput.processor.config.ProcessorProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Deletes deduplication ledger rows older than
 * {@code processor.dedup.retention} (21 days, ADR-0005), in batches so
 * that no single transaction holds many row locks or produces a large burst of
 * WAL.
 *
 * <p>
 * Runs hourly in every instance; concurrent runs delete different rows or find
 * nothing. A missed run only delays
 * deletion: rows stay longer, so a late duplicate is still rejected.
 */
@Component
public class LedgerRetentionJob {

    static final int BATCH_SIZE = 10_000;

    private static final Logger log = LoggerFactory.getLogger(LedgerRetentionJob.class);

    private final EventStatsRepository repository;
    private final Duration retention;
    private final Clock clock;

    public LedgerRetentionJob(EventStatsRepository repository, ProcessorProperties properties, Clock clock) {
        this.repository = repository;
        this.retention = properties.dedup().retention();
        this.clock = clock;
    }

    /** @return number of rows deleted */
    @Scheduled(initialDelay = 1, fixedDelay = 60, timeUnit = TimeUnit.MINUTES)
    public int deleteExpired() {
        Instant cutoff = clock.instant().minus(retention);
        int total = 0;
        int deleted;
        do {
            deleted = repository.deleteProcessedBefore(cutoff, BATCH_SIZE);
            total += deleted;
        } while (deleted == BATCH_SIZE);
        if (total > 0) {
            log.info("Deleted {} deduplication ledger rows processed before {}", total, cutoff);
        }
        return total;
    }
}
