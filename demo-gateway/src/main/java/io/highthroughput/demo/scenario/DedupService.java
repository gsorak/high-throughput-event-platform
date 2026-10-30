package io.highthroughput.demo.scenario;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.platform.DemoEvent;
import io.highthroughput.demo.platform.IngestResult;
import io.highthroughput.demo.platform.PlatformClient;
import io.highthroughput.demo.platform.StatsResult;
import io.highthroughput.demo.session.DemoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;

/**
 * Deduplication: deliver a batch of 100 events, then deliver exactly the same events again and show from the
 * aggregates that nothing was counted twice.
 *
 * <p>A 202 on the replay proves nothing, so the verdict waits for evidence the replay was processed: a marker event
 * sent right after it. Both go to the same tenant and therefore the same Kafka partition, so once the marker is
 * aggregated the replayed copies have been consumed before it.
 */
public class DedupService {

    static final String TYPE = "dedup.check";
    static final String MARKER_TYPE = "dedup.marker";
    static final String SCENARIO = "dedup";
    private static final int BATCH_SIZE = 100;
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    /** Prometheus scrapes every 5 s; allow a few scrapes for the duplicate counter to move. */
    private static final Duration DUPLICATE_METRIC_WAIT = Duration.ofSeconds(20);

    private static final Logger log = LoggerFactory.getLogger(DedupService.class);

    private final DemoProperties.Limits limits;
    private final PlatformClient platform;
    private final SystemMetrics metrics;
    private final Executor executor;
    private final Clock clock;

    public DedupService(DemoProperties.Limits limits, PlatformClient platform, SystemMetrics metrics, Executor executor,
            Clock clock) {
        this.limits = limits;
        this.platform = platform;
        this.metrics = metrics;
        this.executor = executor;
        this.clock = clock;
    }

    /** Delivers a new batch with fresh event ids; the scenario continues in the background. */
    public DedupState send(DemoSession session) {
        begin(session, BATCH_SIZE);
        Instant occurredAt = clock.instant();
        List<DemoEvent> batch = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            batch.add(DemoEvent.of(UUID.randomUUID(), TYPE, occurredAt, 1));
        }
        DedupState state = new DedupState(DedupState.Phase.DELIVERING, BATCH_SIZE,
                batch.stream().limit(3).map(DemoEvent::eventId).toList(), null, null, null, null, null,
                "Delivering " + BATCH_SIZE + " new events.", occurredAt);
        session.setDedup(state);
        run(session, () -> deliver(session, batch, state));
        return state;
    }

    /** Delivers the previous batch again, with the same event ids; the scenario continues in the background. */
    public DedupState replay(DemoSession session) {
        DedupState current = session.dedup();
        List<DemoEvent> batch = session.dedupBatch();
        if (batch == null || current == null || current.countAfterFirst() == null) {
            throw DemoException.busy("Deliver a batch first; only a processed batch can be replayed.");
        }
        begin(session, BATCH_SIZE + 1);
        DedupState state = new DedupState(DedupState.Phase.REPLAYING, current.batchSize(), current.sampleEventIds(),
                current.countBefore(), current.countAfterFirst(), null, null, null,
                "Sending the same " + BATCH_SIZE + " events again.", clock.instant());
        session.setDedup(state);
        run(session, () -> replay(session, batch, state));
        return state;
    }

    private void begin(DemoSession session, int events) {
        if (!session.tryBegin(SCENARIO)) {
            throw DemoException.busy("The deduplication check is already running.");
        }
        if (!session.tryReserve(events)) {
            session.end(SCENARIO);
            throw new DemoException(HttpStatus.UNPROCESSABLE_CONTENT, "Session budget exhausted",
                    "The session's event budget of " + session.eventBudget() + " is used up.");
        }
    }

    private void run(DemoSession session, Runnable task) {
        try {
            executor.execute(() -> {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    log.warn("Deduplication scenario failed", e);
                    session.setDedup(session.dedup().with(DedupState.Phase.FAILED, "The check failed unexpectedly.",
                            clock.instant()));
                } finally {
                    session.end(SCENARIO);
                }
            });
        } catch (RuntimeException e) {
            session.end(SCENARIO);
            throw e;
        }
    }

    private void deliver(DemoSession session, List<DemoEvent> batch, DedupState state) {
        StatsResult before = stats(session, TYPE);
        if (!before.isOk()) {
            fail(session, state, "The statistics API is unavailable (status " + before.status() + ").");
            return;
        }
        IngestResult result = platform.ingest(session.tenantA(), batch);
        if (!result.isAccepted()) {
            fail(session, state, "The ingestion API answered " + result.status() + " instead of 202.");
            return;
        }
        long expected = before.totalCount() + BATCH_SIZE;
        Long after = awaitCount(session, TYPE, count -> count >= expected);
        if (after == null) {
            fail(session, state, "The batch was accepted (202) but not aggregated within "
                    + limits.drainTimeout().toSeconds() + " seconds.");
            return;
        }
        session.setDedupBatch(batch);
        session.setDedup(new DedupState(DedupState.Phase.DELIVERED, BATCH_SIZE, state.sampleEventIds(),
                before.totalCount(), after, null, null, null,
                "Kafka accepted the batch and all " + BATCH_SIZE + " events are aggregated.", clock.instant()));
    }

    private void replay(DemoSession session, List<DemoEvent> batch, DedupState state) {
        Long duplicatesBefore = metrics.duplicatesTotal();
        IngestResult result = platform.ingest(session.tenantA(), batch);
        DedupState sent = new DedupState(DedupState.Phase.REPLAYING, state.batchSize(), state.sampleEventIds(),
                state.countBefore(), state.countAfterFirst(), result.status(), null, null,
                result.isAccepted() ? "The replay was accepted (202); waiting until it is processed."
                        : "The replay was rejected.", clock.instant());
        session.setDedup(sent);
        if (!result.isAccepted()) {
            fail(session, sent, "The ingestion API answered " + result.status() + " to the replay instead of 202.");
            return;
        }

        StatsResult markerBefore = stats(session, MARKER_TYPE);
        if (!markerBefore.isOk()
                || !platform.ingest(session.tenantA(), List.of(DemoEvent.of(UUID.randomUUID(), MARKER_TYPE,
                        clock.instant(), 0))).isAccepted()) {
            fail(session, sent, "The marker event that confirms the replay was processed could not be sent.");
            return;
        }
        if (awaitCount(session, MARKER_TYPE, count -> count > markerBefore.totalCount()) == null) {
            fail(session, sent, "The replay was not processed within " + limits.drainTimeout().toSeconds()
                    + " seconds.");
            return;
        }
        StatsResult after = stats(session, TYPE);
        if (!after.isOk()) {
            fail(session, sent, "The statistics API is unavailable (status " + after.status() + ").");
            return;
        }
        boolean unchanged = after.totalCount() == state.countAfterFirst();
        DedupState verdict = new DedupState(unchanged ? DedupState.Phase.VERIFIED : DedupState.Phase.FAILED,
                state.batchSize(), state.sampleEventIds(), state.countBefore(), state.countAfterFirst(),
                result.status(), after.totalCount(), null,
                unchanged ? "The replay was processed and the aggregate is unchanged: no event was counted twice."
                        : "The aggregate changed from " + state.countAfterFirst() + " to " + after.totalCount()
                                + " after the replay.",
                clock.instant());
        session.setDedup(verdict);
        Long skipped = awaitDuplicateIncrease(duplicatesBefore);
        if (skipped != null) {
            session.setDedup(new DedupState(verdict.phase(), verdict.batchSize(), verdict.sampleEventIds(),
                    verdict.countBefore(), verdict.countAfterFirst(), verdict.replayStatus(), verdict.countAfterReplay(),
                    skipped, verdict.message(), verdict.updatedAt()));
        }
    }

    private void fail(DemoSession session, DedupState state, String message) {
        session.setDedup(state.with(DedupState.Phase.FAILED, message, clock.instant()));
    }

    private StatsResult stats(DemoSession session, String type) {
        return platform.stats(session.tenantA(), session.statsFrom(), session.statsTo(), type);
    }

    private Long awaitCount(DemoSession session, String type, LongPredicate done) {
        Instant deadline = clock.instant().plus(limits.drainTimeout());
        while (!session.isClosed()) {
            StatsResult stats = stats(session, type);
            if (stats.isOk() && done.test(stats.totalCount())) {
                return stats.totalCount();
            }
            if (clock.instant().isAfter(deadline) || !sleep(POLL_INTERVAL)) {
                return null;
            }
        }
        return null;
    }

    /** Platform-wide, so other traffic can add to it; null if Prometheus did not report an increase in time. */
    private Long awaitDuplicateIncrease(Long before) {
        if (before == null) {
            return null;
        }
        Instant deadline = clock.instant().plus(DUPLICATE_METRIC_WAIT);
        while (true) {
            Long now = metrics.duplicatesTotal();
            if (now != null && now - before >= BATCH_SIZE) {
                return now - before;
            }
            if (clock.instant().isAfter(deadline) || !sleep(Duration.ofSeconds(1))) {
                return now == null || now <= before ? null : now - before;
            }
        }
    }

    private static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
