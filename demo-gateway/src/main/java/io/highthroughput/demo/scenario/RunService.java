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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Load runs: a server-side generator sends one session's events to the ingestion API at a bounded rate, then follows
 * the statistics API until every accepted event is aggregated.
 *
 * <p>Only one run executes system-wide. The slot is taken with a compare-and-set and held until the run's backlog has
 * drained (or the drain timeout passed), so runs of different visitors never pile up on the platform.
 *
 * <p>The generator paces itself like a well-behaved client. Progress is sampled on a separate thread, and after a
 * stall (a slow response) the generator falls back by at most {@link #MAX_CATCH_UP} of events instead of sending the
 * whole shortfall at once: catching up 2 s or more at 1000 events/s exceeded tenant A's burst of 2000 and produced
 * 429s that were an artefact of the generator, not of the configured rate.
 */
public class RunService {

    public static final List<String> EVENT_TYPES = List.of("page.view", "api.call", "order.placed", "payment.failed");

    private static final Logger log = LoggerFactory.getLogger(RunService.class);
    private static final int MAX_BATCH = 500;
    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);
    /** Sending stops this long after the planned duration even if the API is too slow to take every event. */
    private static final Duration SEND_GRACE = Duration.ofSeconds(10);
    /** The most the generator sends ahead of its pace after falling behind, as time at the configured rate. */
    static final Duration MAX_CATCH_UP = Duration.ofMillis(500);

    private final DemoProperties.Limits limits;
    private final PlatformClient platform;
    private final Executor executor;
    private final Clock clock;
    private final AtomicReference<Run> active = new AtomicReference<>();

    public RunService(DemoProperties.Limits limits, PlatformClient platform, Executor executor, Clock clock) {
        this.limits = limits;
        this.platform = platform;
        this.executor = executor;
        this.clock = clock;
    }

    public record RunRequest(String type, int eventsPerSecond, int durationSeconds) {
    }

    public Run start(DemoSession session, RunRequest request) {
        validate(request);
        Run previous = session.run();
        if (previous != null && !previous.isFinished()) {
            throw DemoException.busy("Your previous run is still in progress.");
        }
        long planned = (long) request.eventsPerSecond() * request.durationSeconds();
        if (!session.tryReserve(planned)) {
            throw new DemoException(HttpStatus.UNPROCESSABLE_CONTENT, "Session budget exhausted",
                    "This run needs " + planned + " events but only " + (session.eventBudget() - session.eventsUsed())
                            + " of the session's " + session.eventBudget() + " remain.");
        }
        Run run = new Run(session.tenantA(), request.type(), request.eventsPerSecond(), request.durationSeconds(),
                clock.instant());
        if (!active.compareAndSet(null, run)) {
            session.refund(planned);
            Run other = active.get();
            long running = other == null ? 0 : Duration.between(other.getStartedAt(), clock.instant()).toSeconds();
            throw new DemoException(HttpStatus.CONFLICT, "Another run is in progress",
                    "Only one load run executes at a time across all visitors, so results stay meaningful. "
                            + "The current one started " + running + " seconds ago; try again shortly.", 10L);
        }
        session.setRun(run);
        try {
            executor.execute(() -> execute(session, run));
        } catch (RuntimeException e) {
            active.compareAndSet(run, null);
            session.refund(planned);
            run.finish(Run.Phase.FAILED, clock.instant(), "The run could not be started.");
            throw e;
        }
        return run;
    }

    public void stop(DemoSession session) {
        Run run = session.run();
        if (run == null || run.getPhase() != Run.Phase.SENDING) {
            throw DemoException.busy("No run of yours is sending.");
        }
        run.requestStop();
    }

    /** The run holding the system-wide slot, if any. */
    public Optional<Run> active() {
        return Optional.ofNullable(active.get());
    }

    private void validate(RunRequest request) {
        if (request.type() == null || !EVENT_TYPES.contains(request.type())) {
            throw invalid("'type' must be one of " + EVENT_TYPES + ".");
        }
        if (request.eventsPerSecond() < 1 || request.eventsPerSecond() > limits.maxEventsPerSecond()) {
            throw invalid("'eventsPerSecond' must be between 1 and " + limits.maxEventsPerSecond() + ".");
        }
        if (request.durationSeconds() < 1 || request.durationSeconds() > limits.maxRunDuration().toSeconds()) {
            throw invalid("'durationSeconds' must be between 1 and " + limits.maxRunDuration().toSeconds() + ".");
        }
        if ((long) request.eventsPerSecond() * request.durationSeconds() > limits.maxEventsPerRun()) {
            throw invalid("A run may send at most " + limits.maxEventsPerRun() + " events (rate x duration).");
        }
    }

    private static DemoException invalid(String detail) {
        return new DemoException(HttpStatus.BAD_REQUEST, "Invalid run", detail);
    }

    private void execute(DemoSession session, Run run) {
        try {
            StatsResult base = platform.stats(run.tenantId(), session.statsFrom(), session.statsTo(), run.getType());
            if (!base.isOk()) {
                session.refund(run.getPlannedEvents());
                run.finish(Run.Phase.FAILED, clock.instant(),
                        "The statistics API is unavailable (status " + base.status() + "); nothing was sent.");
                return;
            }
            long baseline = base.totalCount();

            CountDownLatch samplerDone = new CountDownLatch(1);
            executor.execute(() -> {
                try {
                    sampleWhileSending(session, run, baseline);
                } finally {
                    samplerDone.countDown();
                }
            });
            try {
                send(run);
            } finally {
                session.refund(run.getPlannedEvents() - run.getSubmitted());
                run.sendingEnded(clock.instant());
            }
            samplerDone.await();

            Instant deadline = clock.instant().plus(limits.drainTimeout());
            while (true) {
                sample(session, run, baseline);
                if (run.getProcessed() >= run.getAccepted()) {
                    run.finish(Run.Phase.COMPLETED, clock.instant(), completedMessage(run));
                    return;
                }
                if (clock.instant().isAfter(deadline)) {
                    run.finish(Run.Phase.INCOMPLETE, clock.instant(), run.getPending() + " accepted events were not yet "
                            + "aggregated after " + limits.drainTimeout().toSeconds() + " seconds; they stay in Kafka "
                            + "and are processed later.");
                    return;
                }
                Thread.sleep(SAMPLE_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            run.finish(Run.Phase.FAILED, clock.instant(), "The gateway is shutting down.");
        } catch (RuntimeException e) {
            log.warn("Run failed", e);
            run.finish(Run.Phase.FAILED, clock.instant(), "The run failed unexpectedly.");
        } finally {
            active.compareAndSet(run, null);
        }
    }

    /** Sends the run's events at its rate, until all are sent, a stop is requested or the grace time is over. */
    private void send(Run run) throws InterruptedException {
        long rate = run.getEventsPerSecond();
        long maxCatchUp = Math.max(1, rate * MAX_CATCH_UP.toMillis() / 1000);
        long start = System.nanoTime();
        long sendLimit = Duration.ofSeconds(run.getDurationSeconds()).plus(SEND_GRACE).toNanos();
        long slipped = 0;
        long sent = 0;
        while (sent < run.getPlannedEvents() && !run.isStopRequested()) {
            long elapsed = System.nanoTime() - start;
            if (elapsed > sendLimit) {
                break;
            }
            long due = (long) Math.ceil(rate * ((elapsed - slipped) / 1e9));
            if (due - sent > maxCatchUp) {
                slipped += (long) ((due - sent - maxCatchUp) * 1e9 / rate);
                due = sent + maxCatchUp;
            }
            long target = Math.min(run.getPlannedEvents(), due);
            while (sent < target && !run.isStopRequested()) {
                int size = (int) Math.min(MAX_BATCH, target - sent);
                sendBatch(run, size);
                sent += size;
            }
            Thread.sleep(TICK);
        }
    }

    private void sampleWhileSending(DemoSession session, Run run, long baseline) {
        try {
            while (run.getPhase() == Run.Phase.SENDING) {
                sample(session, run, baseline);
                Thread.sleep(SAMPLE_INTERVAL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("Sampling a run failed", e);
        }
    }

    private static String completedMessage(Run run) {
        String text = "All " + run.getAccepted() + " accepted events are aggregated in PostgreSQL.";
        if (run.getRejected() > 0) {
            text += " " + run.getRejected() + " submitted events were refused by the ingestion API and never reached Kafka.";
        }
        if (run.getFailed() > 0) {
            text += " " + run.getFailed() + " submitted events got no acknowledgement and are not counted as accepted.";
        }
        return text;
    }

    private void sendBatch(Run run, int size) {
        Instant now = clock.instant();
        List<DemoEvent> events = new ArrayList<>(size);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            events.add(DemoEvent.of(UUID.randomUUID(), run.getType(), now, Math.round(random.nextDouble(1, 500) * 100) / 100.0));
        }
        run.recordSubmitted(size);
        IngestResult result = platform.ingest(run.tenantId(), events);
        if (result.isAccepted()) {
            run.recordAccepted(result.accepted());
        } else if (result.status() == 0 || result.status() >= 500) {
            run.recordFailed(size, describe(result));
        } else {
            run.recordRejected(size, result.status() == 429 ? result.limit() : null, describe(result));
        }
    }

    private static String describe(IngestResult result) {
        if (result.status() == 0) {
            return result.detail();
        }
        String text = "HTTP " + result.status() + (result.limit() == null ? "" : " (" + result.limit() + " limit)");
        return result.retryAfterSeconds() == null ? text : text + ", Retry-After " + result.retryAfterSeconds() + " s";
    }

    private void sample(DemoSession session, Run run, long baseline) {
        StatsResult stats = platform.stats(run.tenantId(), session.statsFrom(), session.statsTo(), run.getType());
        long processed = stats.isOk() ? Math.max(0, stats.totalCount() - baseline) : run.getProcessed();
        run.recordSample(clock.instant(), processed);
    }
}
