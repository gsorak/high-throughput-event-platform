package io.highthroughput.demo.scenario;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A load run of one session. Events count as <em>processed</em> only once the statistics API reports them
 * aggregated in PostgreSQL; a 202 means only that Kafka acknowledged them.
 */
public final class Run {

    public enum Phase {
        /** The generator is sending. */
        SENDING,
        /** Sending ended; waiting until every accepted event is aggregated. */
        PROCESSING,
        /** Every accepted event is aggregated. */
        COMPLETED,
        /** The backlog did not drain within the drain timeout. */
        INCOMPLETE,
        /** The run could not be carried out, see {@code message}. */
        FAILED
    }

    private final String tenantId;
    private final String type;
    private final int eventsPerSecond;
    private final int durationSeconds;
    private final long plannedEvents;
    private final Instant startedAt;
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong rejectedByRate = new AtomicLong();
    private final AtomicLong rejectedByQuota = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final List<Point> timeline = new CopyOnWriteArrayList<>();

    private volatile Phase phase = Phase.SENDING;
    private volatile long processed;
    private volatile Instant sendingEndedAt;
    private volatile Instant finishedAt;
    private volatile String message;
    private volatile String lastRejection;
    private volatile boolean stopRequested;

    Run(String tenantId, String type, int eventsPerSecond, int durationSeconds, Instant startedAt) {
        this.tenantId = tenantId;
        this.type = type;
        this.eventsPerSecond = eventsPerSecond;
        this.durationSeconds = durationSeconds;
        this.plannedEvents = (long) eventsPerSecond * durationSeconds;
        this.startedAt = startedAt;
    }

    public String tenantId() {
        return tenantId;
    }

    public String getType() {
        return type;
    }

    public int getEventsPerSecond() {
        return eventsPerSecond;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public long getPlannedEvents() {
        return plannedEvents;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Phase getPhase() {
        return phase;
    }

    public long getSubmitted() {
        return submitted.get();
    }

    public long getAccepted() {
        return accepted.get();
    }

    /** Refused by the ingestion API with a 4xx; these events never reached Kafka. */
    public long getRejected() {
        return rejected.get();
    }

    /** Part of {@link #getRejected()}: HTTP 429 for the events-per-second limit. */
    public long getRejectedByRate() {
        return rejectedByRate.get();
    }

    /** Part of {@link #getRejected()}: HTTP 429 for the daily quota. */
    public long getRejectedByQuota() {
        return rejectedByQuota.get();
    }

    public long getFailed() {
        return failed.get();
    }

    public long getProcessed() {
        return processed;
    }

    /** Accepted by Kafka but not yet aggregated. */
    public long getPending() {
        return Math.max(0, accepted.get() - processed);
    }

    public Instant getSendingEndedAt() {
        return sendingEndedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public String getMessage() {
        return message;
    }

    public String getLastRejection() {
        return lastRejection;
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    public List<Point> getTimeline() {
        return List.copyOf(timeline);
    }

    @JsonIgnore
    public boolean isFinished() {
        return phase == Phase.COMPLETED || phase == Phase.INCOMPLETE || phase == Phase.FAILED;
    }

    public void requestStop() {
        stopRequested = true;
    }

    void recordSubmitted(int events) {
        submitted.addAndGet(events);
    }

    void recordAccepted(int events) {
        accepted.addAndGet(events);
    }

    /** @param limit {@code rate} or {@code quota} for a 429, else null */
    void recordRejected(int events, String limit, String reason) {
        rejected.addAndGet(events);
        if ("rate".equals(limit)) {
            rejectedByRate.addAndGet(events);
        } else if ("quota".equals(limit)) {
            rejectedByQuota.addAndGet(events);
        }
        lastRejection = reason;
    }

    void recordFailed(int events, String reason) {
        failed.addAndGet(events);
        lastRejection = reason;
    }

    void recordSample(Instant at, long processedNow) {
        long acceptedNow = accepted.get();
        processed = Math.min(processedNow, acceptedNow);
        timeline.add(new Point((at.toEpochMilli() - startedAt.toEpochMilli()) / 1000.0, submitted.get(), acceptedNow,
                processed));
    }

    void sendingEnded(Instant at) {
        sendingEndedAt = at;
        phase = Phase.PROCESSING;
    }

    void finish(Phase result, Instant at, String text) {
        message = text;
        finishedAt = at;
        phase = result;
    }

    /**
     * Cumulative counts of this run only.
     *
     * @param seconds since the start of the run
     */
    public record Point(double seconds, long submitted, long accepted, long processed) {
    }
}
