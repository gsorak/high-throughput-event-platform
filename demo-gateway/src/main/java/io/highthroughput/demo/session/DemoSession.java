package io.highthroughput.demo.session;

import io.highthroughput.demo.platform.DemoEvent;
import io.highthroughput.demo.scenario.DedupState;
import io.highthroughput.demo.scenario.IsolationReport;
import io.highthroughput.demo.scenario.LimitAttempt;
import io.highthroughput.demo.scenario.Run;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One visitor's demo: two temporary tenants, an event budget and the latest result of every scenario. Results stay
 * until the session expires, so reloading the page shows them again.
 */
public final class DemoSession {

    private static final int MAX_LIMIT_ATTEMPTS = 20;

    private final String id;
    private final String clientAddress;
    private final Instant createdAt;
    private final Instant expiresAt;
    private final String tenantA;
    private final String tenantB;
    private final long eventBudget;
    private final AtomicLong eventsUsed = new AtomicLong();
    private final Set<String> busy = ConcurrentHashMap.newKeySet();
    private final Deque<LimitAttempt> limitAttempts = new ArrayDeque<>();
    private long limitEventsAccepted;
    private LocalDate limitQuotaDay;

    private volatile boolean closed;
    private volatile Run run;
    private volatile DedupState dedup;
    private volatile List<DemoEvent> dedupBatch;
    private volatile IsolationReport isolation;

    DemoSession(String id, String clientAddress, Instant createdAt, Duration ttl, String tenantA, String tenantB,
            long eventBudget) {
        this.id = id;
        this.clientAddress = clientAddress;
        this.createdAt = createdAt;
        this.expiresAt = createdAt.plus(ttl);
        this.tenantA = tenantA;
        this.tenantB = tenantB;
        this.eventBudget = eventBudget;
    }

    public String id() {
        return id;
    }

    String clientAddress() {
        return clientAddress;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant expiresAt() {
        return expiresAt;
    }

    public boolean isExpired(Instant now) {
        return !now.isBefore(expiresAt);
    }

    /** The statistics window that covers every event of this session. */
    public Instant statsFrom() {
        return createdAt.truncatedTo(ChronoUnit.MINUTES).minus(Duration.ofMinutes(1));
    }

    public Instant statsTo() {
        return expiresAt.plus(Duration.ofMinutes(10));
    }

    public String tenantA() {
        return tenantA;
    }

    public String tenantB() {
        return tenantB;
    }

    public long eventBudget() {
        return eventBudget;
    }

    public long eventsUsed() {
        return eventsUsed.get();
    }

    /** Reserves {@code events} of the session budget; all-or-nothing and safe under concurrent requests. */
    public boolean tryReserve(long events) {
        while (true) {
            long used = eventsUsed.get();
            if (closed || used + events > eventBudget) {
                return false;
            }
            if (eventsUsed.compareAndSet(used, used + events)) {
                return true;
            }
        }
    }

    /** Returns reserved but unsent events to the budget. */
    public void refund(long events) {
        if (events > 0) {
            eventsUsed.addAndGet(-events);
        }
    }

    /** Marks {@code scenario} as running; false if it already is, so one session cannot run it twice at once. */
    public boolean tryBegin(String scenario) {
        return !closed && busy.add(scenario);
    }

    public void end(String scenario) {
        busy.remove(scenario);
    }

    public Set<String> busy() {
        return Set.copyOf(busy);
    }

    public boolean isClosed() {
        return closed;
    }

    /** Called once when the session expires or is ended: a run still sending stops. */
    void close() {
        closed = true;
        Run current = run;
        if (current != null) {
            current.requestStop();
        }
    }

    public Run run() {
        return run;
    }

    public void setRun(Run run) {
        this.run = run;
    }

    public DedupState dedup() {
        return dedup;
    }

    public void setDedup(DedupState dedup) {
        this.dedup = dedup;
    }

    public List<DemoEvent> dedupBatch() {
        return dedupBatch;
    }

    public void setDedupBatch(List<DemoEvent> dedupBatch) {
        this.dedupBatch = List.copyOf(dedupBatch);
    }

    public IsolationReport isolation() {
        return isolation;
    }

    public void setIsolation(IsolationReport isolation) {
        this.isolation = isolation;
    }

    public synchronized void addLimitAttempt(LimitAttempt attempt) {
        limitAttempts.addFirst(attempt);
        while (limitAttempts.size() > MAX_LIMIT_ATTEMPTS) {
            limitAttempts.removeLast();
        }
        LocalDate day = LocalDate.ofInstant(attempt.at(), ZoneOffset.UTC);
        if (!day.equals(limitQuotaDay)) {
            limitQuotaDay = day;
            limitEventsAccepted = 0;
        }
        limitEventsAccepted += attempt.accepted();
    }

    /**
     * Events of tenant B the ingestion API accepted in this session on the UTC day of {@code now}. Tenant B is used
     * only by this session, so this is what its daily quota has consumed; the quota refills at 00:00 UTC.
     */
    public synchronized long limitQuotaUsed(Instant now) {
        return LocalDate.ofInstant(now, ZoneOffset.UTC).equals(limitQuotaDay) ? limitEventsAccepted : 0;
    }

    /** Newest first. */
    public synchronized List<LimitAttempt> limitAttempts() {
        return List.copyOf(limitAttempts);
    }
}
