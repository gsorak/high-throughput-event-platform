package io.highthroughput.demo.scenario;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.platform.DemoEvent;
import io.highthroughput.demo.platform.IngestResult;
import io.highthroughput.demo.platform.PlatformClient;
import io.highthroughput.demo.session.DemoSession;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Rate limits and quotas on the small-plan tenant B, enforced by the ingestion API itself. Nothing here resets limiter
 * state: once B's daily quota is used up it stays so until 00:00 UTC, or the visitor starts a new session with new
 * tenants.
 */
public class LimitsService {

    public enum Step {
        WITHIN, BURST, QUOTA
    }

    static final String SCENARIO = "limits";
    private static final int SMALL_REQUEST = 50;
    private static final int LARGE_REQUEST = 500;
    private static final int MAX_QUOTA_ATTEMPTS = 12;
    private static final long MAX_WAIT_SECONDS = 6;

    private final PlatformClient platform;
    private final DemoProperties.TenantProfile tenantB;
    private final Clock clock;

    public LimitsService(PlatformClient platform, DemoProperties.TenantProfile tenantB, Clock clock) {
        this.platform = platform;
        this.tenantB = tenantB;
        this.clock = clock;
    }

    public List<LimitAttempt> run(DemoSession session, Step step) {
        if (!session.tryBegin(SCENARIO)) {
            throw DemoException.busy("A rate-limit step is already running.");
        }
        try {
            List<LimitAttempt> attempts = new ArrayList<>();
            switch (step) {
                case WITHIN -> attempts.add(send(session, "within", SMALL_REQUEST));
                case BURST -> {
                    attempts.add(send(session, "burst", LARGE_REQUEST));
                    attempts.add(send(session, "burst", LARGE_REQUEST));
                }
                case QUOTA -> exhaustQuota(session, attempts);
            }
            return attempts;
        } finally {
            session.end(SCENARIO);
        }
    }

    /**
     * Uses up what is left of B's daily quota, waiting out rate rejections as {@code Retry-After} asks, then sends a
     * single event to show that even that is refused. Stops at the first quota rejection.
     */
    private void exhaustQuota(DemoSession session, List<LimitAttempt> attempts) {
        for (int i = 0; i < MAX_QUOTA_ATTEMPTS && !session.isClosed(); i++) {
            long remaining = tenantB.dailyQuota() - session.limitQuotaUsed(clock.instant());
            LimitAttempt attempt = send(session, "quota", (int) Math.max(1, Math.min(LARGE_REQUEST, remaining)));
            attempts.add(attempt);
            if (attempt.status() == 429 && "rate".equals(attempt.limit())) {
                long wait = Math.min(MAX_WAIT_SECONDS,
                        attempt.retryAfterSeconds() == null ? 1 : Math.max(1, attempt.retryAfterSeconds()));
                try {
                    Thread.sleep(Duration.ofSeconds(wait));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } else if (attempt.status() != 202) {
                return;
            }
        }
    }

    private LimitAttempt send(DemoSession session, String step, int size) {
        if (!session.tryReserve(size)) {
            throw new DemoException(HttpStatus.UNPROCESSABLE_CONTENT, "Session budget exhausted",
                    "The session's event budget of " + session.eventBudget() + " is used up.");
        }
        Instant now = clock.instant();
        long quotaRemaining = Math.max(0, tenantB.dailyQuota() - session.limitQuotaUsed(now));
        List<DemoEvent> events = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            events.add(DemoEvent.of(UUID.randomUUID(), "api.call", now, 1));
        }
        IngestResult result = platform.ingest(session.tenantB(), events);
        LimitAttempt attempt = new LimitAttempt(now, step, size, result.status(), result.accepted(), result.limit(),
                result.retryAfterSeconds(), result.detail(), quotaRemaining);
        session.addLimitAttempt(attempt);
        return attempt;
    }
}
