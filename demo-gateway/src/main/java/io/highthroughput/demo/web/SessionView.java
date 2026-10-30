package io.highthroughput.demo.web;

import io.highthroughput.demo.scenario.DedupState;
import io.highthroughput.demo.scenario.IsolationReport;
import io.highthroughput.demo.scenario.LimitAttempt;
import io.highthroughput.demo.scenario.Run;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Everything the UI shows about a session. Contains no token or key material: the tenant ids are the visitor's own,
 * and tokens are minted per request inside the gateway.
 */
public record SessionView(
        Instant expiresAt,
        long secondsRemaining,
        List<Tenant> tenants,
        Budget budget,
        RunLimits runLimits,
        Run run,
        SystemRun systemRun,
        DedupState dedup,
        IsolationReport isolation,
        RateLimits rateLimits,
        Set<String> busy) {

    public record Tenant(String key, String id, String name, String plan, int eventsPerSecond, int burst,
            long dailyQuota) {
    }

    public record Budget(long used, long limit) {
    }

    public record RunLimits(int maxEventsPerSecond, long maxDurationSeconds, int maxEventsPerRun,
            List<String> eventTypes) {
    }

    /**
     * @param busy           a run holds the system-wide slot
     * @param yours          it is this session's run
     * @param secondsRunning how long it has been running
     */
    public record SystemRun(boolean busy, boolean yours, Long secondsRunning) {
    }

    /**
     * @param attempts       newest first
     * @param acceptedEvents events of tenant B accepted in this session today (UTC); they count against its daily quota
     * @param quotaRemaining daily quota minus {@code acceptedEvents}; exact because tenant B exists only in this session
     * @param quotaExhausted {@code quotaRemaining} is zero: every further event is refused until the reset
     * @param quotaResetsAt  next reset of the daily quota (00:00 UTC)
     */
    public record RateLimits(List<LimitAttempt> attempts, long acceptedEvents, long quotaRemaining,
            boolean quotaExhausted, Instant quotaResetsAt) {
    }
}
