package io.highthroughput.ingest.ratelimit;

import java.time.Duration;

/**
 * The tenant's batch does not fit its plan. Mapped to 429 with
 * {@code Retry-After}.
 */
public class RateLimitExceededException extends RuntimeException {

    public enum Limit {
        /** Events per second, with burst. */
        RATE,
        /** Events per UTC day. */
        QUOTA
    }

    private final Limit limit;
    private final Duration retryAfter;

    public RateLimitExceededException(Limit limit, Duration retryAfter) {
        // QUOTA also covers a batch larger than what is left of the quota, so
        // "exhausted" would be wrong.
        super(limit == Limit.QUOTA ? "The batch exceeds this tenant's remaining daily event quota"
                : "This tenant is sending events faster than its plan allows");
        this.limit = limit;
        this.retryAfter = retryAfter;
    }

    public Limit limit() {
        return limit;
    }

    /**
     * When the whole batch is expected to fit again; whole seconds, at least one.
     */
    public Duration retryAfter() {
        return retryAfter;
    }
}
