package io.highthroughput.demo.scenario;

import java.time.Instant;

/**
 * One request of the rate-limit scenario, as the ingestion API answered it.
 *
 * @param step                  which demonstration step sent it
 * @param events                events in the request
 * @param status                HTTP status; 0 if the API was unreachable
 * @param accepted              events accepted
 * @param limit                 {@code rate} or {@code quota} on a 429
 * @param retryAfterSeconds     {@code Retry-After} of a rejection
 * @param quotaRemainingBefore  tenant B's daily quota left before this request, counted from the 202s of this
 *                              session (tenant B exists only in this session)
 */
public record LimitAttempt(Instant at, String step, int events, int status, int accepted, String limit,
        Long retryAfterSeconds, String detail, long quotaRemainingBefore) {
}
