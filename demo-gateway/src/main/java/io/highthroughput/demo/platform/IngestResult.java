package io.highthroughput.demo.platform;

/**
 * Outcome of one {@code POST /v1/events}.
 *
 * @param status            HTTP status; 0 if the API was unreachable
 * @param accepted          events acknowledged by Kafka (202 only)
 * @param limit             {@code rate} or {@code quota} on a 429
 * @param retryAfterSeconds {@code Retry-After} header, if any
 * @param detail            problem detail of a rejection
 */
public record IngestResult(int status, int accepted, String limit, Long retryAfterSeconds, String detail) {

    static IngestResult unreachable(Exception e) {
        return new IngestResult(0, 0, null, null, "Ingestion API unreachable: " + e.getClass().getSimpleName());
    }

    public boolean isAccepted() {
        return status == 202;
    }
}
