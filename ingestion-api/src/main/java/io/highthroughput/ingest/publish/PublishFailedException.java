package io.highthroughput.ingest.publish;

/**
 * Kafka did not acknowledge every event of a batch before the deadline. Some
 * events may have been written anyway,
 * which is why the client must retry the whole batch with the same eventIds.
 * Mapped to 503 + Retry-After.
 */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
