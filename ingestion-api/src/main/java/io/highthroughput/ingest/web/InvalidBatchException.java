package io.highthroughput.ingest.web;

import java.util.List;

/**
 * The batch violates at least one rule; nothing was published. Mapped to 400.
 */
public class InvalidBatchException extends RuntimeException {

    private final List<FieldViolation> violations;

    public InvalidBatchException(String message, List<FieldViolation> violations) {
        super(message);
        this.violations = List.copyOf(violations);
    }

    public List<FieldViolation> violations() {
        return violations;
    }
}
