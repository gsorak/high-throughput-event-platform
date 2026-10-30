package io.highthroughput.processor.stats;

/** Mapped to 400. */
public class InvalidStatsQueryException extends RuntimeException {

    public InvalidStatsQueryException(String message) {
        super(message);
    }
}
