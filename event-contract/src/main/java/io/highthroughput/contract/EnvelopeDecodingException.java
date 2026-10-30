package io.highthroughput.contract;

/**
 * A record value could not be turned into a valid {@link EventEnvelope}.
 * Retrying cannot fix this, so consumers treat
 * it as non-retryable (skip in Stage 1, dead-letter topic in Stage 3).
 */
public class EnvelopeDecodingException extends RuntimeException {

    public EnvelopeDecodingException(String message) {
        super(message);
    }

    public EnvelopeDecodingException(String message, Throwable cause) {
        super(message, cause);
    }
}
