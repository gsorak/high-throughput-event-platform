package io.highthroughput.ingest.web;

/**
 * Thrown by {@link AdmissionControlFilter} when all permits are taken. Mapped
 * to 503 with {@code Retry-After}.
 */
public class ServerBusyException extends RuntimeException {

    public ServerBusyException() {
        super("Too many requests are in progress on this instance. Retry later.");
    }
}
