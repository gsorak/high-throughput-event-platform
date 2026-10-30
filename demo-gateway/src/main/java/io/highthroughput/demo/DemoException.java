package io.highthroughput.demo;

import org.springframework.http.HttpStatus;

/** A request the gateway refuses; rendered as {@code application/problem+json}. */
public class DemoException extends RuntimeException {

    private final HttpStatus status;
    private final String title;
    private final Long retryAfterSeconds;

    public DemoException(HttpStatus status, String title, String detail) {
        this(status, title, detail, null);
    }

    public DemoException(HttpStatus status, String title, String detail, Long retryAfterSeconds) {
        super(detail);
        this.status = status;
        this.title = title;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public HttpStatus status() {
        return status;
    }

    public String title() {
        return title;
    }

    public Long retryAfterSeconds() {
        return retryAfterSeconds;
    }

    public static DemoException busy(String detail) {
        return new DemoException(HttpStatus.CONFLICT, "Busy", detail);
    }
}
