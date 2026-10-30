package io.highthroughput.ingest.web;

/**
 * One validation problem, addressed by an RFC 6901 JSON Pointer into the
 * request body
 * (e.g. {@code /events/3/type}), so clients can map it back to the offending
 * event.
 */
public record FieldViolation(String pointer, String message) {
}
