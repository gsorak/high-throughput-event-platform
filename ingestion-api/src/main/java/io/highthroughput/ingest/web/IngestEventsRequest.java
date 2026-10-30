package io.highthroughput.ingest.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Body of {@code POST /v1/events}. The tenant is deliberately not part of the
 * body: it comes from the caller's identity.
 */
public record IngestEventsRequest(
        @NotNull @Size(min = 1, max = MAX_EVENTS) List<@NotNull @Valid EventPayload> events) {

    public static final int MAX_EVENTS = 500;
}
