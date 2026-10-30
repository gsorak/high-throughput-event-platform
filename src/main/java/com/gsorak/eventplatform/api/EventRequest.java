package com.gsorak.eventplatform.api;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventRequest(
        @NotNull UUID eventId,
        @NotBlank @Size(max = 128) String type,
        @NotNull Instant occurredAt,
        @NotEmpty Map<String, Object> payload
) {
}
