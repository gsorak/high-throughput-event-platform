package io.highthroughput.demo;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

/**
 * Demo gateway settings. The limits are enforced server-side; the UI only mirrors them.
 *
 * @param ingestionUrl  base URL of ingestion-api
 * @param processorUrl  base URL of event-processor (statistics API)
 * @param prometheusUrl base URL of Prometheus; queried server-side with fixed queries only
 * @param grafanaUrl    link shown in the UI when set; leave empty for a public deployment
 * @param uiLocation    location of the built UI ({@code demo-ui/dist}); empty serves the API only
 * @param cookieSecure  mark the session cookie {@code Secure}; required behind HTTPS
 * @param tenantA       the standard-plan tenant of each session
 * @param tenantB       the small-plan tenant of each session
 */
@Validated
@ConfigurationProperties("demo")
public record DemoProperties(
        @NotNull URI ingestionUrl,
        @NotNull URI processorUrl,
        @NotNull URI prometheusUrl,
        String grafanaUrl,
        String uiLocation,
        boolean cookieSecure,
        @NotNull Duration backendTimeout,
        @NotNull @Valid Limits limits,
        @NotNull @Valid TenantProfile tenantA,
        @NotNull @Valid TenantProfile tenantB) {

    /**
     * @param maxSessions          concurrent sessions system-wide
     * @param maxSessionsPerClient concurrent sessions per client address
     * @param sessionTtl           lifetime of a session; not extended by activity
     * @param maxEventsPerSecond   highest rate a run may request
     * @param maxRunDuration       longest sending phase of a run
     * @param maxEventsPerRun      events one run may send
     * @param maxEventsPerSession  events all actions of one session may send
     * @param drainTimeout         how long a run waits for its backlog after sending
     */
    public record Limits(
            @Min(1) @Max(1000) int maxSessions,
            @Min(1) int maxSessionsPerClient,
            @NotNull Duration sessionTtl,
            @Min(1) @Max(1000) int maxEventsPerSecond,
            @NotNull Duration maxRunDuration,
            @Min(1) int maxEventsPerRun,
            @Min(1) long maxEventsPerSession,
            @NotNull Duration drainTimeout) {
    }

    /**
     * Display values of a tenant's plan; must match {@code rate-limit.plans} of ingestion-api, which enforces them.
     */
    public record TenantProfile(
            @NotBlank String name,
            @NotBlank String plan,
            @Min(1) int eventsPerSecond,
            @Min(1) int burst,
            @Min(1) long dailyQuota) {
    }
}
