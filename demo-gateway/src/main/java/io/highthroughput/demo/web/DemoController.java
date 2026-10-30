package io.highthroughput.demo.web;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.scenario.DedupService;
import io.highthroughput.demo.scenario.IsolationService;
import io.highthroughput.demo.scenario.LimitsService;
import io.highthroughput.demo.scenario.Run;
import io.highthroughput.demo.scenario.RunService;
import io.highthroughput.demo.scenario.SystemMetrics;
import io.highthroughput.demo.session.DemoSession;
import io.highthroughput.demo.session.SessionStore;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * The demo API used by the UI. A session is identified only by the {@value #COOKIE} cookie; every action works on that
 * session's own tenants, so one visitor cannot reach another's session or data.
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    static final String COOKIE = "demo_session";

    private final SessionStore sessions;
    private final RunService runs;
    private final DedupService dedup;
    private final IsolationService isolation;
    private final LimitsService limits;
    private final SystemMetrics metrics;
    private final DemoProperties properties;
    private final Clock clock;

    public DemoController(SessionStore sessions, RunService runs, DedupService dedup, IsolationService isolation,
            LimitsService limits, SystemMetrics metrics, DemoProperties properties, Clock clock) {
        this.sessions = sessions;
        this.runs = runs;
        this.dedup = dedup;
        this.isolation = isolation;
        this.limits = limits;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
    }

    /** Returns the caller's live session, or starts a new one. */
    @PostMapping("/session")
    public ResponseEntity<SessionView> start(@CookieValue(name = COOKIE, required = false) String id,
            HttpServletRequest request) {
        Optional<DemoSession> existing = sessions.find(id);
        if (existing.isPresent()) {
            return ResponseEntity.ok(view(existing.get()));
        }
        DemoSession session = sessions.create(request.getRemoteAddr());
        return ResponseEntity.status(HttpStatus.CREATED)
                .header(HttpHeaders.SET_COOKIE, cookie(session.id(), remaining(session)).toString())
                .body(view(session));
    }

    @GetMapping("/session")
    public SessionView get(@CookieValue(name = COOKIE, required = false) String id) {
        return view(session(id));
    }

    @DeleteMapping("/session")
    public ResponseEntity<Void> end(@CookieValue(name = COOKIE, required = false) String id) {
        sessions.find(id).ifPresent(sessions::remove);
        return ResponseEntity.noContent()
                .header(HttpHeaders.SET_COOKIE, cookie("", Duration.ZERO).toString())
                .build();
    }

    @PostMapping("/runs")
    public SessionView startRun(@CookieValue(name = COOKIE, required = false) String id,
            @RequestBody RunService.RunRequest request) {
        DemoSession session = session(id);
        runs.start(session, request);
        return view(session);
    }

    @PostMapping("/runs/current/stop")
    public SessionView stopRun(@CookieValue(name = COOKIE, required = false) String id) {
        DemoSession session = session(id);
        runs.stop(session);
        return view(session);
    }

    @PostMapping("/dedup/send")
    public SessionView sendBatch(@CookieValue(name = COOKIE, required = false) String id) {
        DemoSession session = session(id);
        dedup.send(session);
        return view(session);
    }

    @PostMapping("/dedup/replay")
    public SessionView replayBatch(@CookieValue(name = COOKIE, required = false) String id) {
        DemoSession session = session(id);
        dedup.replay(session);
        return view(session);
    }

    @PostMapping("/isolation/check")
    public SessionView checkIsolation(@CookieValue(name = COOKIE, required = false) String id) {
        DemoSession session = session(id);
        isolation.check(session);
        return view(session);
    }

    @PostMapping("/limits/{step}")
    public SessionView limits(@CookieValue(name = COOKIE, required = false) String id, @PathVariable String step) {
        LimitsService.Step parsed;
        try {
            parsed = LimitsService.Step.valueOf(step.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new DemoException(HttpStatus.NOT_FOUND, "Unknown step", "Steps are within, burst and quota.");
        }
        DemoSession session = session(id);
        limits.run(session, parsed);
        return view(session);
    }

    /** Platform-wide metrics; only for visitors with a session, and from fixed queries only. */
    @GetMapping("/overview")
    public SystemMetrics.Overview overview(@CookieValue(name = COOKIE, required = false) String id,
            @RequestParam(defaultValue = "5m") String window) {
        session(id);
        return metrics.overview(SystemMetrics.Window.of(window).orElseThrow(() -> new DemoException(
                HttpStatus.BAD_REQUEST, "Unknown window", "The overview window is 5m or 15m.")));
    }

    private DemoSession session(String id) {
        return sessions.find(id).orElseThrow(() -> new DemoException(HttpStatus.UNAUTHORIZED, "No demo session",
                "There is no active demo session; it may have expired. Start a new one."));
    }

    private Duration remaining(DemoSession session) {
        return Duration.between(clock.instant(), session.expiresAt());
    }

    private ResponseCookie cookie(String value, Duration maxAge) {
        return ResponseCookie.from(COOKIE, value)
                .path("/demo")
                .httpOnly(true)
                .secure(properties.cookieSecure())
                .sameSite("Strict")
                .maxAge(maxAge)
                .build();
    }

    private SessionView view(DemoSession session) {
        DemoProperties.Limits limits = properties.limits();
        Instant now = clock.instant();
        Optional<Run> active = runs.active();
        return new SessionView(
                session.expiresAt(),
                Math.max(0, Duration.between(now, session.expiresAt()).toSeconds()),
                List.of(tenant("a", session.tenantA(), properties.tenantA()),
                        tenant("b", session.tenantB(), properties.tenantB())),
                new SessionView.Budget(session.eventsUsed(), session.eventBudget()),
                new SessionView.RunLimits(limits.maxEventsPerSecond(), limits.maxRunDuration().toSeconds(),
                        limits.maxEventsPerRun(), RunService.EVENT_TYPES),
                session.run(),
                new SessionView.SystemRun(active.isPresent(), active.isPresent() && active.get() == session.run(),
                        active.map(r -> Duration.between(r.getStartedAt(), now).toSeconds()).orElse(null)),
                session.dedup(),
                session.isolation(),
                rateLimits(session, now),
                session.busy());
    }

    private SessionView.RateLimits rateLimits(DemoSession session, Instant now) {
        long used = session.limitQuotaUsed(now);
        long remaining = Math.max(0, properties.tenantB().dailyQuota() - used);
        return new SessionView.RateLimits(session.limitAttempts(), used, remaining, remaining == 0,
                LocalDate.ofInstant(now, ZoneOffset.UTC).plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant());
    }

    private static SessionView.Tenant tenant(String key, String id, DemoProperties.TenantProfile profile) {
        return new SessionView.Tenant(key, id, profile.name(), profile.plan(), profile.eventsPerSecond(),
                profile.burst(), profile.dailyQuota());
    }
}
