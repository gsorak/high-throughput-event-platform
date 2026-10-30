package io.highthroughput.demo.session;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.DemoProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory demo sessions. Creation is serialised so the global and per-client caps cannot be exceeded by concurrent
 * requests; expired sessions are rejected on access and swept periodically. A restart of the gateway ends all
 * sessions, which is acceptable for a demo (ADR-0015).
 */
public class SessionStore {

    private static final Logger log = LoggerFactory.getLogger(SessionStore.class);

    private final DemoProperties.Limits limits;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, DemoSession> sessions = new ConcurrentHashMap<>();

    public SessionStore(DemoProperties.Limits limits, Clock clock) {
        this.limits = limits;
        this.clock = clock;
    }

    public synchronized DemoSession create(String clientAddress) {
        Instant now = clock.instant();
        removeExpired(now);
        if (sessions.size() >= limits.maxSessions()) {
            long retryAfter = sessions.values().stream().map(DemoSession::expiresAt).min(Instant::compareTo)
                    .map(expiry -> Math.max(1, Duration.between(now, expiry).toSeconds())).orElse(60L);
            throw new DemoException(HttpStatus.SERVICE_UNAVAILABLE, "Demo at capacity",
                    "All " + limits.maxSessions() + " demo sessions are in use. A session frees up within "
                            + retryAfter + " seconds.", retryAfter);
        }
        long fromClient = sessions.values().stream().filter(s -> s.clientAddress().equals(clientAddress)).count();
        if (fromClient >= limits.maxSessionsPerClient()) {
            throw new DemoException(HttpStatus.TOO_MANY_REQUESTS, "Too many sessions",
                    "This network address already has " + fromClient + " active demo sessions.", 60L);
        }
        String suffix = HexFormat.of().formatHex(randomBytes(8));
        DemoSession session = new DemoSession(Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes(32)),
                clientAddress, now, limits.sessionTtl(), "demo-" + suffix + "-a", "demo-" + suffix + "-b",
                limits.maxEventsPerSession());
        sessions.put(session.id(), session);
        log.info("Demo session created for tenants {} and {} ({} active)", session.tenantA(), session.tenantB(),
                sessions.size());
        return session;
    }

    /** The live session with this id; an expired one is removed and not returned. */
    public Optional<DemoSession> find(String id) {
        if (id == null) {
            return Optional.empty();
        }
        DemoSession session = sessions.get(id);
        if (session == null) {
            return Optional.empty();
        }
        if (session.isExpired(clock.instant())) {
            remove(session);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    public void remove(DemoSession session) {
        if (sessions.remove(session.id(), session)) {
            session.close();
            log.info("Demo session for tenants {} and {} ended ({} active)", session.tenantA(), session.tenantB(),
                    sessions.size());
        }
    }

    public int activeSessions() {
        return sessions.size();
    }

    @Scheduled(fixedDelayString = "PT10S")
    public void removeExpired() {
        removeExpired(clock.instant());
    }

    private void removeExpired(Instant now) {
        sessions.values().stream().filter(s -> s.isExpired(now)).toList().forEach(this::remove);
    }

    private byte[] randomBytes(int count) {
        byte[] bytes = new byte[count];
        random.nextBytes(bytes);
        return bytes;
    }
}
