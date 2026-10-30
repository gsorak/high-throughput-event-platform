package io.highthroughput.demo.scenario;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.platform.DemoEvent;
import io.highthroughput.demo.platform.IngestResult;
import io.highthroughput.demo.platform.PlatformClient;
import io.highthroughput.demo.platform.StatsResult;
import io.highthroughput.demo.session.DemoSession;
import io.highthroughput.demo.token.TokenIssuer;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tenant isolation: the platform answers each tenant from its verified token only, and rejects tokens that were
 * altered or signed by an unknown key. Every check is a real request to the platform; the gateway only compares the
 * answer with the expected one.
 */
public class IsolationService {

    static final String SCENARIO = "isolation";

    private final PlatformClient platform;
    private final TokenIssuer tokens;
    private final Clock clock;

    public IsolationService(PlatformClient platform, TokenIssuer tokens, Clock clock) {
        this.platform = platform;
        this.tokens = tokens;
        this.clock = clock;
    }

    public IsolationReport check(DemoSession session) {
        if (!session.tryBegin(SCENARIO)) {
            throw DemoException.busy("The isolation check is already running.");
        }
        try {
            String a = session.tenantA();
            String b = session.tenantB();
            String tokenA = tokens.issue(a);
            String tokenB = tokens.issue(b);
            String altered = tokens.withTenant(tokenA, b);
            String untrusted = tokens.issueUntrusted(b);

            List<IsolationReport.Check> checks = new ArrayList<>();
            checks.add(stats(session, "Tenant A reads its statistics", tokenA, "Signed by the platform's trusted issuer.",
                    200, a));
            checks.add(stats(session, "Tenant B reads its statistics", tokenB, "Signed by the platform's trusted issuer.",
                    200, b));
            checks.add(stats(session, "Tenant A's token, altered to claim tenant B", altered,
                    "Tenant A's valid token with tenant_id changed to B afterwards; the signature no longer matches.",
                    401, null));
            checks.add(ingest(session, "Ingest with the altered token", altered,
                    "The same altered token, used to write events for tenant B.", 401));
            checks.add(stats(session, "Token for tenant B from an unknown issuer key", untrusted,
                    "Correct claims, but signed with a key the platform does not trust.", 401, null));

            IsolationReport report = new IsolationReport(clock.instant(),
                    checks.stream().allMatch(IsolationReport.Check::passed), List.copyOf(checks));
            session.setIsolation(report);
            return report;
        } finally {
            session.end(SCENARIO);
        }
    }

    private IsolationReport.Check stats(DemoSession session, String title, String token, String note, int expected,
            String expectedTenant) {
        StatsResult result = platform.statsWithToken(token, session.statsFrom(), session.statsTo(), null);
        boolean passed = result.status() == expected
                && (expectedTenant == null || expectedTenant.equals(result.tenantId()));
        return new IsolationReport.Check(title, "GET /v1/stats", tokens.claimsOf(token), note, expected,
                result.status(), result.tenantId(), result.isOk() ? result.totalCount() : null, passed);
    }

    private IsolationReport.Check ingest(DemoSession session, String title, String token, String note, int expected) {
        if (!session.tryReserve(1)) {
            throw new DemoException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "Session budget exhausted", "The session's event budget is used up.");
        }
        IngestResult result = platform.ingestWithToken(token,
                List.of(DemoEvent.of(UUID.randomUUID(), "isolation.probe", clock.instant(), 1)));
        return new IsolationReport.Check(title, "POST /v1/events", tokens.claimsOf(token), note, expected,
                result.status(), null, null, result.status() == expected);
    }
}
