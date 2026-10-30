package io.highthroughput.demo.scenario;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Result of the tenant isolation checks of a session. */
public record IsolationReport(Instant checkedAt, boolean allPassed, List<Check> checks) {

    /**
     * @param title             what was attempted
     * @param request           the platform request that was made
     * @param claims            claims of the token that was sent, decoded for display; the token itself is not shown
     * @param tokenNote         how the token was produced
     * @param expectedStatus    what the platform must answer
     * @param actualStatus      what it answered; 0 if it was unreachable
     * @param answeredForTenant tenant the statistics API answered for, from the verified token
     */
    public record Check(
            String title,
            String request,
            Map<String, Object> claims,
            String tokenNote,
            int expectedStatus,
            int actualStatus,
            String answeredForTenant,
            Long totalCount,
            boolean passed) {
    }
}
