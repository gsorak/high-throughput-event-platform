package io.highthroughput.demo.platform;

/**
 * Outcome of one {@code GET /v1/stats}.
 *
 * @param status     HTTP status; 0 if the API was unreachable
 * @param tenantId   tenant the platform answered for (taken from the verified token)
 * @param totalCount aggregated events in the window
 */
public record StatsResult(int status, String tenantId, long totalCount, double totalValueSum) {

    public boolean isOk() {
        return status == 200;
    }
}
