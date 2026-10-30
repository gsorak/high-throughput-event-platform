package io.highthroughput.contract;

import java.util.regex.Pattern;

/**
 * The tenant identifier format shared by the HTTP edge and the Kafka contract.
 * Lowercase, starts with a letter or digit, 2–63 characters — safe to use in
 * URLs, logs and SQL parameters.
 */
public final class TenantIds {

    private static final Pattern VALID = Pattern.compile("^[a-z0-9][a-z0-9-]{1,62}$");

    /**
     * JWT claim that carries the tenant; both services read the tenant only from a
     * verified token.
     */
    public static final String JWT_CLAIM = "tenant_id";

    private TenantIds() {
    }

    public static boolean isValid(String tenantId) {
        return tenantId != null && VALID.matcher(tenantId).matches();
    }
}
