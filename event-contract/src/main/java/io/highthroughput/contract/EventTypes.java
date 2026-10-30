package io.highthroughput.contract;

import java.util.regex.Pattern;

/**
 * The event type format: dot-separated lowercase segments such as
 * {@code api.call} or {@code payment.card_declined},
 * at most 64 characters. Shared so that ingestion and the stats query accept
 * exactly the same types.
 */
public final class EventTypes {

    /**
     * For {@code @Pattern(regexp = ...)}; the length limit is checked separately.
     */
    public static final String PATTERN = "^[a-z0-9]+(\\.[a-z0-9_-]+)*$";

    public static final int MAX_LENGTH = 64;

    private static final Pattern VALID = Pattern.compile(PATTERN);

    private EventTypes() {
    }

    public static boolean isValid(String type) {
        return type != null && type.length() <= MAX_LENGTH && VALID.matcher(type).matches();
    }
}
