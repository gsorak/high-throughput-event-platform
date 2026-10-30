package io.highthroughput.demo.scenario;

import io.highthroughput.demo.platform.PrometheusClient;
import io.highthroughput.demo.platform.PrometheusClient.Point;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Whole-platform metrics for the UI, from fixed Prometheus queries. The
 * platform has no per-tenant metrics, so these
 * include every visitor's traffic; session-specific numbers come from the
 * statistics API instead.
 */
public class SystemMetrics {

    private static final String LAG = "kafka_consumergroup_lag{consumergroup=\"event-processor\",topic=\"events.v1\"}";
    private static final String COMMITTED = "kafka_consumergroup_current_offset{consumergroup=\"event-processor\",topic=\"events.v1\"}";
    /**
     * kafka-exporter reports a committed offset of -1 (and a lag of -1) for a
     * partition without a commit. The lag is
     * clamped to 0 and the events such a partition already holds (log end - oldest;
     * the processor reads from the earliest
     * offset) are added back. The partition is selected by its committed offset,
     * the
     * exact sentinel, not by a lag of -1.
     * No consumer-group series at all gives no result (unknown).
     */
    static final String BACKLOG = "sum(clamp_min(" + LAG
            + ", 0)) + (sum((kafka_topic_partition_current_offset{topic=\"events.v1\"}"
            + " - kafka_topic_partition_oldest_offset{topic=\"events.v1\"}) and on(topic, partition) (" + COMMITTED
            + " == -1)) or vector(0))";
    static final String DUPLICATES_TOTAL = "sum(processor_events_duplicate_total)";
    static final String INGEST_RATE = "sum(rate(ingestion_events_accepted_total[30s]))";
    static final String PROCESS_RATE = "sum(rate(processor_events_aggregated_total[30s]))";
    /**
     * Prometheus scrapes every 5 s;
     * three samples per point keep a
     * 10-second run visible as a peak. A scrape interval above 7.5 s would leave
     * these rates empty.
     */
    static final String INGEST_RATE_SERIES = "sum(rate(ingestion_events_accepted_total[15s]))";
    static final String PROCESS_RATE_SERIES = "sum(rate(processor_events_aggregated_total[15s]))";
    private static final String HTTP_EVENTS = "http_server_requests_seconds_count{application=\"ingestion-api\",uri=\"/v1/events\",";
    private static final Duration CACHE_TTL = Duration.ofSeconds(3);

    /**
     * Events (or requests) added to a counter during the window. increase() handles
     * counter resets, and the sum is
     * taken after it, so a restarted replica does not distort the total. No series
     * gives no result, not 0.
     */
    static String increase(String selector, Window window) {
        return "sum(increase(" + selector + "[" + window.key + "]))";
    }

    /**
     * Exact number of increments of a counter during the window. increase()
     * extrapolates to the window edges and
     * overstates a burst by up to window/scrape-span (15,000 events showed as
     * 15,069 and 15,216). Here the per-step
     * deltas telescope to the real difference; a negative delta is a counter reset,
     * after which the new value counts.
     * No series gives no result, not 0.
     */
    static String counted(String selector, Window window) {
        String step = window.step.toSeconds() + "s";
        String previous = selector + " offset " + step;
        return "sum(sum_over_time(clamp_min(" + selector + " - " + previous + ", 0)[" + window.key + ":" + step
                + "])) + (sum(sum_over_time((" + selector + " < " + previous + ")[" + window.key + ":" + step
                + "])) or vector(0))";
    }

    /**
     * Counters whose series only appear with the first occurrence (an HTTP status);
     * absent means 0. increase() needs two samples
     * and never sees the jump from "no series" to the first value, so a series born
     * inside the window also counts its first
     * value (its minimum over the window). A series is new if it had no sample in
     * the 10 minutes before the window start; last_over_time skips the stale marker
     * of a failed scrape that a plain offset would hit, and 10 minutes covers many
     * 5 s scrape intervals. Caveat: a series both born and reset
     * within one window undercounts.
     */
    static String increaseOrZero(String selector, Window window) {
        return "(" + increase(selector, window) + " or vector(0)) + (sum(min_over_time(" + selector + "[" + window.key
                + "]) unless last_over_time(" + selector + "[10m] offset " + window.key + ")) or vector(0))";
    }

    /**
     * The chart ranges the UI offers; only these, so a visitor never influences a
     * query.
     */
    public enum Window {
        FIVE_MINUTES("5m", Duration.ofMinutes(5), Duration.ofSeconds(5)),
        // Same 5 s step as the short window: the peak is the highest plotted 15 s
        // average, and a coarser step
        // would make it depend on when the dashboard happens to refresh.
        FIFTEEN_MINUTES("15m", Duration.ofMinutes(15), Duration.ofSeconds(5));

        private final String key;
        private final Duration range;
        private final Duration step;

        Window(String key, Duration range, Duration step) {
            this.key = key;
            this.range = range;
            this.step = step;
        }

        public static Optional<Window> of(String key) {
            return Arrays.stream(values()).filter(w -> w.key.equals(key)).findFirst();
        }
    }

    private final PrometheusClient prometheus;
    private final Clock clock;
    private final String grafanaUrl;
    private final Map<Window, Overview> cached = new EnumMap<>(Window.class);
    // When the answer was completed; the queries can take longer than the TTL when
    // Prometheus hangs.
    private final Map<Window, Instant> cachedAt = new EnumMap<>(Window.class);

    public SystemMetrics(PrometheusClient prometheus, Clock clock, String grafanaUrl) {
        this.prometheus = prometheus;
        this.clock = clock;
        this.grafanaUrl = grafanaUrl == null || grafanaUrl.isBlank() ? null : grafanaUrl;
    }

    /** Consumer lag of the processor; null if unknown. */
    public Long backlog() {
        return asLong(prometheus.query(BACKLOG));
    }

    /** Duplicates the processor has skipped since it started; null if unknown. */
    public Long duplicatesTotal() {
        return asLong(prometheus.query(DUPLICATES_TOTAL));
    }

    /**
     * Cached for a few seconds, so many open browsers cause few Prometheus queries.
     */
    public synchronized Overview overview(Window window) {
        Instant now = clock.instant();
        Overview last = cached.get(window);
        if (last != null && cachedAt.get(window).plus(CACHE_TTL).isAfter(now)) {
            return last;
        }
        // The two HTTP counters fall back to 0, so they answer whenever Prometheus
        // does; the rest are null if missing.
        Long rejected = asLong(prometheus.query(increaseOrZero(HTTP_EVENTS + "status=\"429\"}", window)));
        Long serverErrors = asLong(prometheus.query(increaseOrZero(HTTP_EVENTS + "status=~\"5..\"}", window)));
        boolean available = rejected != null || serverErrors != null;
        if (!available) {
            // Prometheus did not answer: skip the other queries instead of waiting for each
            // to fail.
            Overview unavailable = new Overview(now, false, null, null, null, null, null, null, null, null, null,
                    null, null, null, window.key, window.step.toSeconds(), List.of(), grafanaUrl);
            cached.put(window, unavailable);
            cachedAt.put(window, clock.instant());
            return unavailable;
        }
        List<SeriesPoint> series = series(now, window);
        Overview next = new Overview(now, available,
                asDouble(prometheus.query(INGEST_RATE)),
                asDouble(prometheus.query(PROCESS_RATE)),
                backlog(),
                asLong(prometheus.query(counted("ingestion_events_accepted_total", window))),
                asLong(prometheus.query(counted("processor_events_aggregated_total", window))),
                peak(series, SeriesPoint::ingestRate),
                peak(series, SeriesPoint::processRate),
                asLong(prometheus.query(counted("processor_events_duplicate_total", window))),
                rejected,
                serverErrors,
                asLong(prometheus.query(counted("processor_events_dead_lettered_total", window))),
                asLong(prometheus.query(counted("processor_delivery_failures_total", window))),
                window.key, window.step.toSeconds(), series, grafanaUrl);
        cached.put(window, next);
        cachedAt.put(window, clock.instant());
        return next;
    }

    /**
     * The highest plotted value, so a peak figure always matches its chart; null if
     * nothing was plotted.
     */
    private static Double peak(List<SeriesPoint> series, Function<SeriesPoint, Double> value) {
        return series.stream().map(value).filter(Objects::nonNull).max(Double::compare).orElse(null);
    }

    private List<SeriesPoint> series(Instant now, Window window) {
        Instant start = now.minus(window.range);
        Map<Long, Double> ingest = byTime(prometheus.queryRange(INGEST_RATE_SERIES, start, now, window.step));
        Map<Long, Double> processed = byTime(prometheus.queryRange(PROCESS_RATE_SERIES, start, now, window.step));
        Map<Long, Double> backlog = byTime(prometheus.queryRange(BACKLOG, start, now, window.step));
        TreeMap<Long, Boolean> times = new TreeMap<>();
        ingest.keySet().forEach(t -> times.put(t, true));
        processed.keySet().forEach(t -> times.put(t, true));
        backlog.keySet().forEach(t -> times.put(t, true));
        List<SeriesPoint> points = new ArrayList<>();
        times.keySet().forEach(t -> points.add(new SeriesPoint(t, ingest.get(t), processed.get(t), backlog.get(t))));
        return points;
    }

    private static Map<Long, Double> byTime(List<Point> points) {
        return points.stream().collect(Collectors.toMap(Point::epochSecond, Point::value, (a, b) -> b));
    }

    private static Long asLong(OptionalDouble value) {
        return value.isPresent() ? Math.round(value.getAsDouble()) : null;
    }

    private static Double asDouble(OptionalDouble value) {
        return value.isPresent() ? value.getAsDouble() : null;
    }

    /**
     * Platform-wide figures. Three kinds, never mixed: current values
     * ({@code ingestRate}, {@code processRate}: events
     * per second averaged over 30 s; {@code backlog}: the level now), totals and
     * peaks over the last {@code window}
     * ({@code *Total}, {@code *PeakRate}, the counters below them) and the
     * {@code series} of the charts. A null
     * figure is unknown (its metric is missing), not zero.
     *
     * <p>
     * {@code processedTotal} counts events added to the aggregates; duplicate
     * deliveries are in
     * {@code duplicatesSkipped}, so ingested minus processed is not the backlog.
     * Peaks are the highest 15 s average
     * in {@code series}.
     *
     * @param available   false if Prometheus did not answer
     * @param window      range of {@code series} and of every total, peak and
     *                    counter: {@code 5m} or {@code 15m}
     * @param stepSeconds spacing of {@code series}
     */
    public record Overview(
            Instant at,
            boolean available,
            Double ingestRate,
            Double processRate,
            Long backlog,
            Long ingestedTotal,
            Long processedTotal,
            Double ingestPeakRate,
            Double processPeakRate,
            Long duplicatesSkipped,
            Long rejectedRequests,
            Long serverErrors,
            Long deadLettered,
            Long deliveryFailures,
            String window,
            long stepSeconds,
            List<SeriesPoint> series,
            String grafanaUrl) {
    }

    /**
     * One step of the window; rates averaged over 15 s. A missing value is null.
     */
    public record SeriesPoint(long epochSecond, Double ingestRate, Double processRate, Double backlog) {
    }
}
