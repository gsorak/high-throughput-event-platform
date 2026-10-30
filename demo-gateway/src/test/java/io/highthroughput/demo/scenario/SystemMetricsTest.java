package io.highthroughput.demo.scenario;

import io.highthroughput.demo.platform.PrometheusClient;
import io.highthroughput.demo.platform.PrometheusClient.Point;
import io.highthroughput.demo.scenario.SystemMetrics.Overview;
import io.highthroughput.demo.scenario.SystemMetrics.Window;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;

class SystemMetricsTest {

    private static final Instant NOW = Instant.parse("2030-03-17T10:00:00Z");

    /**
     * Answers from a map of exact query strings; anything else is missing. Records
     * every query.
     */
    private static class StubPrometheus extends PrometheusClient {
        final Map<String, Double> instant = new HashMap<>();
        final Map<String, List<Point>> ranges = new HashMap<>();
        final List<String> queries = new ArrayList<>();
        final List<String> rangeQueries = new ArrayList<>();
        final List<Duration> steps = new ArrayList<>();
        final List<Duration> spans = new ArrayList<>();

        StubPrometheus() {
            super(null, null, null);
        }

        @Override
        public OptionalDouble query(String promql) {
            queries.add(promql);
            Double value = instant.get(promql);
            return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
        }

        @Override
        public List<Point> queryRange(String promql, Instant start, Instant end, Duration step) {
            rangeQueries.add(promql);
            steps.add(step);
            spans.add(Duration.between(start, end));
            return ranges.getOrDefault(promql, List.of());
        }
    }

    private final StubPrometheus prometheus = new StubPrometheus();
    private final SystemMetrics metrics = new SystemMetrics(prometheus, Clock.fixed(NOW, ZoneOffset.UTC), null);

    private static String httpEvents(String status) {
        return "http_server_requests_seconds_count{application=\"ingestion-api\",uri=\"/v1/events\"," + status + "}";
    }

    @Test
    void everyHistoricalFigureUsesTheSelectedWindow() {
        for (Window window : Window.values()) {
            StubPrometheus stub = new StubPrometheus();
            stub.instant.put(SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), window), 0.0);
            new SystemMetrics(stub, Clock.fixed(NOW, ZoneOffset.UTC), null).overview(window);

            assertThat(stub.queries).contains(
                    SystemMetrics.counted("ingestion_events_accepted_total", window),
                    SystemMetrics.counted("processor_events_aggregated_total", window),
                    SystemMetrics.counted("processor_events_duplicate_total", window),
                    SystemMetrics.counted("processor_events_dead_lettered_total", window),
                    SystemMetrics.counted("processor_delivery_failures_total", window),
                    SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), window),
                    SystemMetrics.increaseOrZero(httpEvents("status=~\"5..\""), window));
        }
    }

    @Test
    void theFirstBurstOfRejectionsIsCountedEvenWhenItsSeriesDidNotExistBefore() {
        String selector = httpEvents("status=\"429\"");

        // increase() alone never sees the jump from no series to the first value; the
        // second term adds it for
        // series that had no sample at the start of the window. The no-series case
        // stays 0.
        assertThat(SystemMetrics.increaseOrZero(selector, Window.FIVE_MINUTES)).isEqualTo(
                "(sum(increase(" + selector + "[5m])) or vector(0)) + (sum(min_over_time(" + selector
                        + "[5m]) unless last_over_time(" + selector + "[10m] offset 5m)) or vector(0))");
        assertThat(SystemMetrics.increaseOrZero(selector, Window.FIFTEEN_MINUTES))
                .contains("[15m]) unless last_over_time(" + selector + "[10m] offset 15m))");
    }

    @Test
    void theCurrentRatesStayThirtySecondAveragesAndAreNotWindowDependent() {
        prometheus.instant.put(
                SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), Window.FIFTEEN_MINUTES), 0.0);
        metrics.overview(Window.FIFTEEN_MINUTES);

        assertThat(prometheus.queries).contains(
                "sum(rate(ingestion_events_accepted_total[30s]))",
                "sum(rate(processor_events_aggregated_total[30s]))");
    }

    @Test
    void theChartsSpanTheWindowAndFeedThePeaks() {
        // A short burst: 1,200/s for one step, idle before and after.
        List<Point> ingest = List.of(new Point(1000, 0), new Point(1005, 1200), new Point(1010, 0));
        List<Point> processed = List.of(new Point(1000, 0), new Point(1005, 300), new Point(1010, 450));
        prometheus.instant.put(increaseOrZero("429"), 0.0);
        prometheus.ranges.put(SystemMetrics.INGEST_RATE_SERIES, ingest);
        prometheus.ranges.put(SystemMetrics.PROCESS_RATE_SERIES, processed);

        Overview overview = metrics.overview(Window.FIVE_MINUTES);

        assertThat(overview.available()).isTrue();
        assertThat(overview.ingestPeakRate()).isEqualTo(1200.0);
        assertThat(overview.processPeakRate()).isEqualTo(450.0);
        assertThat(overview.series()).hasSize(3);
        assertThat(overview.stepSeconds()).isEqualTo(5);
        assertThat(prometheus.spans).containsOnly(Duration.ofMinutes(5));
        assertThat(prometheus.steps).containsOnly(Duration.ofSeconds(5));
        assertThat(prometheus.rangeQueries).containsExactlyInAnyOrder(
                SystemMetrics.INGEST_RATE_SERIES, SystemMetrics.PROCESS_RATE_SERIES, SystemMetrics.BACKLOG);

    }

    @Test
    void theFifteenMinuteWindowUsesAFifteenMinuteRangeAndTheSameFiveSecondStep() {
        prometheus.instant.put(SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), Window.FIFTEEN_MINUTES), 0.0);

        Overview overview = metrics.overview(Window.FIFTEEN_MINUTES);

        assertThat(overview.window()).isEqualTo("15m");
        // One step for every window: the peak is the highest plotted 15 s average, so
        // it must not depend on when
        // the dashboard samples.
        assertThat(overview.stepSeconds()).isEqualTo(5);
        assertThat(prometheus.spans).containsOnly(Duration.ofMinutes(15));
        assertThat(prometheus.steps).containsOnly(Duration.ofSeconds(5));
    }

    @Test
    void thePeakIsTheHighestPlottedPointOfTheChartInBothWindows() {
        List<Point> ingest = List.of(new Point(1000, 100), new Point(1005, 1000), new Point(1010, 500));
        for (Window window : Window.values()) {
            StubPrometheus stub = new StubPrometheus();
            stub.instant.put(SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), window), 0.0);
            stub.ranges.put(SystemMetrics.INGEST_RATE_SERIES, ingest);

            Overview overview = new SystemMetrics(stub, Clock.fixed(NOW, ZoneOffset.UTC), null).overview(window);

            assertThat(overview.ingestPeakRate())
                    .isEqualTo(overview.series().stream().mapToDouble(p -> p.ingestRate()).max().getAsDouble());
        }
    }

    @Test
    void aBurstThatEndedStillShowsTotalsAndPeakWhileTheCurrentRateIsZero() {
        prometheus.instant.put("sum(rate(ingestion_events_accepted_total[30s]))", 0.0);
        prometheus.instant.put(SystemMetrics.counted("ingestion_events_accepted_total", Window.FIVE_MINUTES), 40_000.0);
        prometheus.instant.put(SystemMetrics.counted("processor_events_aggregated_total", Window.FIVE_MINUTES),
                40_000.0);
        prometheus.instant.put(increaseOrZero("429"), 0.0);
        prometheus.ranges.put(SystemMetrics.INGEST_RATE_SERIES, List.of(new Point(1000, 950), new Point(1005, 0)));

        Overview overview = metrics.overview(Window.FIVE_MINUTES);

        assertThat(overview.ingestRate()).isZero();
        assertThat(overview.ingestedTotal()).isEqualTo(40_000);
        assertThat(overview.processedTotal()).isEqualTo(40_000);
        assertThat(overview.ingestPeakRate()).isEqualTo(950.0);
    }

    @Test
    void missingMetricsAreNullNotZeroExceptTheLazyHttpCounters() {
        prometheus.instant.put(increaseOrZero("429"), 0.0);

        Overview overview = metrics.overview(Window.FIVE_MINUTES);

        assertThat(overview.available()).isTrue();
        assertThat(overview.rejectedRequests()).isZero();
        assertThat(overview.serverErrors()).isNull();
        assertThat(overview.ingestRate()).isNull();
        assertThat(overview.backlog()).isNull();
        assertThat(overview.ingestedTotal()).isNull();
        assertThat(overview.processedTotal()).isNull();
        assertThat(overview.duplicatesSkipped()).isNull();
        assertThat(overview.deadLettered()).isNull();
        assertThat(overview.deliveryFailures()).isNull();
        assertThat(overview.ingestPeakRate()).isNull();
        assertThat(overview.processPeakRate()).isNull();
    }

    @Test
    void prometheusBeingDownMakesTheOverviewUnavailableWithoutCharts() {
        Overview overview = metrics.overview(Window.FIVE_MINUTES);

        assertThat(overview.available()).isFalse();
        assertThat(overview.series()).isEmpty();
        assertThat(prometheus.rangeQueries).isEmpty();
        // Only the two availability queries ran; the other figures were not asked for.
        assertThat(prometheus.queries).hasSize(2);
    }

    @Test
    void anAnswerIsCachedPerWindow() {
        prometheus.instant.put(increaseOrZero("429"), 0.0);

        metrics.overview(Window.FIVE_MINUTES);
        int afterFirst = prometheus.queries.size();
        metrics.overview(Window.FIVE_MINUTES);
        assertThat(prometheus.queries).hasSize(afterFirst);

        metrics.overview(Window.FIFTEEN_MINUTES);
        assertThat(prometheus.queries.size()).isGreaterThan(afterFirst);
    }

    @Test
    void aSlowAnswerIsCachedFromWhenItCompletedNotFromWhenItStarted() {
        // Queries take 10 s (longer than the 3 s TTL), as when Prometheus hangs until
        // its timeout.
        Instant[] now = { NOW };
        Clock clock = new Clock() {
            @Override
            public java.time.ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return now[0];
            }
        };
        StubPrometheus slow = new StubPrometheus() {
            @Override
            public OptionalDouble query(String promql) {
                now[0] = now[0].plusSeconds(5);
                return super.query(promql);
            }
        };
        SystemMetrics slowMetrics = new SystemMetrics(slow, clock, null);

        Overview first = slowMetrics.overview(Window.FIVE_MINUTES);
        assertThat(first.available()).isFalse();
        assertThat(slow.queries).hasSize(2);

        now[0] = now[0].plusSeconds(2);
        assertThat(slowMetrics.overview(Window.FIVE_MINUTES)).isSameAs(first);
        assertThat(slow.queries).hasSize(2);

        now[0] = now[0].plusSeconds(2);
        slowMetrics.overview(Window.FIVE_MINUTES);
        assertThat(slow.queries.size()).isGreaterThan(2);
    }

    @Test
    void theBacklogCountsUncommittedPartitionsAndNeverGoesNegative() {
        String lag = "kafka_consumergroup_lag{consumergroup=\"event-processor\",topic=\"events.v1\"}";
        String committed = "kafka_consumergroup_current_offset{consumergroup=\"event-processor\",topic=\"events.v1\"}";
        assertThat(SystemMetrics.BACKLOG)
                .contains("clamp_min(" + lag + ", 0)")
                .contains("kafka_topic_partition_current_offset{topic=\"events.v1\"}")
                .contains("kafka_topic_partition_oldest_offset{topic=\"events.v1\"}")
                // Uncommitted partitions are selected by their committed offset, not by a lag
                // of -1.
                .contains("(" + committed + " == -1)")
                .doesNotContain(lag + " == -1");
    }

    /** The promtool unit tests must evaluate the very queries the gateway sends. */
    @Test
    void thePromtoolTestsUseTheGatewaysQueries() throws IOException {
        String yaml = Files.readString(Path.of("src/test/promql/overview.test.yml"));

        assertThat(yaml).contains(
                SystemMetrics.BACKLOG,
                SystemMetrics.INGEST_RATE,
                SystemMetrics.INGEST_RATE_SERIES,
                SystemMetrics.counted("ingestion_events_accepted_total", Window.FIVE_MINUTES),
                SystemMetrics.counted("processor_events_duplicate_total", Window.FIVE_MINUTES),
                SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), Window.FIVE_MINUTES),
                SystemMetrics.increaseOrZero(httpEvents("status=\"429\""), Window.FIFTEEN_MINUTES),
                SystemMetrics.increaseOrZero(httpEvents("status=~\"5..\""), Window.FIVE_MINUTES));
    }

    private static String increaseOrZero(String status) {
        return SystemMetrics.increaseOrZero(httpEvents("status=\"" + status + "\""), Window.FIVE_MINUTES);
    }
}
