package io.highthroughput.demo.scenario;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The consumer-lag expression exists in the gateway, two shell scripts, the
 * Grafana
 * dashboard and the promtool tests.
 * They must stay the same, or the UI, the scripts and the dashboard would
 * disagree about the backlog.
 */
class LagQueryConsistencyTest {

    private static final Path ROOT = Path.of("..").toAbsolutePath().normalize();
    private static final String LAG = "kafka_consumergroup_lag{consumergroup=\"event-processor\",topic=\"events.v1\"}";
    private static final String COMMITTED = "kafka_consumergroup_current_offset{consumergroup=\"event-processor\",topic=\"events.v1\"}";
    private static final String UNCOMMITTED_LOG = "((kafka_topic_partition_current_offset{topic=\"events.v1\"}"
            + " - kafka_topic_partition_oldest_offset{topic=\"events.v1\"}) and on(topic, partition) (" + COMMITTED
            + " == -1))";
    /**
     * Per partition: the lag of a committed partition, the events held by an
     * uncommitted one.
     */
    private static final String BY_PARTITION = LAG + " >= 0 or on(topic, partition) " + UNCOMMITTED_LOG;
    private static final String MAX_BY_PARTITION = "max(" + BY_PARTITION + ")";

    private static String read(String path) throws IOException {
        return Files.readString(ROOT.resolve(path));
    }

    private static String jsonEscaped(String promql) {
        return promql.replace("\"", "\\\"");
    }

    @Test
    void theScriptsUseTheGatewaysBacklogExpression() throws IOException {
        assertThat(read("load-tests/run.sh")).contains(SystemMetrics.BACKLOG);
        assertThat(read("scripts/resilience/lib.sh")).contains(SystemMetrics.BACKLOG).contains(MAX_BY_PARTITION);
    }

    @Test
    void theGrafanaDashboardUsesTheSameExpressions() throws IOException {
        String dashboard = read("infra/local/grafana/dashboards/overload-and-recovery.json");

        assertThat(dashboard).contains(jsonEscaped(SystemMetrics.BACKLOG)).contains(jsonEscaped(BY_PARTITION));
    }

    @Test
    void thePromtoolTestsCoverThePerPartitionExpressions() throws IOException {
        assertThat(Files.readString(Path.of("src/test/promql/overview.test.yml")))
                .contains(BY_PARTITION)
                .contains(MAX_BY_PARTITION);
    }

    @Test
    void noScriptOrDashboardSumsTheRawLag() throws IOException {
        // A raw sum would count the -1 of every partition without a commit.
        for (String file : new String[] { "load-tests/run.sh", "scripts/resilience/lib.sh",
                "infra/local/grafana/dashboards/overload-and-recovery.json" }) {
            assertThat(read(file)).as(file).doesNotContain("sum(kafka_consumergroup_lag");
        }
    }
}
