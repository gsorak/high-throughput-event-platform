package io.highthroughput.demo.scenario;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@code src/test/promql/overview.test.yml} with promtool as part of the
 * normal offline test run. promtool is taken
 * from the PATH, else from the Prometheus image the stack uses (only if it is
 * already pulled: no download here). Without
 * either the test is skipped with a message; set {@code PROMTOOL_REQUIRED=true}
 * (CI does) to fail instead.
 */
class PromqlRegressionTest {

    private static final String IMAGE = "prom/prometheus:v3.15.1";
    private static final Path RULES_DIR = Path.of("src/test/promql").toAbsolutePath();

    @Test
    void theOverviewQueriesPassTheirPromtoolUnitTests() throws Exception {
        List<String> command = promtoolCommand();
        if (command == null) {
            String message = "promtool is not available (not on the PATH, and no local Docker image " + IMAGE
                    + "): the PromQL regression tests did NOT run. Install promtool or pull the image.";
            System.err.println("[PromqlRegressionTest] SKIPPED: " + message);
            assertThat(requiredByEnvironment()).as(message + " PROMTOOL_REQUIRED is set.").isFalse();
            assumeTrue(false, message);
            return;
        }

        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] raw = process.getInputStream().readAllBytes();
        boolean finished = process.waitFor(120, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        String output = new String(raw, StandardCharsets.UTF_8);

        assertThat(finished).as("promtool timed out").isTrue();
        assertThat(process.exitValue()).as("promtool output:%n%s", output).isZero();
        assertThat(output).contains("SUCCESS");
    }

    private static boolean requiredByEnvironment() {
        return "true".equalsIgnoreCase(System.getenv("PROMTOOL_REQUIRED"));
    }

    /**
     * The command to run the tests, or null if neither promtool nor the image is
     * available.
     */
    private static List<String> promtoolCommand() {
        if (succeeds(List.of("promtool", "--version"))) {
            return List.of("promtool", "test", "rules", RULES_DIR.resolve("overview.test.yml").toString());
        }
        if (succeeds(List.of("docker", "image", "inspect", IMAGE))) {
            return List.of("docker", "run", "--rm", "-v", RULES_DIR + ":/t:ro", "--entrypoint", "promtool", IMAGE,
                    "test", "rules", "/t/overview.test.yml");
        }
        return null;
    }

    private static boolean succeeds(List<String> command) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return false;
            }
            return process.exitValue() == 0;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
