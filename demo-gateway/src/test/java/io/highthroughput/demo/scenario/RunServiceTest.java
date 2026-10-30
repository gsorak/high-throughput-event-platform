package io.highthroughput.demo.scenario;

import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.platform.DemoEvent;
import io.highthroughput.demo.platform.IngestResult;
import io.highthroughput.demo.platform.PlatformClient;
import io.highthroughput.demo.platform.StatsResult;
import io.highthroughput.demo.session.DemoSession;
import io.highthroughput.demo.session.SessionStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class RunServiceTest {

    private static final DemoProperties.Limits LIMITS = new DemoProperties.Limits(20, 5, Duration.ofMinutes(15), 1000,
            Duration.ofSeconds(30), 20_000, 50_000, Duration.ofSeconds(20));

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void shutdown() {
        executor.shutdownNow();
    }

    /**
     * A stalled request used to make the generator send the whole shortfall at once, beyond the plan's burst; the
     * ingestion API then refused those batches (the 429s of 1,000/s runs).
     */
    @Test
    void aStalledRequestDoesNotMakeTheGeneratorExceedTheBurst() throws Exception {
        // Tenant A's plan: 1,000 events/s, burst 2,000. The third request stalls for 3 s.
        BucketPlatform platform = new BucketPlatform(1000, 2000, 3, Duration.ofSeconds(3));
        RunService runs = new RunService(LIMITS, platform, executor, Clock.systemUTC());
        DemoSession session = new SessionStore(LIMITS, Clock.systemUTC()).create("10.0.0.1");

        Run run = runs.start(session, new RunService.RunRequest("page.view", 1000, 4));
        Instant deadline = Instant.now().plusSeconds(40);
        while (!run.isFinished() && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }

        assertThat(platform.stalled).isTrue();
        assertThat(run.getPhase()).isEqualTo(Run.Phase.COMPLETED);
        assertThat(run.getRejected()).isZero();
        assertThat(run.getRejectedByRate()).isZero();
        assertThat(run.getAccepted()).isEqualTo(4000);
        assertThat(run.getProcessed()).isEqualTo(4000);
    }

    /** A token bucket like the ingestion API's rate limit; everything accepted is "aggregated" at once. */
    private static final class BucketPlatform extends PlatformClient {

        private final double refillPerNano;
        private final double capacity;
        private final int stallOnRequest;
        private final Duration stall;
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicLong accepted = new AtomicLong();
        private double tokens;
        private long refilledAt = System.nanoTime();
        volatile boolean stalled;

        BucketPlatform(int eventsPerSecond, int burst, int stallOnRequest, Duration stall) {
            super(null, null, null, null);
            this.refillPerNano = eventsPerSecond / 1e9;
            this.capacity = burst;
            this.tokens = burst;
            this.stallOnRequest = stallOnRequest;
            this.stall = stall;
        }

        @Override
        public IngestResult ingest(String tenantId, List<DemoEvent> events) {
            if (requests.incrementAndGet() == stallOnRequest) {
                try {
                    Thread.sleep(stall);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                stalled = true;
            }
            synchronized (this) {
                long now = System.nanoTime();
                tokens = Math.min(capacity, tokens + (now - refilledAt) * refillPerNano);
                refilledAt = now;
                if (tokens < events.size()) {
                    return new IngestResult(429, 0, "rate", 1L, "Rate limit exceeded");
                }
                tokens -= events.size();
            }
            accepted.addAndGet(events.size());
            return new IngestResult(202, events.size(), null, null, null);
        }

        @Override
        public StatsResult stats(String tenantId, Instant from, Instant to, String type) {
            return new StatsResult(200, tenantId, accepted.get(), 0);
        }
    }
}
