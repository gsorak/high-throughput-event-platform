package io.highthroughput.demo.session;

import io.highthroughput.demo.DemoException;
import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.scenario.LimitAttempt;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SessionStoreTest {

    private static final DemoProperties.Limits LIMITS = new DemoProperties.Limits(20, 5, Duration.ofMinutes(15), 1000,
            Duration.ofSeconds(30), 20_000, 50_000, Duration.ofSeconds(120));

    private final MutableClock clock = new MutableClock(Instant.parse("2026-05-01T10:00:00Z"));
    private final SessionStore store = new SessionStore(LIMITS, clock);

    @Test
    void concurrentCreationNeverExceedsTheGlobalCap() throws Exception {
        AtomicInteger atCapacity = new AtomicInteger();
        List<DemoSession> created = runConcurrently(60, i -> {
            try {
                return store.create("10.0.0." + i);
            } catch (DemoException e) {
                assertThat(e.status()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                assertThat(e.retryAfterSeconds()).isPositive();
                atCapacity.incrementAndGet();
                return null;
            }
        });

        assertThat(created).hasSize(20);
        assertThat(atCapacity).hasValue(40);
        assertThat(store.activeSessions()).isEqualTo(20);
    }

    @Test
    void oneClientCannotTakeEverySession() {
        for (int i = 0; i < 5; i++) {
            store.create("10.0.0.1");
        }

        assertThatThrownBy(() -> store.create("10.0.0.1"))
                .isInstanceOfSatisfying(DemoException.class,
                        e -> assertThat(e.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(store.create("10.0.0.2")).isNotNull();
    }

    @Test
    void sessionsHaveUnguessableIdsAndTheirOwnTenants() {
        DemoSession first = store.create("10.0.0.1");
        DemoSession second = store.create("10.0.0.1");

        assertThat(first.id()).hasSize(43).isNotEqualTo(second.id());
        assertThat(first.tenantA()).matches("demo-[0-9a-f]{16}-a");
        assertThat(first.tenantB()).isEqualTo(first.tenantA().replaceFirst("-a$", "-b"));
        assertThat(second.tenantA()).isNotEqualTo(first.tenantA());
        assertThat(store.find(first.id())).containsSame(first);
        assertThat(store.find("unknown")).isEmpty();
        assertThat(store.find(null)).isEmpty();
    }

    @Test
    void expiredSessionsAreRejectedAndRemoved() {
        DemoSession session = store.create("10.0.0.1");

        clock.advance(Duration.ofMinutes(10));
        DemoSession later = store.create("10.0.0.2");
        clock.advance(Duration.ofMinutes(5));

        assertThat(store.find(session.id())).isEmpty();
        assertThat(session.isClosed()).isTrue();
        assertThat(session.tryReserve(1)).isFalse();
        assertThat(store.find(later.id())).isPresent();

        clock.advance(Duration.ofMinutes(10));
        store.removeExpired();
        assertThat(store.activeSessions()).isZero();
        assertThat(later.isClosed()).isTrue();
    }

    @Test
    void anExpiredSessionFreesItsSlot() {
        for (int i = 0; i < 20; i++) {
            store.create("10.0.0." + i);
        }
        assertThatThrownBy(() -> store.create("10.0.1.1")).isInstanceOf(DemoException.class);

        clock.advance(Duration.ofMinutes(15));

        assertThat(store.create("10.0.1.1")).isNotNull();
        assertThat(store.activeSessions()).isEqualTo(1);
    }

    @Test
    void theEventBudgetIsReservedAtomically() throws Exception {
        DemoSession session = store.create("10.0.0.1");

        List<Boolean> results = runConcurrently(100, i -> session.tryReserve(1000));

        assertThat(results.stream().filter(Boolean::booleanValue)).hasSize(50);
        assertThat(session.eventsUsed()).isEqualTo(50_000);
        session.refund(1000);
        assertThat(session.tryReserve(1000)).isTrue();
        assertThat(session.tryReserve(1)).isFalse();
    }

    @Test
    void aScenarioRunsOnceAtATimePerSession() {
        DemoSession session = store.create("10.0.0.1");

        assertThat(session.tryBegin("dedup")).isTrue();
        assertThat(session.tryBegin("dedup")).isFalse();
        assertThat(session.tryBegin("limits")).isTrue();
        session.end("dedup");
        assertThat(session.tryBegin("dedup")).isTrue();
    }

    @Test
    void tenantBQuotaUseCountsAcceptedEventsOfTheCurrentUtcDay() {
        DemoSession session = store.create("10.0.0.1");
        Instant beforeMidnight = Instant.parse("2026-05-01T23:59:00Z");

        session.addLimitAttempt(attempt(beforeMidnight, 202, 500));
        session.addLimitAttempt(attempt(beforeMidnight, 429, 0));

        assertThat(session.limitQuotaUsed(beforeMidnight.plusSeconds(30))).isEqualTo(500);
        // The ingestion API refills the daily quota at 00:00 UTC.
        assertThat(session.limitQuotaUsed(beforeMidnight.plusSeconds(90))).isZero();

        session.addLimitAttempt(attempt(beforeMidnight.plusSeconds(120), 202, 50));
        assertThat(session.limitQuotaUsed(beforeMidnight.plusSeconds(150))).isEqualTo(50);
    }

    private static LimitAttempt attempt(Instant at, int status, int accepted) {
        return new LimitAttempt(at, "within", 500, status, accepted, status == 429 ? "quota" : null, null, null, 0);
    }

    private static <T> List<T> runConcurrently(int tasks, IndexedTask<T> task) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(16)) {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < tasks; i++) {
                int index = i;
                Callable<T> callable = () -> {
                    start.await();
                    return task.run(index);
                };
                futures.add(executor.submit(callable));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                T result = future.get();
                if (result != null) {
                    results.add(result);
                }
            }
            return results;
        }
    }

    @FunctionalInterface
    private interface IndexedTask<T> {
        T run(int index);
    }

    private static final class MutableClock extends Clock {

        private volatile Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
