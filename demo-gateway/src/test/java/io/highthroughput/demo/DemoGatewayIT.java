package io.highthroughput.demo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** The gateway end to end over HTTP, against {@link FakePlatform}. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DemoGatewayIT {

    private static final FakePlatform PLATFORM;

    static {
        try {
            PLATFORM = new FakePlatform();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("demo.ingestion-url", PLATFORM::url);
        registry.add("demo.processor-url", PLATFORM::url);
        registry.add("demo.prometheus-url", PLATFORM::url);
        registry.add("demo.ui-location", () -> "");
        registry.add("demo.limits.max-sessions", () -> "50");
        registry.add("demo.limits.max-sessions-per-client", () -> "50");
        registry.add("demo.limits.drain-timeout", () -> "15s");
        registry.add("demo.token.private-key-location", PLATFORM::privateKeyLocation);
        registry.add("demo.token.issuer", () -> FakePlatform.ISSUER);
        registry.add("demo.token.audience", () -> FakePlatform.AUDIENCE);
    }

    @AfterAll
    static void stopPlatform() throws Exception {
        PLATFORM.close();
    }

    @Value("${local.server.port}")
    int port;

    private final JsonMapper json = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void theSessionCookieIsHardenedAndARefreshKeepsTheSession() throws Exception {
        Visitor visitor = new Visitor();
        Response created = visitor.send("POST", "/demo/session", null);

        assertThat(created.status()).isEqualTo(201);
        String cookie = created.headers().firstValue("Set-Cookie").orElseThrow();
        assertThat(cookie).startsWith("demo_session=").contains("Path=/demo", "HttpOnly", "SameSite=Strict")
                .containsPattern("Max-Age=(899|900);").doesNotContain("Secure");
        assertThat(created.body().path("secondsRemaining").asLong()).isBetween(890L, 900L);
        List<String> tenants = tenants(created.body());
        assertThat(tenants.get(0)).matches("demo-[0-9a-f]{16}-a");

        Response refreshed = visitor.send("GET", "/demo/session", null);
        assertThat(refreshed.status()).isEqualTo(200);
        assertThat(tenants(refreshed.body())).isEqualTo(tenants);
        Response again = visitor.send("POST", "/demo/session", null);
        assertThat(again.status()).isEqualTo(200);
        assertThat(tenants(again.body())).isEqualTo(tenants);

        Response ended = visitor.send("DELETE", "/demo/session", null);
        assertThat(ended.status()).isEqualTo(204);
        assertThat(ended.headers().firstValue("Set-Cookie").orElseThrow()).contains("Max-Age=0");
        assertThat(visitor.send("GET", "/demo/session", null).status()).isEqualTo(401);
    }

    @Test
    void requestsNeedASessionAndStateChangesNeedTheClientHeader() throws Exception {
        Response anonymous = new Visitor().send("GET", "/demo/session", null);
        assertThat(anonymous.status()).isEqualTo(401);
        assertThat(anonymous.headers().firstValue("Content-Type").orElseThrow()).contains("application/problem+json");
        assertThat(anonymous.headers().firstValue("Content-Security-Policy").orElseThrow())
                .contains("default-src 'self'", "frame-ancestors 'none'");
        assertThat(anonymous.headers().firstValue("Cache-Control").orElseThrow()).isEqualTo("no-store");
        assertThat(new Visitor().send("GET", "/demo/overview", null).status()).isEqualTo(401);

        HttpResponse<String> forged = http.send(HttpRequest.newBuilder(uri("/demo/session"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(forged.statusCode()).isEqualTo(403);

        Visitor guessing = new Visitor();
        guessing.cookie = "demo_session=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assertThat(guessing.send("GET", "/demo/session", null).status()).isEqualTo(401);
    }

    @Test
    void visitorsGetSeparateTenantsAndNeverSeeTokens() throws Exception {
        Visitor first = Visitor.started(this);
        Visitor second = Visitor.started(this);

        Response firstView = first.send("GET", "/demo/session", null);
        Response secondView = second.send("GET", "/demo/session", null);

        assertThat(tenants(firstView.body())).doesNotContainAnyElementsOf(tenants(secondView.body()));
        first.send("POST", "/demo/isolation/check", null);
        String body = first.send("GET", "/demo/session", null).raw();
        assertThat(body).doesNotContain("eyJ").doesNotContain("PRIVATE KEY");
    }

    @Test
    void aRunCompletesOnlyOnceEveryAcceptedEventIsAggregated() throws Exception {
        Visitor visitor = Visitor.started(this);
        visitor.await(view -> !view.path("systemRun").path("busy").asBoolean());

        Response started = visitor.send("POST", "/demo/runs",
                "{\"type\":\"page.view\",\"eventsPerSecond\":200,\"durationSeconds\":2}");
        assertThat(started.status()).isEqualTo(200);
        assertThat(started.body().path("run").path("phase").asString()).isIn("SENDING", "PROCESSING");

        JsonNode run = visitor.await(view -> view.path("run").path("phase").asString().equals("COMPLETED"))
                .path("run");
        assertThat(run.path("submitted").asLong()).isEqualTo(400);
        assertThat(run.path("accepted").asLong()).isEqualTo(400);
        assertThat(run.path("processed").asLong()).isEqualTo(400);
        assertThat(run.path("pending").asLong()).isZero();
        assertThat(run.path("rejected").asLong()).isZero();
        assertThat(run.path("rejectedByRate").asLong()).isZero();
        assertThat(run.path("timeline").size()).isPositive();
        assertThat(visitor.send("GET", "/demo/session", null).body().path("budget").path("used").asLong())
                .isEqualTo(400);
    }

    @Test
    void onlyOneRunExecutesAcrossAllVisitors() throws Exception {
        Visitor first = Visitor.started(this);
        Visitor second = Visitor.started(this);
        first.await(view -> !view.path("systemRun").path("busy").asBoolean());
        String request = "{\"type\":\"api.call\",\"eventsPerSecond\":100,\"durationSeconds\":3}";

        assertThat(first.send("POST", "/demo/runs", request).status()).isEqualTo(200);
        Response other = second.send("POST", "/demo/runs", request);
        assertThat(other.status()).isEqualTo(409);
        assertThat(other.headers().firstValue("Retry-After")).isPresent();
        assertThat(first.send("POST", "/demo/runs", request).status()).isEqualTo(409);
        assertThat(second.send("GET", "/demo/session", null).body().path("budget").path("used").asLong()).isZero();
        assertThat(second.send("GET", "/demo/session", null).body().path("systemRun").path("busy").asBoolean())
                .isTrue();

        assertThat(first.send("POST", "/demo/runs/current/stop", null).status()).isEqualTo(200);
        JsonNode run = first.await(view -> view.path("run").path("phase").asString().equals("COMPLETED")).path("run");
        assertThat(run.path("submitted").asLong()).isLessThan(300);
        assertThat(first.send("GET", "/demo/session", null).body().path("budget").path("used").asLong())
                .isEqualTo(run.path("submitted").asLong());
        second.await(view -> !view.path("systemRun").path("busy").asBoolean());
        assertThat(second.send("POST", "/demo/runs", request).status()).isEqualTo(200);
        second.await(view -> view.path("run").path("phase").asString().equals("COMPLETED"));
    }

    @Test
    void runRequestsOutsideTheLimitsAreRejected() throws Exception {
        Visitor visitor = Visitor.started(this);

        for (String request : List.of(
                "{\"type\":\"page.view\",\"eventsPerSecond\":1001,\"durationSeconds\":10}",
                "{\"type\":\"page.view\",\"eventsPerSecond\":1000,\"durationSeconds\":30}",
                "{\"type\":\"page.view\",\"eventsPerSecond\":100,\"durationSeconds\":31}",
                "{\"type\":\"page.view\",\"eventsPerSecond\":0,\"durationSeconds\":10}",
                "{\"type\":\"drop.tables\",\"eventsPerSecond\":10,\"durationSeconds\":1}",
                "not json")) {
            assertThat(visitor.send("POST", "/demo/runs", request).status()).as(request).isEqualTo(400);
        }
        assertThat(visitor.send("GET", "/demo/session", null).body().path("run").isNull()).isTrue();
    }

    @Test
    void aReplayedBatchIsProcessedButNotCountedTwice() throws Exception {
        Visitor visitor = Visitor.started(this);

        assertThat(visitor.send("POST", "/demo/dedup/replay", null).status()).isEqualTo(409);
        assertThat(visitor.send("POST", "/demo/dedup/send", null).status()).isEqualTo(200);
        JsonNode delivered = visitor.await(view -> view.path("dedup").path("phase").asString().equals("DELIVERED"))
                .path("dedup");
        assertThat(delivered.path("countAfterFirst").asLong())
                .isEqualTo(delivered.path("countBefore").asLong() + 100);

        assertThat(visitor.send("POST", "/demo/dedup/replay", null).status()).isEqualTo(200);
        JsonNode verified = visitor.await(view -> view.path("dedup").path("phase").asString().equals("VERIFIED")
                && !view.path("dedup").path("duplicatesSkipped").isNull()).path("dedup");
        assertThat(verified.path("replayStatus").asInt()).isEqualTo(202);
        assertThat(verified.path("countAfterReplay").asLong()).isEqualTo(delivered.path("countAfterFirst").asLong());
        assertThat(verified.path("duplicatesSkipped").asLong()).isGreaterThanOrEqualTo(100);
        assertThat(verified.path("sampleEventIds").size()).isEqualTo(3);
    }

    @Test
    void isolationChecksUseRealTokenVerification() throws Exception {
        Visitor visitor = Visitor.started(this);
        List<String> tenants = tenants(visitor.send("GET", "/demo/session", null).body());

        JsonNode report = visitor.send("POST", "/demo/isolation/check", null).body().path("isolation");

        assertThat(report.path("allPassed").asBoolean()).isTrue();
        List<Integer> statuses = new ArrayList<>();
        report.path("checks").forEach(check -> statuses.add(check.path("actualStatus").asInt()));
        assertThat(statuses).containsExactly(200, 200, 401, 401, 401);
        assertThat(report.path("checks").get(0).path("answeredForTenant").asString()).isEqualTo(tenants.get(0));
        assertThat(report.path("checks").get(1).path("answeredForTenant").asString()).isEqualTo(tenants.get(1));
        assertThat(report.path("checks").get(2).path("claims").path("tenant_id").asString()).isEqualTo(tenants.get(1));
    }

    @Test
    void rateAndQuotaRejectionsComeFromTheIngestionApi() throws Exception {
        Visitor visitor = Visitor.started(this);

        JsonNode within = visitor.send("POST", "/demo/limits/within", null).body().path("rateLimits");
        assertThat(within.path("attempts").get(0).path("status").asInt()).isEqualTo(202);

        JsonNode burst = visitor.send("POST", "/demo/limits/burst", null).body().path("rateLimits");
        JsonNode rejected = burst.path("attempts").get(0);
        assertThat(rejected.path("status").asInt()).isEqualTo(429);
        assertThat(rejected.path("limit").asString()).isEqualTo("rate");
        assertThat(rejected.path("retryAfterSeconds").asLong()).isPositive();

        JsonNode quota = visitor.send("POST", "/demo/limits/quota", null).body().path("rateLimits");
        JsonNode last = quota.path("attempts").get(0);
        assertThat(last.path("limit").asString()).isEqualTo("quota");
        assertThat(last.path("events").asInt()).isEqualTo(1);
        assertThat(quota.path("acceptedEvents").asLong()).isEqualTo(1000);
        assertThat(quota.path("quotaRemaining").asLong()).isZero();
        assertThat(quota.path("quotaExhausted").asBoolean()).isTrue();
        assertThat(Instant.parse(quota.path("quotaResetsAt").asString())).isAfter(Instant.now());

        JsonNode afterwards = visitor.send("POST", "/demo/limits/within", null).body().path("rateLimits");
        assertThat(afterwards.path("attempts").get(0).path("limit").asString()).isEqualTo("quota");
        assertThat(visitor.send("POST", "/demo/limits/unknown", null).status()).isEqualTo(404);
    }

    @Test
    void aBatchLargerThanTheRemainingQuotaIsRefusedWhileQuotaRemains() throws Exception {
        Visitor visitor = Visitor.started(this);

        visitor.send("POST", "/demo/limits/within", null);
        // Let the bucket refill enough for one 500-event batch.
        Thread.sleep(1100);
        JsonNode burst = visitor.send("POST", "/demo/limits/burst", null).body().path("rateLimits");
        assertThat(burst.path("acceptedEvents").asLong()).isEqualTo(550);

        JsonNode again = visitor.send("POST", "/demo/limits/burst", null).body().path("rateLimits");
        JsonNode refused = again.path("attempts").get(0);
        assertThat(refused.path("status").asInt()).isEqualTo(429);
        assertThat(refused.path("limit").asString()).isEqualTo("quota");
        assertThat(refused.path("events").asInt()).isEqualTo(500);
        assertThat(refused.path("quotaRemainingBefore").asLong()).isEqualTo(450);
        assertThat(again.path("acceptedEvents").asLong()).isEqualTo(550);
        assertThat(again.path("quotaRemaining").asLong()).isEqualTo(450);
        assertThat(again.path("quotaExhausted").asBoolean()).isFalse();
    }

    @Test
    void theOverviewIsAvailableToSessions() throws Exception {
        Visitor visitor = Visitor.started(this);

        Response overview = visitor.send("GET", "/demo/overview", null);

        assertThat(overview.status()).isEqualTo(200);
        assertThat(overview.body().path("available").asBoolean()).isTrue();
        assertThat(overview.body().has("backlog")).isTrue();
        assertThat(overview.body().path("window").asString()).isEqualTo("5m");

        Response shorter = visitor.send("GET", "/demo/overview?window=15m", null);
        assertThat(shorter.status()).isEqualTo(200);
        assertThat(shorter.body().path("window").asString()).isEqualTo("15m");
        assertThat(shorter.body().path("stepSeconds").asLong()).isEqualTo(5);
        assertThat(visitor.send("GET", "/demo/overview?window=1d", null).status()).isEqualTo(400);
    }

    private static List<String> tenants(JsonNode view) {
        List<String> ids = new ArrayList<>();
        view.path("tenants").forEach(tenant -> ids.add(tenant.path("id").asString()));
        return ids;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private record Response(int status, java.net.http.HttpHeaders headers, String raw, JsonNode body) {
    }

    /** A browser: keeps the session cookie and sends the client header. */
    private final class Visitor {

        String cookie;

        static Visitor started(DemoGatewayIT test) throws Exception {
            Visitor visitor = test.new Visitor();
            assertThat(visitor.send("POST", "/demo/session", null).status()).isEqualTo(201);
            return visitor;
        }

        Response send(String method, String path, String body) throws Exception {
            HttpRequest.Builder request = HttpRequest.newBuilder(uri(path))
                    .timeout(Duration.ofSeconds(60))
                    .header("X-Demo-Client", "1")
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                            : HttpRequest.BodyPublishers.ofString(body));
            if (cookie != null) {
                request.header("Cookie", cookie);
            }
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            response.headers().firstValue("Set-Cookie").ifPresent(set -> cookie = set.substring(0, set.indexOf(';')));
            String raw = response.body();
            return new Response(response.statusCode(), response.headers(), raw,
                    raw.isEmpty() ? json.missingNode() : json.readTree(raw));
        }

        JsonNode await(Predicate<JsonNode> done) throws Exception {
            Instant deadline = Instant.now().plusSeconds(30);
            while (true) {
                JsonNode view = send("GET", "/demo/session", null).body();
                if (done.test(view)) {
                    return view;
                }
                assertThat(Instant.now()).as("waiting for %s", view).isBefore(deadline);
                Thread.sleep(200);
            }
        }
    }
}
