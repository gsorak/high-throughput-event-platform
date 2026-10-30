package io.highthroughput.demo.platform;

import io.highthroughput.demo.DemoProperties;
import io.highthroughput.demo.token.TokenIssuer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Calls the platform's public APIs exactly as an external client would: {@code POST /v1/events} on ingestion-api and
 * {@code GET /v1/stats} on event-processor, each with a bearer token. A transport failure is reported as status 0.
 */
public class PlatformClient {

    private static final Logger log = LoggerFactory.getLogger(PlatformClient.class);

    private final DemoProperties properties;
    private final TokenIssuer tokens;
    private final JsonMapper json;
    private final HttpClient http;

    public PlatformClient(DemoProperties properties, TokenIssuer tokens, JsonMapper json, HttpClient http) {
        this.properties = properties;
        this.tokens = tokens;
        this.json = json;
        this.http = http;
    }

    public IngestResult ingest(String tenantId, List<DemoEvent> events) {
        return ingestWithToken(tokens.issue(tenantId), events);
    }

    public IngestResult ingestWithToken(String token, List<DemoEvent> events) {
        HttpRequest request = HttpRequest.newBuilder(properties.ingestionUrl().resolve("/v1/events"))
                .timeout(properties.backendTimeout())
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(Map.of("events", events))))
                .build();
        try {
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            JsonNode body = parse(response.body());
            int status = response.statusCode();
            Long retryAfter = response.headers().firstValueAsLong("Retry-After").isPresent()
                    ? response.headers().firstValueAsLong("Retry-After").getAsLong() : null;
            return new IngestResult(status,
                    status == 202 ? body.path("accepted").asInt(0) : 0,
                    status == 429 ? body.path("limit").asString(null) : null,
                    retryAfter,
                    status == 202 ? null : body.path("detail").asString(null));
        } catch (IOException e) {
            log.warn("POST /v1/events failed: {}", e.toString());
            return IngestResult.unreachable(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IngestResult.unreachable(e);
        }
    }

    public StatsResult stats(String tenantId, Instant from, Instant to, String type) {
        return statsWithToken(tokens.issue(tenantId), from, to, type);
    }

    public StatsResult statsWithToken(String token, Instant from, Instant to, String type) {
        String query = "from=" + from + "&to=" + to
                + (type == null ? "" : "&type=" + URLEncoder.encode(type, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(properties.processorUrl().resolve(URI.create("/v1/stats?" + query)))
                .timeout(properties.backendTimeout())
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                return new StatsResult(response.statusCode(), null, 0, 0);
            }
            JsonNode body = parse(response.body());
            return new StatsResult(200, body.path("tenantId").asString(null),
                    body.path("totalCount").asLong(0), body.path("totalValueSum").asDouble(0));
        } catch (IOException e) {
            log.warn("GET /v1/stats failed: {}", e.toString());
            return new StatsResult(0, null, 0, 0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StatsResult(0, null, 0, 0);
        }
    }

    private JsonNode parse(byte[] body) {
        if (body.length == 0) {
            return json.missingNode();
        }
        try {
            return json.readTree(body);
        } catch (RuntimeException e) {
            return json.missingNode();
        }
    }
}
