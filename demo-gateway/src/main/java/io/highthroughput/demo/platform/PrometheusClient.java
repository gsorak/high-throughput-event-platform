package io.highthroughput.demo.platform;

import io.highthroughput.demo.DemoProperties;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Reads platform metrics from Prometheus. Callers pass constant queries only; nothing from the browser reaches a
 * query, and Prometheus itself is never exposed.
 */
public class PrometheusClient {

    private static final Logger log = LoggerFactory.getLogger(PrometheusClient.class);

    private final DemoProperties properties;
    private final JsonMapper json;
    private final HttpClient http;

    public PrometheusClient(DemoProperties properties, JsonMapper json, HttpClient http) {
        this.properties = properties;
        this.json = json;
        this.http = http;
    }

    /** Value of an instant query returning one series; empty if there is no series or Prometheus is unavailable. */
    public OptionalDouble query(String promql) {
        JsonNode result = get("/api/v1/query?query=" + encode(promql));
        if (result == null || result.isEmpty()) {
            return OptionalDouble.empty();
        }
        return parse(result.get(0).path("value").path(1));
    }

    /** Points of a range query returning one series; empty if there is no data. */
    public List<Point> queryRange(String promql, Instant start, Instant end, Duration step) {
        JsonNode result = get("/api/v1/query_range?query=" + encode(promql) + "&start=" + start.getEpochSecond()
                + "&end=" + end.getEpochSecond() + "&step=" + step.toSeconds());
        List<Point> points = new ArrayList<>();
        if (result != null && !result.isEmpty()) {
            for (JsonNode value : result.get(0).path("values")) {
                OptionalDouble v = parse(value.path(1));
                if (v.isPresent()) {
                    points.add(new Point(value.path(0).asLong(0), v.getAsDouble()));
                }
            }
        }
        return points;
    }

    private JsonNode get(String pathAndQuery) {
        HttpRequest request = HttpRequest.newBuilder(properties.prometheusUrl().resolve(URI.create(pathAndQuery)))
                .timeout(properties.backendTimeout())
                .GET()
                .build();
        try {
            HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() != 200) {
                log.warn("Prometheus query failed with {}", response.statusCode());
                return null;
            }
            return json.readTree(response.body()).path("data").path("result");
        } catch (IOException | RuntimeException e) {
            log.warn("Prometheus unavailable: {}", e.toString());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static OptionalDouble parse(JsonNode value) {
        try {
            double v = Double.parseDouble(value.asString(""));
            return Double.isFinite(v) ? OptionalDouble.of(v) : OptionalDouble.empty();
        } catch (NumberFormatException e) {
            return OptionalDouble.empty();
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /** @param epochSecond time of the sample */
    public record Point(long epochSecond, double value) {
    }
}
