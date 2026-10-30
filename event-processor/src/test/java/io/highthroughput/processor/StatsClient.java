package io.highthroughput.processor;

import org.springframework.boot.test.web.server.LocalServerPort;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

/**
 * Calls {@code GET /v1/stats} over HTTP with a token for the tenant and returns
 * the parsed JSON body.
 */
final class StatsClient {

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();
    private final int port;

    /** @param port from {@link LocalServerPort} */
    StatsClient(int port) {
        this.port = port;
    }

    JsonNode get(String tenant, Instant from, Instant to, String type) {
        String query = "from=" + from + "&to=" + to + (type == null ? "" : "&type=" + type);
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/stats?" + query))
                .header("Authorization", TestJwt.bearer(tenant))
                .GET()
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new AssertionError("GET /v1/stats returned " + response.statusCode() + ": " + response.body());
            }
            return json.readTree(response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    long totalCount(String tenant, Instant from, Instant to) {
        return get(tenant, from, to, null).get("totalCount").asLong();
    }
}
