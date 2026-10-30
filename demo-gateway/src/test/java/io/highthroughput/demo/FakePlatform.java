package io.highthroughput.demo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Stand-in for ingestion-api, event-processor and Prometheus with the behaviour the gateway relies on: RS256 token
 * verification, deduplication by event id, per-tenant statistics, and rate and quota limits for the small-plan tenant
 * (100 events/s, burst 500, quota 1000).
 */
final class FakePlatform implements AutoCloseable {

    static final String ISSUER = "event-platform-dev";
    static final String AUDIENCE = "event-platform";

    private final JsonMapper json = JsonMapper.builder().build();
    private final KeyPair keys;
    private final HttpServer server;
    private final Path privateKey;
    /** tenant -> event id -> type */
    private final Map<String, Map<String, String>> events = new HashMap<>();
    private final Map<String, double[]> buckets = new HashMap<>();
    private final Map<String, Long> quotaUsed = new HashMap<>();
    private long duplicates;

    FakePlatform() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keys = generator.generateKeyPair();
        privateKey = Files.createTempFile("demo-gateway-it", ".pem");
        Files.writeString(privateKey, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder().encodeToString(keys.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/v1/events", this::events);
        server.createContext("/v1/stats", this::stats);
        server.createContext("/api/v1/query", this::query);
        server.createContext("/api/v1/query_range", exchange -> respond(exchange, 200,
                "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}", Map.of()));
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    String privateKeyLocation() {
        return privateKey.toUri().toString();
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        Files.deleteIfExists(privateKey);
    }

    private void events(HttpExchange exchange) throws IOException {
        String tenant = verifiedTenant(exchange);
        if (tenant == null) {
            respond(exchange, 401, "", Map.of());
            return;
        }
        JsonNode batch = json.readTree(exchange.getRequestBody().readAllBytes()).path("events");
        int size = batch.size();
        synchronized (this) {
            if (tenant.endsWith("-b")) {
                String[] rejection = limit(tenant, size);
                if (rejection != null) {
                    respond(exchange, 429, "{\"title\":\"Too Many Requests\",\"status\":429,\"limit\":\""
                            + rejection[0] + "\"}", Map.of("Retry-After", rejection[1],
                            "Content-Type", "application/problem+json"));
                    return;
                }
            }
            Map<String, String> seen = events.computeIfAbsent(tenant, t -> new HashMap<>());
            for (JsonNode event : batch) {
                if (seen.putIfAbsent(event.path("eventId").asString(), event.path("type").asString()) != null) {
                    duplicates++;
                }
            }
        }
        respond(exchange, 202, "{\"accepted\":" + size + "}", Map.of("Content-Type", "application/json"));
    }

    /** Token bucket of 500 refilled at 100/s, plus a quota of 1000; null if allowed, else {limit, Retry-After}. */
    private String[] limit(String tenant, int size) {
        double[] bucket = buckets.computeIfAbsent(tenant, t -> new double[] {500, System.nanoTime()});
        long now = System.nanoTime();
        bucket[0] = Math.min(500, bucket[0] + (now - bucket[1]) / 1e9 * 100);
        bucket[1] = now;
        long used = quotaUsed.getOrDefault(tenant, 0L);
        if (used + size > 1000) {
            return new String[] {"quota", "3600"};
        }
        if (bucket[0] < size) {
            return new String[] {"rate", Long.toString((long) Math.ceil((size - bucket[0]) / 100))};
        }
        bucket[0] -= size;
        quotaUsed.put(tenant, used + size);
        return null;
    }

    private void stats(HttpExchange exchange) throws IOException {
        String tenant = verifiedTenant(exchange);
        if (tenant == null) {
            respond(exchange, 401, "", Map.of());
            return;
        }
        String type = params(exchange).get("type");
        long count;
        synchronized (this) {
            count = events.getOrDefault(tenant, Map.of()).values().stream()
                    .filter(t -> type == null || t.equals(type)).count();
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("tenantId", tenant);
        body.put("totalCount", count);
        body.put("totalValueSum", (double) count);
        respond(exchange, 200, json.writeValueAsString(body), Map.of("Content-Type", "application/json"));
    }

    private void query(HttpExchange exchange) throws IOException {
        String promql = params(exchange).getOrDefault("query", "");
        long value;
        synchronized (this) {
            value = promql.equals("sum(processor_events_duplicate_total)") ? duplicates : 0;
        }
        respond(exchange, 200, "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{},"
                + "\"value\":[" + Instant.now().getEpochSecond() + ",\"" + value + "\"]}]}}",
                Map.of("Content-Type", "application/json"));
    }

    /** The tenant of a token signed by the trusted key with the expected issuer, audience and expiry; else null. */
    private String verifiedTenant(HttpExchange exchange) {
        String header = exchange.getRequestHeaders().getFirst("Authorization");
        if (header == null || !header.startsWith("Bearer ")) {
            return null;
        }
        String[] parts = header.substring(7).split("\\.");
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(keys.getPublic());
            signature.update((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
            if (!signature.verify(Base64.getUrlDecoder().decode(parts[2]))) {
                return null;
            }
            JsonNode claims = json.readTree(Base64.getUrlDecoder().decode(parts[1]));
            boolean valid = ISSUER.equals(claims.path("iss").asString(""))
                    && AUDIENCE.equals(claims.path("aud").asString(""))
                    && claims.path("exp").asLong(0) > Instant.now().getEpochSecond();
            return valid ? claims.path("tenant_id").asString(null) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, String> params(HttpExchange exchange) {
        Map<String, String> params = new HashMap<>();
        String raw = URI.create(exchange.getRequestURI().toString()).getRawQuery();
        if (raw != null) {
            for (String pair : raw.split("&")) {
                int eq = pair.indexOf('=');
                params.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    private static void respond(HttpExchange exchange, int status, String body, Map<String, String> headers)
            throws IOException {
        headers.forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }
}
