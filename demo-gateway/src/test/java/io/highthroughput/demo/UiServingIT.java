package io.highthroughput.demo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** The gateway serves the built UI, with the security headers, and falls back to {@code index.html}. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UiServingIT {

    private static final FakePlatform PLATFORM;
    private static final Path UI;

    static {
        try {
            PLATFORM = new FakePlatform();
            UI = Files.createTempDirectory("demo-ui");
            Files.writeString(UI.resolve("index.html"), "<!doctype html><title>demo</title>");
            Files.createDirectory(UI.resolve("assets"));
            Files.writeString(UI.resolve("assets/app.js"), "console.log('demo');");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("demo.ingestion-url", PLATFORM::url);
        registry.add("demo.processor-url", PLATFORM::url);
        registry.add("demo.prometheus-url", PLATFORM::url);
        registry.add("demo.ui-location", () -> UI.toUri().toString());
        registry.add("demo.token.private-key-location", PLATFORM::privateKeyLocation);
    }

    @AfterAll
    static void stopPlatform() throws Exception {
        PLATFORM.close();
    }

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void theRootAndUnknownPagesServeTheApp() throws Exception {
        for (String path : new String[] {"/", "/index.html", "/some/page"}) {
            HttpResponse<String> response = get(path);
            assertThat(response.statusCode()).as(path).isEqualTo(200);
            assertThat(response.body()).as(path).contains("<title>demo</title>");
            assertThat(response.headers().firstValue("Content-Security-Policy")).as(path).isPresent();
            assertThat(response.headers().firstValue("X-Frame-Options")).as(path).hasValue("DENY");
        }
    }

    @Test
    void assetsAreServedAndMissingFilesAreNotFound() throws Exception {
        assertThat(get("/assets/app.js").body()).contains("console.log");
        assertThat(get("/assets/missing.js").statusCode()).isEqualTo(404);
        assertThat(get("/demo/unknown").statusCode()).isEqualTo(404);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
