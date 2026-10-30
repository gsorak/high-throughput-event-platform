package io.highthroughput.ingest;

import io.highthroughput.contract.TenantIds;
import io.highthroughput.contract.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Token validation with the real decoder and key (ADR-0013). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class AuthenticationIT {

    private static final String TENANT = "auth-test";

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    static Stream<Arguments> invalidTokens() {
        Instant twoHoursAgo = Instant.now().minus(Duration.ofHours(2));
        return Stream.of(
                arguments("expired", TestJwt.sign(TestJwt.claims(TENANT)
                        .issuedAt(twoHoursAgo).expiresAt(twoHoursAgo.plus(Duration.ofMinutes(10))).build())),
                arguments("without exp", TestJwt.sign(TestJwt.claims(TENANT)
                        .claims(claims -> claims.remove(JwtClaimNames.EXP)).build())),
                arguments("wrong issuer", TestJwt.sign(TestJwt.claims(TENANT).issuer("someone-else").build())),
                arguments("wrong audience", TestJwt.sign(TestJwt.claims(TENANT)
                        .audience(List.of("another-service")).build())),
                arguments("without tenant", TestJwt.sign(TestJwt.claims(TENANT)
                        .claims(claims -> claims.remove(TenantIds.JWT_CLAIM)).build())),
                arguments("malformed tenant", TestJwt.sign(TestJwt.claims("ACME/../other").build())),
                arguments("untrusted signing key", TestJwt.signWithOtherKey(TestJwt.claims(TENANT).build())),
                arguments("not a JWT", "not-a-jwt"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTokens")
    void invalidTokenIsRejected(String description, String token) throws Exception {
        HttpResponse<String> response = post("Bearer " + token, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .hasValueSatisfying(value -> assertThat(value).startsWith("Bearer").contains("invalid_token")
                        .doesNotContain("resource_metadata"));
        assertThat(response.headers().firstValue("Content-Type")).hasValue("application/problem+json");
    }

    @Test
    void missingTokenIsRejected() throws Exception {
        HttpResponse<String> response = post(null, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.headers().firstValue("WWW-Authenticate"))
                .hasValue("Bearer");
    }

    @Test
    void tenantComesFromTheTokenEvenIfAHeaderNamesAnother() throws Exception {
        String tokenTenant = TestBatches.uniqueTenant();
        String headerTenant = TestBatches.uniqueTenant();

        HttpResponse<String> response = post(TestJwt.bearer(tokenTenant), headerTenant);

        assertThat(response.statusCode()).isEqualTo(202);
        List<ConsumerRecord<String, byte[]>> records = TopicContents.readAll(kafka.getBootstrapServers(),
                Topics.EVENTS);
        assertThat(records).filteredOn(record -> tokenTenant.equals(record.key())).hasSize(1);
        assertThat(records).filteredOn(record -> headerTenant.equals(record.key())).isEmpty();
    }

    private HttpResponse<String> post(String authorization, String tenantHeader) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/events"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(TestBatches.withEventIds(TestBatches.randomIds(1))));
        if (authorization != null) {
            request.header("Authorization", authorization);
        }
        if (tenantHeader != null) {
            request.header("X-Tenant-Id", tenantHeader);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
