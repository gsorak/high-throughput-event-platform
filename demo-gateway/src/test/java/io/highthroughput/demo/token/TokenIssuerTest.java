package io.highthroughput.demo.token;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.FileSystemResource;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class TokenIssuerTest {

    private static final Instant NOW = Instant.parse("2026-05-01T10:00:00Z");

    @TempDir
    Path dir;

    @Test
    void issuesRs256TokensThePlatformCanVerify() throws Exception {
        KeyPair keys = generate();
        TokenIssuer issuer = issuer(keys);

        String token = issuer.issue("demo-0123456789abcdef-a");

        assertThat(verifies(token, keys.getPublic())).isTrue();
        var claims = issuer.claimsOf(token);
        assertThat(claims)
                .containsEntry("iss", "event-platform-dev")
                .containsEntry("aud", "event-platform")
                .containsEntry("tenant_id", "demo-0123456789abcdef-a");
        assertThat(((Number) claims.get("iat")).longValue()).isEqualTo(NOW.getEpochSecond());
        assertThat(((Number) claims.get("exp")).longValue()).isEqualTo(NOW.plusSeconds(300).getEpochSecond());
        assertThat(new String(Base64.getUrlDecoder().decode(token.split("\\.")[0]), StandardCharsets.UTF_8))
                .contains("\"alg\":\"RS256\"");
    }

    @Test
    void anAlteredTokenKeepsItsSignatureAndNoLongerVerifies() throws Exception {
        KeyPair keys = generate();
        TokenIssuer issuer = issuer(keys);
        String token = issuer.issue("demo-0123456789abcdef-a");

        String altered = issuer.withTenant(token, "demo-0123456789abcdef-b");

        assertThat(issuer.claimsOf(altered)).containsEntry("tenant_id", "demo-0123456789abcdef-b");
        assertThat(altered.split("\\.")[2]).isEqualTo(token.split("\\.")[2]);
        assertThat(verifies(altered, keys.getPublic())).isFalse();
    }

    @Test
    void untrustedTokensAreSignedWithAnotherKey() throws Exception {
        KeyPair keys = generate();
        TokenIssuer issuer = issuer(keys);

        String token = issuer.issueUntrusted("demo-0123456789abcdef-b");

        assertThat(issuer.claimsOf(token)).containsEntry("tenant_id", "demo-0123456789abcdef-b");
        assertThat(verifies(token, keys.getPublic())).isFalse();
    }

    private TokenIssuer issuer(KeyPair keys) throws Exception {
        Path pem = dir.resolve("private.pem");
        Files.writeString(pem, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(
                        keys.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n");
        return new TokenIssuer(new TokenProperties(new FileSystemResource(pem), "event-platform-dev", "event-platform",
                Duration.ofMinutes(5)), JsonMapper.builder().build(), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static KeyPair generate() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static boolean verifies(String token, PublicKey key) throws Exception {
        int dot = token.lastIndexOf('.');
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initVerify(key);
        signature.update(token.substring(0, dot).getBytes(StandardCharsets.US_ASCII));
        return signature.verify(Base64.getUrlDecoder().decode(token.substring(dot + 1)));
    }
}
