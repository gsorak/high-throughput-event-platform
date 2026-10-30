package io.highthroughput.processor;

import io.highthroughput.contract.TenantIds;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * Mints RS256 tokens like {@code scripts/DevJwt.java} does, with a key pair
 * generated per test JVM. The public key
 * is written to a temporary PEM file that
 * {@code security.jwt.public-key-location} points to.
 */
final class TestJwt {

    private static final KeyPair KEYS = generateKeyPair();
    private static final String PUBLIC_KEY_LOCATION = writePublicKey();

    private TestJwt() {
    }

    /** Value for {@code security.jwt.public-key-location}. */
    static String publicKeyLocation() {
        return PUBLIC_KEY_LOCATION;
    }

    static String bearer(String tenant) {
        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("event-platform-dev")
                .audience(List.of("event-platform"))
                .issuedAt(now)
                .expiresAt(now.plus(Duration.ofMinutes(10)))
                .claim(TenantIds.JWT_CLAIM, tenant)
                .build();
        NimbusJwtEncoder encoder = NimbusJwtEncoder
                .withKeyPair((RSAPublicKey) KEYS.getPublic(), (RSAPrivateKey) KEYS.getPrivate())
                .build();
        JwsHeader header = JwsHeader.with(SignatureAlgorithm.RS256).build();
        return "Bearer " + encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String writePublicKey() {
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(KEYS.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        try {
            Path file = Files.createTempFile("jwt-public", ".pem");
            file.toFile().deleteOnExit();
            Files.writeString(file, pem);
            return file.toUri().toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
