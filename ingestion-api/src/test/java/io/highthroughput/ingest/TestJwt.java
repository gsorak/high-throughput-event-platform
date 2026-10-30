package io.highthroughput.ingest;

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
public final class TestJwt {

    public static final String ISSUER = "event-platform-dev";
    public static final String AUDIENCE = "event-platform";

    private static final KeyPair KEYS = generateKeyPair();
    private static final KeyPair OTHER_KEYS = generateKeyPair();
    private static final String PUBLIC_KEY_LOCATION = writePublicKey();

    private TestJwt() {
    }

    /** Value for {@code security.jwt.public-key-location}. */
    public static String publicKeyLocation() {
        return PUBLIC_KEY_LOCATION;
    }

    public static String bearer(String tenant) {
        return "Bearer " + sign(claims(tenant).build());
    }

    /**
     * Valid claims for the tenant; tests change one claim to make the token
     * invalid.
     */
    public static JwtClaimsSet.Builder claims(String tenant) {
        Instant now = Instant.now();
        return JwtClaimsSet.builder()
                .issuer(ISSUER)
                .audience(List.of(AUDIENCE))
                .issuedAt(now)
                .expiresAt(now.plus(Duration.ofMinutes(10)))
                .claim(TenantIds.JWT_CLAIM, tenant);
    }

    public static String sign(JwtClaimsSet claims) {
        return sign(KEYS, claims);
    }

    /** Signed with a key the application does not trust. */
    public static String signWithOtherKey(JwtClaimsSet claims) {
        return sign(OTHER_KEYS, claims);
    }

    private static String sign(KeyPair keys, JwtClaimsSet claims) {
        NimbusJwtEncoder encoder = NimbusJwtEncoder
                .withKeyPair((RSAPublicKey) keys.getPublic(), (RSAPrivateKey) keys.getPrivate())
                .build();
        JwsHeader header = JwsHeader.with(SignatureAlgorithm.RS256).build();
        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
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
