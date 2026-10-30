package io.highthroughput.demo.token;

import io.highthroughput.contract.TenantIds;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mints the RS256 bearer tokens the gateway sends to the platform on behalf of a demo tenant. Tokens are created per
 * request and never returned to the browser; the UI only sees decoded claims.
 *
 * <p>The two negative variants exist for the isolation demonstration: a valid token whose {@code tenant_id} was
 * altered afterwards, and a token signed by a key the platform does not trust. The platform must reject both.
 */
public class TokenIssuer {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP = new TypeReference<>() {
    };

    private final TokenProperties properties;
    private final JsonMapper json;
    private final Clock clock;
    private final PrivateKey signingKey;
    private final PrivateKey untrustedKey;

    public TokenIssuer(TokenProperties properties, JsonMapper json, Clock clock) {
        this.properties = properties;
        this.json = json;
        this.clock = clock;
        this.signingKey = readPrivateKey(properties);
        this.untrustedKey = generateKey();
    }

    /** A token the platform accepts for {@code tenantId}. */
    public String issue(String tenantId) {
        return sign(claims(tenantId), signingKey);
    }

    /** A well-formed token for {@code tenantId} signed with a throwaway key generated at startup. */
    public String issueUntrusted(String tenantId) {
        return sign(claims(tenantId), untrustedKey);
    }

    /** {@code token} with its {@code tenant_id} claim replaced; header and signature are kept as they were. */
    public String withTenant(String token, String tenantId) {
        String[] parts = token.split("\\.");
        Map<String, Object> claims = decode(parts[1]);
        claims.put(TenantIds.JWT_CLAIM, tenantId);
        return parts[0] + "." + encode(claims) + "." + parts[2];
    }

    /** The claims of {@code token}, for display. The signature is not checked here; the platform checks it. */
    public Map<String, Object> claimsOf(String token) {
        return decode(token.split("\\.")[1]);
    }

    private Map<String, Object> claims(String tenantId) {
        Instant now = clock.instant();
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("iss", properties.issuer());
        claims.put("aud", properties.audience());
        claims.put(TenantIds.JWT_CLAIM, tenantId);
        claims.put("iat", now.getEpochSecond());
        claims.put("exp", now.plus(properties.ttl()).getEpochSecond());
        return claims;
    }

    private String sign(Map<String, Object> claims, PrivateKey key) {
        String signingInput = encode(Map.of("alg", "RS256", "typ", "JWT")) + "." + encode(claims);
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(key);
            signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
            return signingInput + "." + ENCODER.encodeToString(signature.sign());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot sign token", e);
        }
    }

    private String encode(Map<String, Object> value) {
        return ENCODER.encodeToString(json.writeValueAsBytes(value));
    }

    private Map<String, Object> decode(String part) {
        return json.readValue(DECODER.decode(part), MAP);
    }

    private static PrivateKey readPrivateKey(TokenProperties properties) {
        try (var in = properties.privateKeyLocation().getInputStream()) {
            String pem = new String(in.readAllBytes(), StandardCharsets.US_ASCII)
                    .replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read demo.token.private-key-location "
                    + properties.privateKeyLocation(), e);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalStateException("demo.token.private-key-location is not a PKCS#8 PEM RSA key", e);
        }
    }

    private static PrivateKey generateKey() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair().getPrivate();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
