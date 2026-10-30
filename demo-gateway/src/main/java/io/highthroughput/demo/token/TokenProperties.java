package io.highthroughput.demo.token;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Token issuing of the demo gateway, kept apart from the platform's own settings (ADR-0015). Locally this is the
 * development key pair; a public deployment uses a dedicated demo issuer and key pair.
 *
 * @param privateKeyLocation PKCS#8 PEM RSA private key
 * @param issuer             {@code iss} claim the platform expects
 * @param audience           {@code aud} claim the platform expects
 * @param ttl                lifetime of each token; tokens are minted per request and never leave the gateway
 */
@Validated
@ConfigurationProperties("demo.token")
public record TokenProperties(
        @NotNull Resource privateKeyLocation,
        @NotBlank String issuer,
        @NotBlank String audience,
        @NotNull Duration ttl) {
}
