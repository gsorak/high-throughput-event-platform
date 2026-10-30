package io.highthroughput.ingest.security;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

/**
 * Settings under {@code security.jwt.*} (ADR-0013).
 *
 * @param publicKeyLocation PEM-encoded RSA public key that verifies token
 *                          signatures (RS256)
 * @param issuer            required value of the {@code iss} claim
 * @param audience          value that the {@code aud} claim must contain
 */
@Validated
@ConfigurationProperties("security.jwt")
public record JwtProperties(
        @NotNull Resource publicKeyLocation,
        @NotBlank String issuer,
        @NotBlank String audience) {
}
