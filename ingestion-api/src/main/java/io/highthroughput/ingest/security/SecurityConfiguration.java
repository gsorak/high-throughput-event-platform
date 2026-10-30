package io.highthroughput.ingest.security;

import io.highthroughput.contract.TenantIds;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.converter.RsaKeyConverters;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.HandlerExceptionResolver;

import java.io.IOException;
import java.io.InputStream;
import java.security.interfaces.RSAPublicKey;
import java.util.Collection;

/**
 * Bearer-token authentication for {@code /v1/**} (ADR-0013). A token is
 * accepted only with a valid RS256 signature,
 * an {@code exp} in the future, the configured {@code iss} and {@code aud}, and
 * a well-formed {@code tenant_id}
 * claim — the only source of the tenant. Actuator endpoints stay open (local
 * only; see the README limitations).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JwtProperties.class)
public class SecurityConfiguration {

    @Bean
    SecurityFilterChain apiSecurity(HttpSecurity http,
            @Qualifier("handlerExceptionResolver") HandlerExceptionResolver exceptionResolver) throws Exception {
        // Sets 401 and the RFC 6750 WWW-Authenticate header, then lets the
        // @RestControllerAdvice write the same problem+json body as other errors.
        BearerTokenAuthenticationEntryPoint bearer = new BearerTokenAuthenticationEntryPoint();
        AuthenticationEntryPoint entryPoint = (request, response, exception) -> {
            bearer.commence(request, response, exception);
            // Spring Security 7 always appends RFC 9728 resource_metadata. Without an
            // authorization server there is no
            // metadata worth serving (ADR-0013), so the parameter is removed rather than
            // pointing to a 404.
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, response.getHeader(HttpHeaders.WWW_AUTHENTICATE)
                    .replaceFirst(",? ?resource_metadata=\"[^\"]*\"", "").stripTrailing());
            exceptionResolver.resolveException(request, response, null, exception);
        };
        return http
                .securityMatcher("/v1/**")
                .authorizeHttpRequests(requests -> requests.anyRequest().authenticated())
                .oauth2ResourceServer(
                        oauth2 -> oauth2.jwt(Customizer.withDefaults()).authenticationEntryPoint(entryPoint))
                .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(entryPoint))
                .sessionManagement(sessions -> sessions.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // No cookies or sessions, so there is nothing for CSRF to exploit.
                .csrf(AbstractHttpConfigurer::disable)
                .build();
    }

    @Bean
    JwtDecoder jwtDecoder(JwtProperties properties) throws IOException {
        RSAPublicKey publicKey;
        try (InputStream pem = properties.publicKeyLocation().getInputStream()) {
            publicKey = RsaKeyConverters.x509().convert(pem);
        }
        JwtTimestampValidator expiry = new JwtTimestampValidator();
        expiry.setAllowEmptyExpiryClaim(false);

        NimbusJwtDecoder decoder = NimbusJwtDecoder.withPublicKey(publicKey).build();
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                expiry,
                new JwtIssuerValidator(properties.issuer()),
                new JwtClaimValidator<Object>(JwtClaimNames.AUD,
                        audience -> audience instanceof Collection<?> values && values.contains(properties.audience())),
                new JwtClaimValidator<Object>(TenantIds.JWT_CLAIM,
                        tenant -> tenant instanceof String id && TenantIds.isValid(id))));
        return decoder;
    }
}
