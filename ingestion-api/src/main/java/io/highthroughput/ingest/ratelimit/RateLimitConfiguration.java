package io.highthroughput.ingest.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitConfiguration {

    @Bean
    ValkeyBuckets valkeyBuckets(RateLimitProperties properties) {
        return new ValkeyBuckets(properties.valkeyUri(), properties.timeout());
    }

    @Bean
    TenantRateLimiter tenantRateLimiter(RateLimitProperties properties, ValkeyBuckets valkeyBuckets, Clock clock,
            MeterRegistry meterRegistry) {
        return new TenantRateLimiter(properties, valkeyBuckets, clock, meterRegistry);
    }
}
