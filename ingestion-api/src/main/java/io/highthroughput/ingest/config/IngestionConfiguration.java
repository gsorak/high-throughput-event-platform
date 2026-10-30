package io.highthroughput.ingest.config;

import io.highthroughput.contract.EventEnvelopeCodec;
import io.highthroughput.ingest.web.AdmissionControlFilter;
import io.highthroughput.ingest.web.RequestBodyLimitFilter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.HandlerExceptionResolver;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(IngestionProperties.class)
public class IngestionConfiguration {

    /**
     * Injected wherever "now" matters, so the time-window rules are explicit
     * dependencies.
     */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventEnvelopeCodec eventEnvelopeCodec() {
        return new EventEnvelopeCodec();
    }

    /** Applies the body-size limit only to the ingestion endpoint. */
    @Bean
    FilterRegistrationBean<RequestBodyLimitFilter> requestBodyLimitFilter(IngestionProperties properties) {
        var registration = new FilterRegistrationBean<>(
                new RequestBodyLimitFilter(properties.maxRequestBodySize().toBytes()));
        registration.addUrlPatterns("/v1/events");
        return registration;
    }

    /**
     * Runs before Spring Security (filter order -100), so even unauthenticated
     * floods take a permit only briefly
     * and cannot queue unbounded work.
     */
    @Bean
    FilterRegistrationBean<AdmissionControlFilter> admissionControlFilter(IngestionProperties properties,
            @Qualifier("handlerExceptionResolver") HandlerExceptionResolver exceptionResolver) {
        var registration = new FilterRegistrationBean<>(
                new AdmissionControlFilter(properties.maxInFlight(), exceptionResolver));
        registration.addUrlPatterns("/v1/events");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        return registration;
    }
}
