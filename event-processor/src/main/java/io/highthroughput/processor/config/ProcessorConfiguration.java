package io.highthroughput.processor.config;

import io.highthroughput.contract.EventEnvelopeCodec;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

/** {@code @EnableScheduling} runs {@code LedgerRetentionJob}. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ProcessorProperties.class)
@EnableScheduling
public class ProcessorConfiguration {

    /**
     * Injected wherever "now" matters (end-to-end latency), so it is an explicit
     * dependency.
     */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventEnvelopeCodec eventEnvelopeCodec() {
        return new EventEnvelopeCodec();
    }
}
