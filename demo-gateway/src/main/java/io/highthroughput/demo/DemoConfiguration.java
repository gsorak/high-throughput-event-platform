package io.highthroughput.demo;

import io.highthroughput.demo.platform.PlatformClient;
import io.highthroughput.demo.platform.PrometheusClient;
import io.highthroughput.demo.scenario.DedupService;
import io.highthroughput.demo.scenario.IsolationService;
import io.highthroughput.demo.scenario.LimitsService;
import io.highthroughput.demo.scenario.RunService;
import io.highthroughput.demo.scenario.SystemMetrics;
import io.highthroughput.demo.session.SessionStore;
import io.highthroughput.demo.token.TokenIssuer;
import io.highthroughput.demo.token.TokenProperties;
import io.highthroughput.demo.web.SecurityHeadersFilter;
import io.highthroughput.demo.web.UiConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration(proxyBeanMethods = false)
public class DemoConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Runs the long-lived scenario tasks; interrupted on shutdown. */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService demoExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    HttpClient platformHttpClient(DemoProperties properties) {
        return HttpClient.newBuilder()
                .connectTimeout(properties.backendTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @Bean
    TokenIssuer tokenIssuer(TokenProperties properties, JsonMapper json, Clock clock) {
        return new TokenIssuer(properties, json, clock);
    }

    @Bean
    PlatformClient platformClient(DemoProperties properties, TokenIssuer tokens, JsonMapper json, HttpClient http) {
        return new PlatformClient(properties, tokens, json, http);
    }

    @Bean
    PrometheusClient prometheusClient(DemoProperties properties, JsonMapper json, HttpClient http) {
        return new PrometheusClient(properties, json, http);
    }

    @Bean
    SystemMetrics systemMetrics(PrometheusClient prometheus, Clock clock, DemoProperties properties) {
        return new SystemMetrics(prometheus, clock, properties.grafanaUrl());
    }

    @Bean
    SessionStore sessionStore(DemoProperties properties, Clock clock) {
        return new SessionStore(properties.limits(), clock);
    }

    @Bean
    RunService runService(DemoProperties properties, PlatformClient platform, ExecutorService demoExecutor,
            Clock clock) {
        return new RunService(properties.limits(), platform, demoExecutor, clock);
    }

    @Bean
    DedupService dedupService(DemoProperties properties, PlatformClient platform, SystemMetrics metrics,
            ExecutorService demoExecutor, Clock clock) {
        return new DedupService(properties.limits(), platform, metrics, demoExecutor, clock);
    }

    @Bean
    IsolationService isolationService(PlatformClient platform, TokenIssuer tokens, Clock clock) {
        return new IsolationService(platform, tokens, clock);
    }

    @Bean
    LimitsService limitsService(PlatformClient platform, DemoProperties properties, Clock clock) {
        return new LimitsService(platform, properties.tenantB(), clock);
    }

    @Bean
    FilterRegistrationBean<SecurityHeadersFilter> securityHeadersFilter() {
        FilterRegistrationBean<SecurityHeadersFilter> registration =
                new FilterRegistrationBean<>(new SecurityHeadersFilter());
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    @ConditionalOnExpression("!'${demo.ui-location:}'.isBlank()")
    UiConfiguration uiConfiguration(DemoProperties properties) {
        return new UiConfiguration(properties.uiLocation());
    }
}
