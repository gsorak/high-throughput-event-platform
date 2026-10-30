package io.highthroughput.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Backend-for-frontend of the interactive demo (ADR-0015). It holds visitor sessions, issues tokens for their
 * temporary tenants and generates bounded load, and talks to the platform only through its public APIs.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class DemoGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoGatewayApplication.class, args);
    }
}
