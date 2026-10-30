# ADR-0001: Platform versions

- Status: Accepted (2026-10-27). Alternatives and references added in the documentation review (2026-10-27).
- Stage: 1 (implemented)
- Assessment: **Design-time.** The approval made Java 25 and Boot 4.1 conditional on library compatibility, which was
  verified by resolving the build. Alternatives to Spring Boot itself and to Maven are compared in
  [technology-selection.md](../architecture/technology-selection.md#21-runtime-framework-and-build).

## Context

The platform compares platform threads with virtual threads (Stage 4). On JDK 21, a virtual thread that blocks inside
`synchronized` pins its carrier thread; JDK 24 removed that (JEP 491). Kafka clients, JDBC drivers and HikariCP use
`synchronized`, so JDK 21 would distort the experiment. Spring Boot 4.0 OSS support ends in Dec 2026; 4.2 was not GA.

## Decision

- **Java 25 (LTS)**, **Spring Boot 4.1.2**. Every library version comes from Boot's BOM; none is overridden.
- Build with the **Maven Wrapper** (Maven 3.9.16). The enforcer plugin rejects older Maven and JDKs < 25.

Verified on 2026-10-27 by resolving the build (`./mvnw dependency:tree`), not from release notes:

| Library                 | Version | Note                                                                                                                                                 |
| ----------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spring Framework        | 7.0.10  |                                                                                                                                                      |
| Spring for Apache Kafka | 4.1.2   |                                                                                                                                                      |
| kafka-clients           | 4.2.2   | Broker in Docker is 4.3.2; Kafka clients and brokers are bidirectionally compatible                                                                  |
| Jackson                 | 3.1.7   | Package `tools.jackson.*`; annotations stay in `com.fasterxml.jackson.annotation`                                                                    |
| HikariCP                | 7.0.2   |                                                                                                                                                      |
| Flyway                  | 12.4.0  | Needs `flyway-database-postgresql`                                                                                                                   |
| PostgreSQL JDBC         | 42.7.14 | Server image `postgres:17.11` (widest managed-service support at the time)                                                                           |
| Micrometer              | 1.17.2  |                                                                                                                                                      |
| Testcontainers          | 2.0.5   | Modules renamed (`testcontainers-kafka`, `testcontainers-postgresql`); classes moved to `org.testcontainers.kafka` / `org.testcontainers.postgresql` |
| JUnit                   | 6.0.3   |                                                                                                                                                      |
| Mockito                 | 5.23.0  | Loaded as `-javaagent` (JDK 21+ warns on self-attach)                                                                                                |

Container images (pinned): `apache/kafka:4.3.2`, `postgres:17.11`, `prom/prometheus:v3.15.1`,
`danielqsj/kafka-exporter:v1.11.0`, `grafana/k6:2.3.0`.

## Alternatives considered

| Option                          | For                                                               | Against                                                                                                                       |
| ------------------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Java 25 + Boot 4.1 (chosen)** | Current LTS; no `synchronized` pinning; current Spring generation | Newest major versions of Spring, Jackson and JUnit; fewer answers online for migration problems                               |
| Java 21 + Boot 4.1              | Widest JDK support in tools and managed runtimes                  | Virtual threads pin in `synchronized` (fixed in JDK 24 by JEP 491), which would distort the Stage 4 thread experiment         |
| Java 25 + Boot 3.5              | Longest track record; Jackson 2; most examples online             | Previous generation: a Boot 4 migration (modular starters, Jackson 3, Spring Framework 7) would follow soon after the project |
| Java 25 + Boot 4.0              | First Boot 4 line                                                 | Open-source support ends in Dec 2026, during the project                                                                      |

## Consequences

- Boot 4 is modular: starters are per technology (`spring-boot-starter-webmvc`, `-kafka`, `-jdbc`, `-flyway`) and test
  slices live in matching `-test` modules (e.g. `@WebMvcTest` is in `org.springframework.boot.webmvc.test.autoconfigure`).
  See [docs/guides/spring-boot-4-notes.md](../guides/spring-boot-4-notes.md).
- `TestRestTemplate` is no longer in `spring-boot-test`; integration tests use the JDK `HttpClient`.
- Kafka 4.4.0 exists but is a `.0`; the broker stays on the latest 4.3 patch until there is a reason to move.
- Upgrades are a deliberate change: bump the Boot parent, re-run `dependency:tree`, update this table.

## Reconsider when

- Boot 4.2 has a patch release and the project is still active: upgrade, following the procedure above.
- A deployment target (Stage 6) does not offer JDK 25. Containers make this unlikely, but managed runtimes lag.
- A Kafka 4.4 patch release appears, or a broker fix is needed.

## References

- JEP 491, Synchronize Virtual Threads without Pinning: https://openjdk.org/jeps/491
- Spring Boot support timeline: https://spring.io/projects/spring-boot#support
- Spring Boot 4.0 migration guide: https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide
- Kafka client/broker compatibility: https://kafka.apache.org/documentation/#upgrade
