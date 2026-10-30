# Technology selection

This document records which technologies were selected or excluded and why. It covers the choices that are too small
for an ADR. Where an ADR exists, this document only links to it.

**Status**

- **S1**: implemented in Stage 1.
- **S2 … S6**: planned for that stage. Provisional: it can change before the stage starts.
- **Excluded**: deliberately not used.

**Assessment**

- **Design-time**: the reasoning was recorded in the architecture review (2026-10-27) or in its approval.
- **Retrospective**: written in the documentation review before Stage 2. Alternatives were not compared when the
  choice was made. A retrospective assessment explains a choice. It does not claim that the choice was deliberated.

Choices given by the project brief (Kafka, Spring Boot, PostgreSQL, Spring Security resource server, a Redis-type
store) were accepted in the review without comparing alternatives. Their assessments here are retrospective.

## 1. Summary

| Area                    | Choice                                                | Status         | Assessment         | Alternatives compared                                       | Details                                                                                                |
| ----------------------- | ----------------------------------------------------- | -------------- | ------------------ | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| JDK and Spring versions | Java 25, Spring Boot 4.1                              | S1             | Design-time        | Java 21, Boot 3.5, Boot 4.0                                 | [ADR-0001](../adr/0001-platform-versions.md)                                                           |
| Application framework   | Spring Boot                                           | S1             | Retrospective      | Quarkus, Micronaut, no framework                            | [2.1](#21-runtime-framework-and-build)                                                                 |
| Build                   | Maven Wrapper + enforcer                              | S1             | Retrospective      | Gradle                                                      | [2.1](#21-runtime-framework-and-build)                                                                 |
| Web stack               | Spring MVC on virtual threads                         | S1             | Partly design-time | WebFlux, platform threads, MVC async                        | [ADR-0009](../adr/0009-spring-mvc-virtual-threads.md)                                                  |
| API protocol and errors | HTTP/JSON, RFC 9457 problem details                   | S1             | Retrospective      | gRPC                                                        | [2.2](#22-http-api)                                                                                    |
| Event log               | Apache Kafka 4.3 (KRaft)                              | S1             | Retrospective      | PostgreSQL queue, RabbitMQ, Redpanda, Pulsar                | [ADR-0008](../adr/0008-kafka-buffer-two-deployables.md)                                                |
| Kafka client            | Spring for Apache Kafka                               | S1             | Retrospective      | Plain `kafka-clients`, Spring Cloud Stream                  | [2.3](#23-kafka-client-and-consumer-configuration)                                                     |
| Wire format             | JSON with an explicit codec and `schemaVersion`       | S1             | Partly design-time | Avro or Protobuf with a registry, CloudEvents               | [ADR-0010](../adr/0010-json-envelope-without-schema-registry.md)                                       |
| Processing              | Plain consumer, PostgreSQL upsert                     | S1             | Partly design-time | Kafka Streams, Flink, ClickHouse, TimescaleDB               | [ADR-0011](../adr/0011-consumer-postgres-aggregation.md)                                               |
| Database                | PostgreSQL 17                                         | S1             | Retrospective      | MySQL, analytical stores                                    | [2.4](#24-persistence)                                                                                 |
| Data access             | Spring JDBC `JdbcClient`                              | S1             | Partly design-time | JPA, Spring Data JDBC, jOOQ, MyBatis                        | [ADR-0007](../adr/0007-spring-jdbc-over-jpa.md)                                                        |
| Migrations, pool        | Flyway, HikariCP                                      | S1             | Retrospective      | Liquibase; other pools, PgBouncer                           | [2.4](#24-persistence)                                                                                 |
| Tenancy model           | Pooled, application predicates                        | S1             | Retrospective      | Pool + RLS, schema per tenant, silo                         | [ADR-0012](../adr/0012-pooled-multi-tenancy.md)                                                        |
| Metrics                 | Micrometer + Prometheus (pull)                        | S1             | Retrospective      | OpenTelemetry metrics and OTLP, VictoriaMetrics, hosted     | [2.5](#25-observability)                                                                               |
| Consumer lag            | kafka-exporter                                        | S1             | Design-time        | KMinion, Burrow, client metrics                             | [2.5](#25-observability)                                                                               |
| Tests                   | JUnit 6, AssertJ, Awaitility, Mockito, Testcontainers | S1             | Partly design-time | Embedded Kafka, H2                                          | [2.6](#26-testing-and-load-generation)                                                                 |
| Load generation         | k6                                                    | S1             | Retrospective      | Gatling, JMeter                                             | [2.6](#26-testing-and-load-generation)                                                                 |
| Tenant identity         | Self-issued JWT, Spring Security resource server      | S2             | Design-time        | Keycloak, Spring Authorization Server, API keys, mTLS       | [ADR-0013](../adr/0013-jwt-without-identity-provider.md)                                               |
| Rate limiting           | Bucket4j in the API, state in Valkey                  | S2             | Partly design-time | Edge limiting, local buckets, Kafka quotas                  | [ADR-0014](../adr/0014-shared-rate-limit-state.md), [ADR-0004](../adr/0004-rate-limit-store-outage.md) |
| Circuit breaker         | Resilience4j CircuitBreaker only                      | S2             | Design-time        | Spring Cloud Circuit Breaker, Spring Framework 7 resilience | [3.1](#31-resilience-around-valkey)                                                                    |
| Admission control       | `Semaphore`, fast 503                                 | S2             | Design-time        | `@ConcurrencyLimit`, Tomcat limits, nginx `limit_conn`      | [3.2](#32-admission-control)                                                                           |
| Load balancer, images   | nginx; Dockerfiles                                    | S2             | Retrospective      | HAProxy, Envoy, Spring Cloud Gateway; Buildpacks, Jib       | [3.3](#33-load-balancing-and-container-images)                                                         |
| Poison records          | Blocking retries, then dead-letter topic              | S3             | Design-time        | Retry topics, error table                                   | [3.4](#34-failure-handling-in-the-consumer-stage-3)                                                    |
| Dashboards, profiling   | Grafana, JFR                                          | S4             | Design-time        | async-profiler (optional)                                   | [3.5](#35-dashboards-profiling-and-tracing-stages-45)                                                  |
| Fault injection         | Toxiproxy                                             | S5             | Design-time        | `docker pause`, `tc netem`                                  | [3.5](#35-dashboards-profiling-and-tracing-stages-45)                                                  |
| Tracing                 | Micrometer Tracing → OpenTelemetry → Tempo            | S5             | Partly design-time | OpenTelemetry Java agent; Jaeger, Zipkin                    | [3.5](#35-dashboards-profiling-and-tracing-stages-45)                                                  |
| Cloud, CI               | AWS; GitHub Actions                                   | S6, last stage | Not assessed       | —                                                           | [3.6](#36-not-yet-assessed)                                                                            |

## 2. Stage 1 choices (implemented)

### 2.1 Runtime, framework and build

Java and Spring versions: [ADR-0001](../adr/0001-platform-versions.md).

**Spring Boot versus Quarkus, Micronaut or no framework.** Retrospective.

| Criterion                     | **Spring Boot (chosen)**                                                   | Quarkus                                               | Micronaut           | No framework                                  |
| ----------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------- | --------------------------------------------- |
| Kafka consumer                | Listener container: concurrency, commits, back-off, dead-letter publishing | SmallRye Reactive Messaging (channels, acks)          | Micronaut Kafka     | Own poll loop, rebalance handling and retries |
| JWT resource server (Stage 2) | Spring Security, configuration only                                        | SmallRye JWT / OIDC extension                         | Micronaut Security  | Hand-written with a JOSE library              |
| Startup and footprint         | Seconds on the JVM; AOT and native image possible                          | Build-time wiring; native image is first-class        | Compile-time wiring | Smallest                                      |
| Matters here?                 | Long-running services measured in steady state                             | Startup and memory matter for scale-to-zero, not here | Same                | —                                             |

- The decisive features are Spring for Apache Kafka's error handling (ADR-0002, Stage 3) and Spring Security's
  resource server (Stage 2). Both are mature and used as configured components, not re-implemented.
- The cold-JVM effect in the baseline (Finding 4 in [00-baseline.md](../performance/00-baseline.md)) is JIT warm-up,
  not startup. A native image would remove warm-up but usually reaches a lower peak throughput than the C2 JIT.
  JDK 25's AOT cache with method profiles (JEP 483, JEP 515) targets warm-up without changing frameworks and is a
  candidate Stage 4 experiment.
- **Reconsider when** a deployment needs scale-to-zero or very small memory (serverless), which is not planned.

**Maven versus Gradle.** Retrospective.

- Maven: declarative POMs, Boot's BOM through the parent, the enforcer rule for JDK and Maven versions, and a wrapper
  that pins Maven itself. Every Java developer can read it.
- Gradle: incremental builds, a build cache and programmable builds. For three modules the difference is small: the
  full `verify` takes about 3.5 minutes, almost all of it Testcontainers integration tests.
- **Reconsider when** compile time, not tests, dominates the build, or custom build logic grows beyond plugin
  configuration.

### 2.2 HTTP API

Spring MVC on virtual threads: [ADR-0009](../adr/0009-spring-mvc-virtual-threads.md).

**HTTP/JSON versus gRPC.** Retrospective.

| Criterion          | **HTTP/JSON (chosen)**                                        | gRPC                                                  |
| ------------------ | ------------------------------------------------------------- | ----------------------------------------------------- |
| Client effort      | Any HTTP client; `curl` for debugging                         | Generated stubs per language; tools such as `grpcurl` |
| Per-event overhead | Text parsing; amortised over batches of up to 500 events      | Compact binary; client streaming possible             |
| Errors             | RFC 9457 problem details with JSON pointers to invalid fields | gRPC status codes and rich error details              |
| Load balancing     | Any HTTP load balancer                                        | Needs HTTP/2-aware balancing (long-lived connections) |

- Tenants are heterogeneous back ends. A JSON batch endpoint is the lowest-friction interface, and batching removes
  most of the per-request overhead that gRPC would save.
- The cost of JSON parsing has not been measured (see [Q16](#6-open-questions-and-recommendations)).
- **Reconsider when** Stage 4 profiling shows request parsing as a major share of API CPU, or tenants need a
  streaming connection.

**Validation and errors.** Bean Validation annotations on the request records check field shapes. `BatchValidator`
adds what annotations cannot express: duplicate `eventId`s within a batch, the `occurredAt` window and finite
`value`s. Error responses use Spring's `ProblemDetail` (RFC 9457) instead of a custom error format.

### 2.3 Kafka client and consumer configuration

Kafka itself: [ADR-0008](../adr/0008-kafka-buffer-two-deployables.md). Transactions:
[ADR-0002](../adr/0002-acceptance-semantics.md). Wire format: [ADR-0010](../adr/0010-json-envelope-without-schema-registry.md).
Kafka Streams: [ADR-0011](../adr/0011-consumer-postgres-aggregation.md).

**Spring for Apache Kafka versus plain `kafka-clients` versus Spring Cloud Stream.** Retrospective.

| Criterion                      | **Spring for Apache Kafka (chosen)**                                     | Plain `kafka-clients`                           | Spring Cloud Stream (Kafka binder)                                                            |
| ------------------------------ | ------------------------------------------------------------------------ | ----------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Programming model              | `KafkaTemplate`, `@KafkaListener`                                        | Producer and consumer objects; own poll threads | `Supplier`/`Function`/`Consumer` beans bound by configuration                                 |
| Commits                        | `AckMode` on the listener container                                      | Hand-written; must be correct across rebalances | Binder properties that map to the Spring Kafka container                                      |
| Error handling                 | `DefaultErrorHandler`, back-off, non-retryable exceptions, DLT recoverer | Hand-written                                    | Binder retry and DLQ settings; fine control through customizers of the Spring Kafka container |
| Layers between code and client | One                                                                      | None                                            | Two: Spring Cloud Stream over Spring Kafka                                                    |
| Broker portability             | None                                                                     | None                                            | Binders for Kafka, RabbitMQ, Pulsar and others                                                |

- ADR-0002 and Stage 3 depend on exact commit behaviour, error classification and dead-letter handling. Spring Kafka
  exposes these directly. The Spring Cloud Stream Kafka binder is built on Spring Kafka, so it adds an abstraction
  over exactly the settings this project studies.
- Broker portability has no value here. Replay, partitions and offsets are Kafka-specific
  ([ADR-0008](../adr/0008-kafka-buffer-two-deployables.md)), so a different binder would change behaviour anyway.
- Plain `kafka-clients` would be instructive, but a correct concurrent consumer with rebalance-safe commits, back-off
  and a dead-letter path is a lot of code that Spring Kafka already provides and tests.
- **Reconsider** Spring Cloud Stream only if the platform must run on more than one broker type.

**Settings that are choices, not defaults** (details in [kafka.md](../guides/kafka.md)):

- `ByteArraySerializer`/`ByteArrayDeserializer` plus the codec instead of Spring's JSON serializers (ADR-0010).
- Listener: record-at-a-time, concurrency 3, `AckMode.BATCH`, `max-poll-records` 500. One database round trip per
  event limits throughput to about 1,400 events/s. Batch listeners are the first Stage 4 experiment.
- `group.protocol: classic`. The KIP-848 `consumer` protocol has been generally available since Kafka 4.0. It moves
  partition assignment to the broker and avoids stop-the-world rebalances. It is a Stage 4 experiment and is relevant
  to consumer scaling ([Q14](#6-open-questions-and-recommendations)).

### 2.4 Persistence

Processing model and analytical stores: [ADR-0011](../adr/0011-consumer-postgres-aggregation.md). Data access API:
[ADR-0007](../adr/0007-spring-jdbc-over-jpa.md). Stats placement: [ADR-0006](../adr/0006-stats-endpoint-in-processor.md).

**PostgreSQL.** Retrospective; given by the brief. The workload needs an atomic upsert with an increment
(`ON CONFLICT … DO UPDATE`), range scans on a primary-key prefix, `ON CONFLICT DO NOTHING` for the Stage 3 ledger and,
optionally, row-level security ([ADR-0012](../adr/0012-pooled-multi-tenancy.md)). MySQL would also work
(`ON DUPLICATE KEY UPDATE`); no feature of this workload separates the two clearly. Both are available as managed
services.

**Flyway versus Liquibase.** Retrospective.

| Criterion        | **Flyway (chosen)**                                   | Liquibase                                                               |
| ---------------- | ----------------------------------------------------- | ----------------------------------------------------------------------- |
| Migration format | Versioned plain SQL files                             | Changelogs in XML, YAML, JSON or SQL; database-independent change types |
| Rollback         | Forward-only (undo is a commercial feature)           | Rollback blocks                                                         |
| Fit here         | One database vendor; the SQL is the reviewed artifact | Its strengths (portability, rollback, diff) are not needed              |

Flyway runs in `event-processor`, the schema owner. Since Flyway 10, PostgreSQL support is a separate module
(`flyway-database-postgresql`).

**HikariCP.** Boot's default pool, and nothing here argues for another one. Pool size is a measured Stage 4
parameter. **Reconsider when** instance count × pool size approaches PostgreSQL's `max_connections`. Then add a
server-side pooler (PgBouncer in transaction mode) instead of shrinking pools.

### 2.5 Observability

**Micrometer versus the OpenTelemetry API.** Retrospective. Spring Boot instruments itself with Micrometer and its
documentation strongly recommends reporting metrics with Micrometer. Boot does not use the OpenTelemetry metrics SDK.
Choosing Micrometer does not close the door on OpenTelemetry: Micrometer can export over OTLP
(`micrometer-registry-otlp`), and tracing bridges to the OpenTelemetry SDK (3.5).

**Prometheus versus alternatives.** Retrospective.

| Criterion                  | **Prometheus, pull (chosen)**                       | OTLP push to an OpenTelemetry Collector     | VictoriaMetrics / Grafana Mimir  | Hosted (CloudWatch, Datadog, Grafana Cloud) |
| -------------------------- | --------------------------------------------------- | ------------------------------------------- | -------------------------------- | ------------------------------------------- |
| Local setup                | One container                                       | Collector plus a backend (often Prometheus) | One container (VictoriaMetrics)  | Account and agent; costs money              |
| Dead target                | Visible: `up == 0`                                  | A silent service just stops sending         | Same as Prometheus with scraping | Depends on the agent                        |
| Resolution for experiments | 5 s scrape interval, set per job                    | OpenTelemetry SDK default export: 60 s      | Same as Prometheus               | Varies; high resolution often costs extra   |
| Query language             | PromQL; `histogram_quantile` over published buckets | Backend-specific                            | PromQL-compatible                | Vendor-specific (CloudWatch) or PromQL      |
| Long-term storage, HA      | Not built in                                        | Backend-specific                            | Built in                         | Built in                                    |

- Experiments need short, controlled sampling and percentiles aggregated across instances. Both services publish
  histogram buckets (`percentiles-histogram`) so that `histogram_quantile` can combine instances, which client-side
  percentiles cannot.
- Pull makes a dead service visible, which matters for Stage 5 failure experiments.
- Prometheus 3 can also receive OTLP. A later move to push does not require a different backend.
- **Reconsider** at Stage 6. Amazon Managed Service for Prometheus keeps PromQL and the dashboards.

**Consumer lag: kafka-exporter versus alternatives.** Design-time for measuring lag outside the consumer.

| Option                                      | Measures lag when the consumer is dead | Notes                                                                                      |
| ------------------------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------ |
| **kafka-exporter (chosen)**                 | Yes                                    | Single binary, Prometheus metrics per group and partition; a community project             |
| KMinion                                     | Yes                                    | Also reads committed offsets; more features (end-to-end probes); also community-maintained |
| Burrow                                      | Yes                                    | Evaluates lag status over a window; its own HTTP API, needs an exporter for Prometheus     |
| Consumer client metrics (`records-lag-max`) | **No**                                 | Disappears exactly when lag matters most                                                   |

See [Q13](#6-open-questions-and-recommendations) for the maintenance risk.

**Logging.** The proposal planned Boot's built-in structured JSON logging (`logging.structured.format.console`,
no extra dependency, unlike `logstash-logback-encoder`). Stage 1 does not configure it
([Q7](#6-open-questions-and-recommendations)).

### 2.6 Testing and load generation

**Testcontainers versus embedded Kafka and H2.** Partly design-time (the proposal listed Testcontainers).

- The tests verify behaviour that only the real systems show: `acks=all` and broker failures (ADR-0002), the
  PostgreSQL upsert and `timestamptz` handling, Flyway migrations. Embedded Kafka runs a broker inside the test JVM,
  which is harder to stop, pause or cut off selectively than a container. H2 emulates PostgreSQL only partly.
- The cost is test time: about 3.5 minutes for `verify`.
- `spring-kafka-test` (embedded Kafka) is on the test classpath only because Boot 4's `spring-boot-starter-kafka-test`
  brings it. No test uses it. This is harmless.

**k6 versus Gatling and JMeter.** Retrospective.

| Criterion                       | **k6 (chosen)**                                   | Gatling                   | JMeter                                    |
| ------------------------------- | ------------------------------------------------- | ------------------------- | ----------------------------------------- |
| Open model (fixed arrival rate) | `constant-arrival-rate` executor                  | Open injection profiles   | Thread groups are closed-model by default |
| Generator saturation visible    | `dropped_iterations` (used in baseline Finding 2) | Via its own metrics       | Indirectly                                |
| Scripts                         | JavaScript                                        | Java, Kotlin or Scala DSL | XML test plans, usually built in a GUI    |

- An open model avoids coordinated omission: a slow server does not reduce the offered load. That is required for
  throughput and lag measurements.
- Gatling would be an equally sound choice, and its Java DSL might suit this audience better. k6 was kept because the
  baseline is already recorded with it.

## 3. Provisional choices (Stage 2 and later)

Stage 2 identity and rate limiting are ADRs: [ADR-0013](../adr/0013-jwt-without-identity-provider.md),
[ADR-0014](../adr/0014-shared-rate-limit-state.md) and [ADR-0004](../adr/0004-rate-limit-store-outage.md).

### 3.1 Resilience around Valkey

Design-time: "Don't add Resilience4j Retry: the Kafka client and Spring Kafka already retry, and stacking retries
multiplies load during an incident."

| Option                                               | What it provides                                                                  | Assessment                                                                            |
| ---------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **Resilience4j CircuitBreaker only (chosen)**        | Sliding-window failure and slow-call rates, half-open probing, Micrometer metrics | Short-circuits to the local limiter instead of waiting for a timeout on every request |
| Command timeout only, no breaker                     | Bounded wait per call                                                             | During an outage every request pays the full timeout before falling back              |
| Spring Cloud Circuit Breaker                         | One API over Resilience4j or Spring Retry                                         | An abstraction with a single implementation behind it; no benefit                     |
| Spring Framework 7 `@Retryable`, `@ConcurrencyLimit` | Retries and concurrency limits without a dependency                               | No circuit breaker; retries are excluded on purpose                                   |
| Resilience4j Retry, Bulkhead, TimeLimiter            | Retries, concurrency and time limits per call                                     | Retry excluded (see above); admission control is handled separately (3.2)             |

### 3.2 Admission control

Virtual threads remove Tomcat's thread limit, so Stage 1 has no admission control ([ADR-0009](../adr/0009-spring-mvc-virtual-threads.md),
[Q3](#6-open-questions-and-recommendations)). Stage 2 adds a `Semaphore` with `tryAcquire`: when no permit is free,
the API answers 503 with `Retry-After` immediately.

| Option                                   | Behaviour at the limit                            | Assessment                                                                                                           |
| ---------------------------------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **`Semaphore.tryAcquire` (chosen)**      | Immediate 503                                     | Bounds in-flight work and memory; the client backs off                                                               |
| Spring Framework 7 `@ConcurrencyLimit`   | Blocks the caller until a permit is free          | Design-time: it throttles rather than rejects. On virtual threads waiting is cheap, so the queue grows without bound |
| Tomcat `max-connections`, `accept-count` | Connections wait in the OS backlog or are refused | No response body, no `Retry-After`; clients see timeouts                                                             |
| nginx `limit_conn`                       | 503 per nginx instance                            | Counts connections, not in-flight work; useful as an outer guard                                                     |

The permit should be taken **before the request body is read**. Otherwise up to 1 MiB per request is already in
memory when the request is rejected. The permit count follows from a memory budget (permits × 1 MiB plus parsed
objects), not from CPU count.

### 3.3 Load balancing and container images

Retrospective.

- **nginx** in front of two API instances. It is enough locally and is replaced by a managed load balancer on AWS.
  Alternatives are HAProxy and Envoy (active health checks, richer metrics) and Spring Cloud Gateway (a Java gateway,
  see [section 5](#5-spring-cloud-project-by-project)). Open-source nginx has only passive health checks
  (`max_fails`, `fail_timeout`), so a stopped instance is detected by failing requests. Stage 5 experiments must
  account for that.
- **Dockerfiles** with Boot's layered jar (`java -Djarmode=tools`). They keep JVM flags and the base image explicit,
  which the thread and memory experiments need. Cloud Native Buildpacks (`spring-boot:build-image`) apply their own
  memory calculator, and Jib builds without a Docker daemon. Either would work but hides the settings under study.

### 3.4 Failure handling in the consumer (Stage 3)

Design-time plan: malformed records go to `events.v1.DLT` and are not retried. Transient database errors are retried
with a capped back-off and never go to the DLT.

| Option                                                         | Order within a partition | Transient database outage                       | Cost                                                  |
| -------------------------------------------------------------- | ------------------------ | ----------------------------------------------- | ----------------------------------------------------- |
| **Blocking retries, `DeadLetterPublishingRecoverer` (chosen)** | Kept                     | Partition waits; lag grows; nothing is lost     | A poison record blocks its partition until classified |
| Non-blocking retry topics (`@RetryableTopic`)                  | Lost for retried records | Records move through retry topics, then the DLT | More topics; a database outage fills the DLT          |
| Error table in PostgreSQL                                      | Kept                     | Cannot write the error either                   | Couples failure handling to the failing store         |

Correct classification is the hard part. **Implemented in Stage 3** (`KafkaErrorHandlingConfiguration`): malformed
records (`EnvelopeDecodingException`) and PostgreSQL data or integrity errors (`DataIntegrityViolationException`) go
to the DLT at once; transient database failures (connection, timeout, deadlock) are retried with back-off and no
limit; anything else is retried 10 times (≈ 2.5 min), then dead-lettered. The offset is not committed if the DLT
publish fails. Details: [delivery-guarantees.md](delivery-guarantees.md#processing-in-event-processor-stage-3).

### 3.5 Dashboards, profiling and tracing (Stages 4–5)

- **Grafana** (Stage 4), dashboards as code. It reads Prometheus and, in Stage 5, Tempo. The Prometheus UI alone is
  enough for ad-hoc queries but not for comparing runs.
- **JFR** (Stage 4): built into the JDK, low overhead, and shows virtual-thread and pinning events. async-profiler is
  optional for CPU flame graphs.
- **Toxiproxy** (Stage 5; named in the proposal's test stack): adds latency, bandwidth limits, timeouts and resets
  per connection, controlled from tests (Testcontainers module). `docker pause` only freezes a whole container.
  `tc netem` needs privileges inside the Docker VM and affects all traffic of an interface.
- **Tracing** (Stage 5): Micrometer Tracing with the OpenTelemetry bridge and OTLP exporter, which is the path Boot
  supports.

| Option                                                 | Assessment                                                                                                                                                          |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Micrometer Tracing + OpenTelemetry bridge (chosen)** | Uses Boot's observation of MVC, `KafkaTemplate` and listeners; context crosses Kafka in record headers                                                              |
| OpenTelemetry Java agent                               | No code, broad instrumentation; a second instrumentation layer next to Micrometer, community-supported with Boot                                                    |
| Backend: **Tempo** versus Jaeger or Zipkin             | Tempo fits Grafana (Stage 4) and stores traces in object storage; Jaeger and Zipkin bring their own UIs. Not decided ([Q15](#6-open-questions-and-recommendations)) |

### 3.6 Not yet assessed

- **AWS (Stage 6)** needs approval and a cost estimate. A separate ADR will choose between Amazon MSK, self-managed
  Kafka and Kinesis; RDS or Aurora; ElastiCache for Valkey; Cognito ([ADR-0013](../adr/0013-jwt-without-identity-provider.md)).
- **CI (publication stage):** GitHub Actions is assumed and has not been compared.

## 4. Deliberately excluded

The proposal excluded JPA, Schema Registry/Avro, Kafka Streams, Kafka transactions, Keycloak, Spring Cloud, WebFlux
and a separate query service in one line: "Each would add complexity without serving a stated capability." Kafka
Connect and Spring Cloud Stream were added in this review. The specific reasons are in the linked records.

| Technology                | What it would provide                                     | Why it is excluded here                                                                                                                           | Record                                                           |
| ------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| JPA / Hibernate           | Entity mapping, persistence context                       | No entities to manage; the hot path is a PostgreSQL upsert                                                                                        | [ADR-0007](../adr/0007-spring-jdbc-over-jpa.md)                  |
| Schema Registry, Avro     | Enforced schema compatibility, compact records            | One producer, one consumer, one repository; a registry is another service                                                                         | [ADR-0010](../adr/0010-json-envelope-without-schema-registry.md) |
| Kafka Streams             | Stateful stream processing, Kafka-to-Kafka exactly-once   | The aggregate is commutative; dedup must commit with the PostgreSQL update                                                                        | [ADR-0011](../adr/0011-consumer-postgres-aggregation.md)         |
| Kafka transactions        | Atomic multi-record writes for `read_committed` consumers | Coordinator round trips and fencing; client-retry duplicates remain                                                                               | [ADR-0002](../adr/0002-acceptance-semantics.md)                  |
| Kafka Connect (JDBC sink) | Topic-to-database without code                            | Its upsert replaces column values; it cannot express `event_count = event_count + 1`. A sink connector is still an option for a raw-event archive | [Q6](#6-open-questions-and-recommendations)                      |
| Keycloak                  | Token issuing, client credentials, key rotation           | Validation code is the same; an identity provider is another stateful service                                                                     | [ADR-0013](../adr/0013-jwt-without-identity-provider.md)         |
| Spring Cloud              | Several distinct capabilities                             | Assessed per project                                                                                                                              | [Section 5](#5-spring-cloud-project-by-project)                  |
| Spring Cloud Stream       | Broker-independent bindings                               | Hides the Spring Kafka settings this project studies                                                                                              | [2.3](#23-kafka-client-and-consumer-configuration)               |
| WebFlux                   | Non-blocking I/O with few threads                         | Blocking libraries; virtual threads give the same concurrency with blocking code                                                                  | [ADR-0009](../adr/0009-spring-mvc-virtual-threads.md)            |
| Separate query service    | Isolation of reads from consumption                       | No evidence yet that the isolation is needed; the split stays cheap                                                                               | [ADR-0006](../adr/0006-stats-endpoint-in-processor.md)           |

## 5. Spring Cloud, project by project

Spring Cloud is a set of independent projects. Release train 2025.1 ("Oakwood") supports Spring Boot 4.0 and, from
2025.1.2, 4.1, so compatibility is **not** the reason for leaving it out. Retrospective; the proposal excluded
"Spring Cloud" as a whole.

| Project                                         | Problem it solves                                                             | Does this system have the problem?                                                                         | What is used instead                                                                                 |
| ----------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Config (Server and Client)                      | Central, versioned configuration with refresh at runtime                      | No: two services, configuration changes go with deployments                                                | `application.yaml`, environment variables; a secrets store on AWS (Stage 6)                          |
| Gateway                                         | Routing, filters, rate limiting at an edge service                            | Partly: load balancing yes; limits per event count no ([ADR-0014](../adr/0014-shared-rate-limit-state.md)) | nginx plus Bucket4j in the API                                                                       |
| Netflix Eureka, Consul, Zookeeper, LoadBalancer | Service discovery and client-side load balancing for service-to-service calls | No: the services talk only through Kafka                                                                   | DNS names; nginx; a managed load balancer on AWS                                                     |
| OpenFeign                                       | Declarative HTTP clients                                                      | No outgoing HTTP calls                                                                                     | Spring's HTTP interface clients if ever needed                                                       |
| Circuit Breaker                                 | One API over several breaker libraries                                        | One breaker, one library                                                                                   | Resilience4j directly ([3.1](#31-resilience-around-valkey))                                          |
| Stream                                          | Broker-independent messaging bindings                                         | No: one broker, Kafka-specific semantics                                                                   | Spring for Apache Kafka ([2.3](#23-kafka-client-and-consumer-configuration))                         |
| Function                                        | Business logic as functions, independent of transport                         | No                                                                                                         | —                                                                                                    |
| Bus                                             | Broadcasting configuration changes and events over a broker                   | No (no Config Server)                                                                                      | —                                                                                                    |
| Task, Data Flow                                 | Short-lived tasks and their orchestration                                     | Barely: the Stage 3 retention job deletes expired ledger rows hourly                                       | A scheduled job inside `event-processor` (`LedgerRetentionJob`, Spring `@Scheduled`)                 |
| Kubernetes                                      | Configuration and discovery from Kubernetes                                   | No Kubernetes target                                                                                       | —                                                                                                    |
| Contract                                        | Consumer-driven contract tests and stubs for HTTP and messaging               | Not yet: producer and consumer share the `event-contract` module in one repository                         | Codec tests and integration tests ([ADR-0010](../adr/0010-json-envelope-without-schema-registry.md)) |
| Sleuth                                          | Distributed tracing for Boot 2                                                | Superseded                                                                                                 | Micrometer Tracing ([3.5](#35-dashboards-profiling-and-tracing-stages-45))                           |

**Reconsider** individual projects on their own triggers: Contract when a consumer outside this repository appears,
Gateway if limits become request-based, Config if several services need runtime configuration changes.

## 6. Open questions and recommendations

Findings from the documentation review. Q1, Q3, Q4 and Q12 were resolved in Stage 2; the others still need a
decision.

**Q1. A single event can block a partition.** By inspection, not reproduced
([ADR-0011](../adr/0011-consumer-postgres-aggregation.md)).

- `BatchValidator` accepts any finite `value`. Two values near `1e308` in one bucket overflow `double precision`,
  and PostgreSQL raises SQLSTATE 22003.
- More generally, the error handler retries every exception except `EnvelopeDecodingException` forever. Any
  permanent database error (a data exception, a constraint violation) stops the partition and every tenant on it.
- **Recommendation:** bound `value` in validation now, with a test. In Stage 3, classify SQLSTATE class 22 (data
  exceptions) and 23 (integrity violations) as non-retryable. Consider fixing the bound before Stage 2, because the
  API is the attack surface.
- **Resolved in Stage 2 (temporary):** `value` must lie within ±1e12. The consumer does not retry
  `DataIntegrityViolationException` (PostgreSQL data and integrity errors such as 22003). Such a record is logged,
  counted as `processor.events.skipped{reason="unprocessable"}` and skipped. **The event is lost** until Stage 3
  adds the dead-letter topic.
- **Resolved in Stage 3:** such records go to `events.v1.DLT` (`processor.events.dead_lettered`), and only
  transient failures are retried without limit.

**Q2. A `schemaVersion` change can lose data silently** ([ADR-0010](../adr/0010-json-envelope-without-schema-registry.md)).
If producers are deployed before consumers, v2 records are skipped as malformed. **Recommendation:** document
"consumers first", alert on `processor.events.skipped > 0`, and treat "valid envelope, newer version" as a retryable
or DLT case rather than as malformed. **Partly resolved in Stage 3:** v2 records go to the DLT
(`processor.events.dead_lettered{reason="malformed"}`) and can be redriven after the consumers are upgraded; the
alert and the retryable "newer version" case remain open.

**Q3. Stage 1 has no admission control** ([ADR-0009](../adr/0009-spring-mvc-virtual-threads.md)). With virtual
threads, only `max-connections` (8,192) and the producer buffer bound concurrency. During a broker stall, memory use
can reach thousands × 1 MiB. **Recommendation:** Stage 2 as planned, with the permit taken before the body is read
([3.2](#32-admission-control)). **Resolved in Stage 2:** a servlet filter ahead of Spring Security takes one of
`ingestion.max-in-flight` (256) permits before the body is read and holds it until the Kafka acknowledgement; with
no permit free the API answers 503 with `Retry-After`.

**Q4. Is Valkey needed?** ([ADR-0014](../adr/0014-shared-rate-limit-state.md)) During an outage the system already
runs on local buckets, and quotas are allowed to be exceeded ([ADR-0004](../adr/0004-rate-limit-store-outage.md)).
Valkey's concrete benefits are a daily quota that survives restarts and accuracy under uneven load balancing.
**Recommendation:** confirm before Stage 2 that these, or the learning goal, justify a stateful dependency on the
hot path. **Resolved:** Valkey kept at the Stage 2 approval, with the ADR-0004 local fallback.

**Q5. Tenant isolation has one line of defence** ([ADR-0012](../adr/0012-pooled-multi-tenancy.md)).
**Recommendation:** in Stage 2, add RLS on the stats read path with a separate role, or at least a test that fails
when a query on `event_stats_minute` lacks the tenant predicate.

**Q6. There is no raw-event store** ([ADR-0008](../adr/0008-kafka-buffer-two-deployables.md),
[ADR-0011](../adr/0011-consumer-postgres-aggregation.md)). New dimensions cannot be backfilled beyond 7 days, and
corrupted aggregates cannot be rebuilt after that. **Recommendation:** decide before Stage 3 whether this is a
requirement. If it is, add an archive sink (object storage through Kafka Connect, or a raw table) and revisit
[ADR-0005](../adr/0005-deduplication-retention.md), which depends on retention.

**Q7. Structured logging was planned but is not configured.** **Recommendation:** add
`logging.structured.format.console` in Stage 2, when two API instances behind nginx make log correlation necessary.

**Q8. Virtual threads in `event-processor` have no measurable purpose.** Three listener threads and a pool of 10 do
not need them. **Recommendation:** keep them, but include the processor in the Stage 4 thread experiment and stop
claiming a benefit there.

**Q9. Management endpoints share the application port.** Already noted in configuration as a Stage 1 limitation.
**Recommendation:** set `management.server.port` with the Stage 2 Dockerfiles, and make sure nginx does not route
`/actuator`.

**Q10. `value_sum` is `double precision`.** Sums are approximate and depend on addition order. **Recommendation:**
decide whether `value` can ever be used for billing. If so, change to `numeric` or integer units before data
accumulates.

**Q11. Tenant erasure is bounded by Kafka retention** ([ADR-0012](../adr/0012-pooled-multi-tenancy.md)). Events stay
in `events.v1` up to 8 days after deletion, and a replay recreates aggregates. **Recommendation:** record erasure as
a non-goal for now, or choose shorter retention or per-tenant encryption.

**Q12. JWTs cannot be revoked, and the plan is fixed for a token's lifetime**
([ADR-0013](../adr/0013-jwt-without-identity-provider.md)). A plan change takes effect only when the token expires.
**Recommendation:** short token lifetimes. Consider resolving the plan from configuration by tenant instead of from
a claim. Decide API keys versus JWT before Stage 2. **Resolved in Stage 2:** JWT kept; the plan comes from
configuration by tenant (`rate-limit.tenants`), so only revocation remains unavailable.

**Q13. kafka-exporter is a community project with a small maintainer base.** It is the only lag source.
**Recommendation:** check compatibility on each Kafka upgrade; KMinion is the fallback. No action now.

**Q14. `group.protocol: classic` while KIP-848 is generally available.** **Recommendation:** keep it in the Stage 4
consumer-scaling experiment, as planned.

**Q15. Tempo versus Jaeger is undecided.** **Recommendation:** Tempo, because Grafana is already planned and one UI
can link metrics and traces. Decide in Stage 5.

**Q16. The cost of JSON is unmeasured** ([ADR-0010](../adr/0010-json-envelope-without-schema-registry.md), 2.2).
**Recommendation:** in Stage 4 profiling, report the CPU share of request parsing, envelope encoding and decoding.
This tests two decisions at little cost.

## 7. References

Technologies covered by an ADR are referenced there.

- Spring Boot virtual threads:
  https://docs.spring.io/spring-boot/reference/features/spring-application.html#features.spring-application.virtual-threads
- Spring Boot observability and metrics: https://docs.spring.io/spring-boot/reference/actuator/observability.html,
  https://docs.spring.io/spring-boot/reference/actuator/metrics.html
- Spring Boot structured logging: https://docs.spring.io/spring-boot/reference/features/logging.html
- Spring Framework resilience features (`@Retryable`, `@ConcurrencyLimit`):
  https://docs.spring.io/spring-framework/reference/core/resilience.html
- Spring for Apache Kafka: https://docs.spring.io/spring-kafka/reference/ (error handling:
  https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)
- Spring Cloud Stream: https://docs.spring.io/spring-cloud-stream/reference/
- Spring Cloud projects and release trains: https://spring.io/projects/spring-cloud
- KIP-848, consumer rebalance protocol:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
- Quarkus: https://quarkus.io/guides/; Micronaut: https://docs.micronaut.io/latest/guide/
- JEP 483, Ahead-of-Time Class Loading & Linking: https://openjdk.org/jeps/483; JEP 515, Ahead-of-Time Method
  Profiling: https://openjdk.org/jeps/515
- Maven Wrapper: https://maven.apache.org/tools/wrapper/; Gradle: https://docs.gradle.org/current/userguide/userguide.html
- gRPC: https://grpc.io/docs/; RFC 9457, problem details: https://www.rfc-editor.org/rfc/rfc9457
- Flyway: https://documentation.red-gate.com/flyway; Liquibase: https://docs.liquibase.com/
- HikariCP pool sizing: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing; PgBouncer:
  https://www.pgbouncer.org/
- Micrometer: https://docs.micrometer.io/micrometer/reference/; Micrometer Tracing:
  https://docs.micrometer.io/tracing/reference/
- Prometheus: https://prometheus.io/docs/introduction/overview/; histograms:
  https://prometheus.io/docs/practices/histograms/; OTLP ingestion: https://prometheus.io/docs/guides/opentelemetry/
- OpenTelemetry Collector: https://opentelemetry.io/docs/collector/; Java agent:
  https://opentelemetry.io/docs/zero-code/java/agent/
- kafka-exporter: https://github.com/danielqsj/kafka_exporter; KMinion: https://github.com/redpanda-data/kminion;
  Burrow: https://github.com/linkedin/Burrow
- Grafana: https://grafana.com/docs/grafana/latest/; Tempo: https://grafana.com/docs/tempo/latest/; Jaeger:
  https://www.jaegertracing.io/docs/
- Testcontainers for Java: https://java.testcontainers.org/; Spring Kafka testing:
  https://docs.spring.io/spring-kafka/reference/testing.html
- k6 open and closed models: https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/;
  Gatling: https://docs.gatling.io/; JMeter: https://jmeter.apache.org/
- Toxiproxy: https://github.com/Shopify/toxiproxy
- nginx upstream health checks (`max_fails`): https://nginx.org/en/docs/http/ngx_http_upstream_module.html
