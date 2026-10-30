# Testing

Tests demonstrate behaviour, including failure behaviour and known limitations. A limitation that is not fixed yet
has a test that pins the _current_ behaviour; its name says so, and it is flipped when the fix lands.

## Running

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 25)   # macOS; any JDK 25 works
./mvnw verify                                        # everything; needs Docker for the integration tests
./mvnw test                                          # unit and slice tests only, no Docker
./mvnw -pl ingestion-api -am verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dit.test=PartialPublishFailureIT   # one integration test
```

`-pl <module> -am` builds the module together with the modules it depends on (`event-contract`). Without `-am`, Maven
looks for `event-contract` in the local repository, where nothing was installed.

- **surefire** runs `*Test` classes in `mvn test`.
- **failsafe** runs `*IT` classes in `mvn verify`, after packaging.
- **promtool** checks the PromQL of the demo's System overview against synthetic series (counter resets, the first
  429s of a new series, uncommitted partitions as kafka-exporter reports them, missing metrics, rates after a burst).
  `PromqlRegressionTest` runs it as part of `./mvnw test`: with `promtool` from the PATH, else in the
  `prom/prometheus:v3.15.1` image if that is already pulled (no download). If neither exists the test is **skipped**,
  with a message on stderr and in the surefire report, so a green build does not prove the PromQL ran. Set
  `PROMTOOL_REQUIRED=true` to fail instead (CI does). `SystemMetricsTest` and `LagQueryConsistencyTest` assert that the
  file, the scripts and the Grafana dashboard hold the gateway's queries. To run it alone:

  ```sh
  docker run --rm -v "$PWD/demo-gateway/src/test/promql:/t:ro" --entrypoint promtool \
    prom/prometheus:v3.15.1 test rules /t/overview.test.yml
  ```

## Kinds of tests

| Kind                                                  | Example                                       | Starts                                                                  | Use for                                                                                                       |
| ----------------------------------------------------- | --------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Plain unit test                                       | `EventEnvelopeCodecTest`                      | nothing                                                                 | Pure logic: encoding, decoding, rejection of malformed input.                                                 |
| Web slice (`@WebMvcTest`)                             | `EventsControllerTest`, `StatsControllerTest` | MVC layer only; collaborators are `@MockitoBean`s                       | Request validation, status codes, `application/problem+json` bodies. Fast, no Docker.                         |
| Integration test (`@SpringBootTest` + Testcontainers) | `EventIngestionIT`, `EventAggregationIT`      | Whole application on a random port, real Kafka and PostgreSQL in Docker | Behaviour that depends on the real broker or database: acknowledgements, keys, partial failure, outages, SQL. |

## Libraries

- **JUnit 6 (Jupiter)**: `@Test`, lifecycle, parameterized tests. Same programming model as JUnit 5.
- **AssertJ**: `assertThat(actual).isEqualTo(expected)`, with readable failure messages.
- **MockMvcTester** (Spring 6.2+): AssertJ-style assertions on MockMvc,
  e.g. `assertThat(post(...)).hasStatus(HttpStatus.BAD_REQUEST).bodyJson()...`. Used in the slice tests.
- **Mockito** through `@MockitoBean` / `@MockitoSpyBean`: replace or wrap one Spring bean. `PartialPublishFailureIT`
  wraps the real `KafkaTemplate` and makes only the third `send` fail, so the other records really reach Kafka.
- **Testcontainers**: starts Docker containers from tests. The containers are `@Bean`s in a `@TestConfiguration`
  ([`ProcessorTestcontainersConfiguration`](../../event-processor/src/test/java/io/highthroughput/processor/ProcessorTestcontainersConfiguration.java)).
  `@ServiceConnection` points Spring Boot at them without property overrides. The images are the same versions as
  `infra/local/compose.yaml`.
- **Awaitility**: `await().atMost(...).untilAsserted(...)` for asynchronous outcomes (a record being consumed and
  aggregated) without fixed sleeps.

## Things worth knowing

- **Context caching**: Spring reuses an application context, and so its containers, across test classes with the same
  configuration. Tests therefore use a unique tenant or marker event per test instead of assuming an empty topic or
  table. `KafkaUnavailableIT` stops its broker, so it is marked `@DirtiesContext` and gets a context of its own.
- **Reading a topic in a test**: [`TopicContents`](../../ingestion-api/src/test/java/io/highthroughput/ingest/TopicContents.java)
  assigns all partitions, seeks to the beginning and reads up to the end offsets taken at the start. It does not use a
  consumer group, so there is no rebalance wait and no committed state.
- **HTTP in integration tests** uses the JDK `HttpClient`; `TestRestTemplate` is not available in Boot 4.
- **First run is slow**: Docker pulls `apache/kafka` and `postgres` once (about 75 s here). Later runs take ~50 s for
  `ingestion-api` and ~30 s for `event-processor`.

## Tests that pin current limitations

| Test                                                                      | Shows                                                                                                                                                                                                  | Changes in                                                                                                    |
| ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `PartialPublishFailureIT` (2 tests)                                       | A failed batch answers 503, some records are already in Kafka, and the client retry writes them again. Two shapes: broker-side failure (the rest were sent) and immediate rejection (sending stopped). | The Kafka-side duplicates remain by design; the processor counts each `eventId` once (`DuplicateDeliveryIT`). |
| `DuplicateDeliveryIT.duplicatesAreSkippedOnlyWhileTheLedgerRowIsRetained` | A duplicate is skipped while its ledger row exists (20 days) and counted again after the retention job deleted it (22 days).                                                                           | Tradeoff (ADR-0005).                                                                                          |

Stage 3 flipped `DuplicateDeliveryIT` to "counted once" (same event, retried batch, redelivery after restart).
`DeadLetterIT` covers the dead-letter topic: malformed and unprocessable records, a failed DLT publish (the test
deletes the DLT, checks that the offset stays uncommitted, then recreates it) and a 12 s table lock that is retried
until it succeeds. `KafkaErrorHandlingConfigurationTest` checks the classification with real PostgreSQL SQLSTATEs,
without a database.
