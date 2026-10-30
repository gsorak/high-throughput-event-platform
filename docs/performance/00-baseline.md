# Performance baseline (Stage 1): single-run laptop measurements

> Recorded 2026-10-27. **Single-run laptop baselines.** Every configuration was run once, on one laptop that also ran
> the load generator, Kafka and PostgreSQL. The numbers describe this code on this machine and are the reference point
> for the Stage 4 experiments (≥ 3 runs each, median and spread). They are not a capacity statement.
>
> **Superseded for end-to-end capacity** by [01-after-stage-4.md](01-after-stage-4.md): with the dedup ledger (Stage 3)
> the processor drains ~470–500 events/s and sustains 400 events/s on this laptop.

## Summary: two different throughputs

| Throughput                            | Definition                                                                     | Result                                                                      |
| ------------------------------------- | ------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| **Ingestion API**                     | Events/s accepted into Kafka (202 after `acks=all`), processor stopped         | ≥ 10,000 events/s at p99 81 ms, 0 errors; the API's ceiling was not reached |
| **Sustainable end-to-end processing** | Highest offered rate at which lag stays bounded and drains when the load stops | 1,000 events/s (warm JVMs); 2,000 events/s is not sustainable               |

- **Ingestion API**: at 20,000 events/s offered, k6 saturated the Docker VM before the API did (Finding 2). A real
  API ceiling needs the load generator on another machine.
- **Sustainable end-to-end**: at 1,000 events/s the lag peaked at 1,280 events and end-to-end p99 was 2.0 s. At 2,000
  the processor peaked at 1,408 events/s and the lag reached 145,900. The limit lies between 1,000 and ~1,400
  events/s; rates in between were not measured.

Both are single runs on one laptop. **The platform's capacity is the second number**; the first only says how fast
events can be put into Kafka. If clients sent 10,000 events/s with the processor running, the backlog would grow by
~8,600 events/s and end-to-end latency with it. Closing that gap is the first Stage 4 experiment (Finding 1).

## What was measured

- **End-to-end runs** (sustainable processing throughput): API and processor both running. Did the processor keep up,
  and what did that cost in latency?
- **Ingestion-only runs** (ingestion API throughput): processor stopped. How many events/s does `POST /v1/events`
  accept into Kafka on its own? Nothing processes them, so this is not platform capacity.

Load model: k6 `constant-arrival-rate` (open model). k6 starts _rate_ requests per second whatever the response time,
so a slow server shows up as latency and dropped iterations instead of silently lowering the load
(see [load-tests/README.md](../../load-tests/README.md)). Each request carries a batch of UUIDv7 events for one of
50 tenants chosen at random. Event types are uniform over 4 values. A request is ~145 bytes per event on the wire;
an encoded envelope in Kafka is ~225 bytes.

## Environment

|                |                                                                                                           |
| -------------- | --------------------------------------------------------------------------------------------------------- |
| Host           | MacBook Pro, Intel i7-4870HQ (4 cores / 8 threads, 2.5 GHz), 16 GB RAM, macOS 14.8.9                      |
| Docker         | Docker Desktop 29.7.2, VM with 8 CPUs and 7.75 GiB RAM (Kafka, PostgreSQL, Prometheus, k6 all inside)     |
| JVM            | Amazon Corretto 25.0.5, defaults (G1, max heap 4 GiB = 1/4 of RAM), apps run on the host with `java -jar` |
| Versions       | Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, HikariCP 7.0.2, PostgreSQL JDBC 42.7.14       |
| Kafka          | `apache/kafka:4.3.2`, one KRaft broker, `events.v1` with 12 partitions, RF 1                              |
| PostgreSQL     | `postgres:17.11`, default configuration (`synchronous_commit = on`)                                       |
| Load generator | `grafana/k6:2.3.0` in Docker, targeting `host.docker.internal:8080`                                       |

Relevant application settings (all in the two `application.yaml` files): producer `acks=all`, idempotence on,
`linger.ms=5`, no compression; listener record-at-a-time, 3 consumer threads, offsets committed per poll; one
auto-committed `INSERT … ON CONFLICT DO UPDATE` per event; Hikari pool of 10.

Two changes landed after these runs and do not affect steady-state numbers: `ProducerWarmUp` (fetches topic metadata at
startup; before it, the first request after each start was a 503) and silencing `LoggingProducerListener`.

**Everything shares one laptop.** k6, the broker and the database compete with the services for the same CPUs, and
k6 reaches the API through Docker Desktop's port forwarding.

## Results — end-to-end (sustainable processing throughput)

One run per row.

| Offered events/s (req/s × batch) | HTTP p50 / p95 / p99 (ms) | Errors | Dropped iterations | Peak processed events/s | Max lag (events) | Drain after load | End-to-end p50 / p99 | Upsert p99 | Hikari pending (max) |
| -------------------------------- | ------------------------- | ------ | ------------------ | ----------------------- | ---------------- | ---------------- | -------------------- | ---------- | -------------------- |
| 250 (25 × 10)                    | 11.1 / 23.3 / 68.6        | 0      | 0                  | 253                     | 100              | —                | 24 ms / 332 ms       | 21 ms      | 0                    |
| 500 (50 × 10)                    | 8.8 / 12.4 / 15.9         | 0      | 0                  | 501                     | 50               | —                | 17 ms / 84 ms        | 7 ms       | 0                    |
| 1,000 (50 × 20)                  | 9.6 / 13.8 / 19.2         | 0      | 0                  | 1,022                   | 1,280            | —                | 156 ms / 2.0 s       | 8 ms       | 0                    |
| 1,000 (50 × 20), **cold JVMs**   | 13.5 / 76.0 / 171.4       | 0      | 0                  | 984                     | 49,720           | 56 s             | not valid ¹          | 23 ms      | 0                    |
| 2,000 (100 × 20)                 | 11.3 / 197.9 / 487.1      | 0      | 401 ²              | **1,408**               | **145,900**      | **127 s**        | 87 s / 157 s         | 17 ms      | 0                    |

Sustained: 250, 500 and 1,000 events/s on warm JVMs (lag bounded, drained within one 5 s scrape after the load).
Not sustained: 2,000 events/s (lag grew for the whole run) and 1,000 events/s on cold JVMs (Finding 4).

In every end-to-end run, events accepted = events aggregated (e.g. 231,980 = 231,980). No publish failed, so no
duplicates were created (see [delivery-guarantees.md](../architecture/delivery-guarantees.md)).

¹ Micrometer's timer histogram stopped at 30 s by default, so every slower event fell into the `+Inf` bucket and
`histogram_quantile` reported exactly 30 s. Fixed before the other runs (`maximum-expected-value: 30m` for
`processor.end.to.end.latency`). This is why observability had to be checked _before_ tuning.
² k6 had 50 pre-allocated VUs (max 200). When responses slowed it could not start every request on time.

## Results — ingestion API only (processor stopped; not platform capacity)

One run per row. Every accepted event stayed in Kafka as backlog.

| Offered events/s (req/s × batch) | HTTP p50 / p95 / p99 / max (ms) | Errors        | Dropped iterations | Accepted events/s | Publish p99 | API CPU (cores) |
| -------------------------------- | ------------------------------- | ------------- | ------------------ | ----------------- | ----------- | --------------- |
| 2,000 (100 × 20)                 | 8.4 / 11.6 / 15.5 / 153         | 0             | 0                  | 2,000             | 12 ms       | 0.14            |
| 5,000 (100 × 50)                 | 8.6 / 15.0 / 71.4 / 469         | 0             | 0                  | 5,000             | 38 ms       | 0.19            |
| 10,000 (200 × 50)                | 11.1 / 37.3 / 80.8 / 593        | 0             | 0                  | 9,999             | 49 ms       | 0.34            |
| 20,000 (200 × 100)               | 75.5 / 1,839 / 3,480 / 12,571   | 12 (0.08 %) ³ | 8,641              | 12,586            | 168 ms      | 0.36            |

³ All 18,550 requests that reached the API in that window returned 202. The 12 failures were client-side connection
errors (status 0) inside k6.

CPU is `process_cpu_usage` × 8 logical CPUs, averaged over the run window.

## Findings

1. **The processor saturates at about 1,400 events/s, and it is not CPU- or pool-bound.** At 2,000 events/s the lag
   grew to 145,900 events and took 127 s to drain, while the processor used ~0.2 cores and no thread ever waited for a
   Hikari connection. 1,400 events/s over 3 threads is ~2.1 ms per event per thread. That matches one database round
   trip and one WAL flush per event: each upsert is its own auto-committed transaction.
   _Hypothesis for Stage 4:_ a batch listener that writes one transaction per poll, with rows pre-aggregated per
   `(tenant, minute, type)`, removes most round trips and flushes.
2. **The API is not the limit on this machine.** It accepted 10,000 events/s with p99 81 ms and no errors, using about
   a third of a core. At 20,000 events/s offered, k6 itself used 825 % CPU (all of the Docker VM) with the host 5 %
   idle. The latency and dropped iterations in that run measure the load generator, not the API. A real API ceiling
   needs k6 on a separate machine.
3. **The publish bound holds under load.** Publish p99 stayed ≤ 168 ms everywhere, far inside the 2.5 s
   `ingestion.publish-timeout`.
4. **Cold JVMs matter.** The first 1,000 events/s run (fresh processor) lagged by up to 49,720 events. The same load
   on a warm processor peaked at 1,280. Measured runs must follow a warm-up. Stage 4's `scripts/bench/run.sh` should
   include one.
5. **A backlog slows the API.** At 2,000 events/s the API's p95 was 198 ms with the processor catching up, and 12 ms
   with the processor stopped. The likely cause is the single broker and the Docker VM serving the consumer's catch-up
   fetches alongside the produce traffic. _Not yet proven_: Stage 4 should compare broker request latency
   (`kafka_producer_request_latency_avg`) and VM CPU in both cases.
6. **End-to-end latency includes queueing.** At 1,000 events/s, p99 end-to-end was 2.0 s while upsert p99 was 8 ms. Per-event
   work is fast; the time is spent waiting in the partition behind other events.

## Reproduce

```sh
docker compose -f infra/local/compose.yaml up -d
java -jar event-processor/target/event-processor-0.1.0-SNAPSHOT.jar   # terminal 1
java -jar ingestion-api/target/ingestion-api-0.1.0-SNAPSHOT.jar       # terminal 2
load-tests/run.sh warmup 50 20 60s                                    # discard
load-tests/run.sh e2e-1000 50 20 120s
ALLOW_LAG=1 DRAIN_TIMEOUT_SECONDS=1 VUS=200 load-tests/run.sh ingest-10000 200 50 120s   # processor stopped
```

Raw output (k6 summary JSON + server-side metrics) is written to `load-tests/results/`, which is not committed.

The ingestion-only runs leave a backlog in `events.v1` (≈ 4.2 M events after this baseline). Remove it from the
processor's path before the next end-to-end run: stop the processor, run `scripts/kafka-reset.sh skip`, start it again.
`run.sh` refuses to start while the consumer lag is not 0. The full procedure, including deleting the records
instead, is in [load-tests/README.md](../../load-tests/README.md#between-experiments). Do not use `down -v` for this:
it also deletes the PostgreSQL aggregates and the Prometheus history.
