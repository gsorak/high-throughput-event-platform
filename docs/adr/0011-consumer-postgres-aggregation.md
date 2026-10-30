# ADR-0011: Aggregate in a plain Kafka consumer into PostgreSQL

- Status: Accepted (2026-10-27)
- Stage: 1 (implemented; batching is a Stage 4 experiment)
- Assessment: **Partly retrospective.** The proposal ruled out Kafka Streams, noting that its exactly-once processing
  covers only Kafka-to-Kafka flows. Flink and analytical databases were not considered. Two risks below were found
  during this review.

## Problem

- Count events and sum `value` per `(tenant, type, minute)`, and serve ranges of these aggregates per tenant.
- Late and duplicate events must be handled. `occurredAt` may be up to 7 days in the past, and delivery is
  at-least-once ([ADR-0002](0002-acceptance-semantics.md)).
- Stage 3 deduplicates by `eventId` and needs the ledger insert and the aggregate update to commit or roll back
  together ([ADR-0005](0005-deduplication-retention.md)).

## Decision

- A Spring Kafka `@KafkaListener` with concurrency 3 and batch acknowledgement. For each record it runs one upsert:
  `INSERT … ON CONFLICT (tenant_id, bucket_start, event_type) DO UPDATE SET event_count = event_count + 1,
value_sum = value_sum + EXCLUDED.value_sum` (see `EventStatsRepository`).
- The table `event_stats_minute` (`value_sum double precision`) is both the state and the query model.
  `GET /v1/stats` reads it directly ([ADR-0006](0006-stats-endpoint-in-processor.md)).
- No stream-processing framework and no separate analytics store.

## Alternatives considered

1. **Kafka Streams.** A windowed aggregation in a local state store (RocksDB) backed by a changelog topic, then a
   sink to PostgreSQL or interactive queries.
2. **Apache Flink.** A keyed window aggregation with checkpoints. Exactly-once delivery to PostgreSQL would need a
   two-phase-commit or idempotent sink.
3. **An analytical store instead of hand-written upserts.** Examples: ClickHouse (ingest raw events, aggregate with
   materialized views) or TimescaleDB on PostgreSQL (hypertable of raw events plus continuous aggregates).

## Comparison

| Criterion                                 | **Consumer + PostgreSQL upsert**               | Kafka Streams                                                          | Flink                                             | ClickHouse / TimescaleDB                                                     |
| ----------------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------- |
| Late events (up to 7 days)                | Trivial: the upsert is commutative             | Windows plus a 7-day grace period → large state                        | Watermarks plus allowed lateness → large state    | Raw rows; aggregates are recomputed or merged                                |
| Stage 3 dedup together with the aggregate | One PostgreSQL transaction                     | EOS covers Kafka → Kafka only; the PostgreSQL sink needs its own dedup | Needs a 2PC or idempotent sink                    | ClickHouse dedup is eventual (ReplacingMergeTree); TimescaleDB as PostgreSQL |
| Raw events retained                       | No                                             | No (changelog holds state, not events)                                 | No                                                | **Yes**                                                                      |
| New infrastructure                        | None                                           | None (library), plus internal topics                                   | A cluster (JobManager, TaskManagers, checkpoints) | A new database (ClickHouse) or an extension (TimescaleDB)                    |
| Measured throughput                       | About 1,400 events/s, one round trip per event | Not measured                                                           | Not measured                                      | Not measured                                                                 |

## Rationale

- **The aggregate is commutative and associative:** count and sum. Order, windows and watermarks do not matter, so
  the main features of stream processors bring no benefit here, while their late-event state would be large.
- **One transaction for dedup and update** (Stage 3) is the simplest correct design for exactly-once effects on
  at-least-once input. Kafka transactions do not extend to PostgreSQL.
- PostgreSQL already serves the query side. One store means one backup and one consistency model.
- The throughput limit is understood: one auto-committed statement per event. It is addressed by batching (Stage 4),
  not by a framework.

## Consequences

- Throughput is limited by database round trips until Stage 4 batches the writes.
- **A partition-blocking poison event is possible through the public API.** It was found by inspection and not
  reproduced.
  - `BatchValidator` accepts any finite `value`.
  - Two events near `1e308` in the same bucket make `value_sum + EXCLUDED.value_sum` overflow `double precision`.
    PostgreSQL raises an error (SQLSTATE 22003) instead of storing infinity.
  - The error handler retries every exception except `EnvelopeDecodingException` without limit. The partition stops,
    and so does every tenant hashed to it.
  - See [technology-selection.md, Q1](../architecture/technology-selection.md#6-open-questions-and-recommendations).
- **`double precision` sums are approximate.** The result depends on the order of additions, so a replay can differ
  in the last bits. If `value` is ever used for billing, it needs `numeric` or integer units (Q10).
- Only aggregates are stored. A new dimension cannot be backfilled beyond the 7-day topic retention
  ([ADR-0008](0008-kafka-buffer-two-deployables.md)).

## Reconsider when

- Aggregations become order-dependent (sessions, sequences, joins across streams). Then use Kafka Streams or Flink,
  and revisit the partition key ([ADR-0003](0003-partition-key.md)).
- Ad-hoc or high-cardinality queries over raw events are required, or the 7-day backfill limit becomes a problem.
  Then add an analytical store. TimescaleDB keeps the PostgreSQL operating model; ClickHouse scales further but adds
  a second database.
- Stage 4 batching does not reach the target rate.

## References

- PostgreSQL `INSERT … ON CONFLICT`: https://www.postgresql.org/docs/17/sql-insert.html
- PostgreSQL floating-point types (overflow raises an error):
  https://www.postgresql.org/docs/17/datatype-numeric.html
- Kafka Streams: https://kafka.apache.org/documentation/streams/
- Spring Kafka transactions (scope of Kafka EOS): https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Apache Flink: https://flink.apache.org/
- ClickHouse: https://clickhouse.com/docs
- TimescaleDB: https://github.com/timescale/timescaledb
