# ADR-0008: Kafka as the durable buffer between two deployables

- Status: Accepted (2026-10-27)
- Stage: 1 (implemented)
- Assessment: **Retrospective.** Kafka and the split into `ingestion-api` and `event-processor` were given by the
  project brief. Nobody compared alternatives when they were chosen. The comparison below was written afterwards and
  does not change the decision.

## Problem

- The API answers 202 only when events are durable ([ADR-0002](0002-acceptance-semantics.md)). That must not depend on
  how fast events are aggregated.
- Aggregation is much slower than acceptance. The baseline accepted at least 10,000 events/s into Kafka but processed
  only about 1,400 events/s ([00-baseline.md](../performance/00-baseline.md)).
- A slow or unavailable PostgreSQL must not stop ingestion ([ADR-0006](0006-stats-endpoint-in-processor.md)).
- Recovery from a bug or a database restore needs to reprocess recent events ([ADR-0005](0005-deduplication-retention.md)).

## Decision

- Apache Kafka in KRaft mode. Topic `events.v1` has 12 partitions, key `tenantId`
  ([ADR-0003](0003-partition-key.md)), `retention.ms` 7 days and `segment.ms` 1 day.
- Two deployables:
  - `ingestion-api` produces and has no database.
  - `event-processor` consumes and owns PostgreSQL.
  - They share only the `event-contract` library ([ADR-0010](0010-json-envelope-without-schema-registry.md)).
- Locally there is one broker with replication factor 1. Replication factor 3 with `min.insync.replicas=2` is planned
  for Stage 5/6.

## Alternatives considered

1. **No broker.** The API writes events or aggregates to PostgreSQL and answers 202 after the commit.
2. **PostgreSQL as the queue.** The API inserts raw events into a table. Workers claim rows with
   `SELECT … FOR UPDATE SKIP LOCKED`.
3. **RabbitMQ.** Either quorum queues, where consuming deletes the message, or streams: an append-only log with replay,
   partitioned through super streams.
4. **Kafka-compatible or similar logs.** Redpanda (Kafka API, single binary, source-available licence) or Apache
   Pulsar (brokers plus a separate BookKeeper storage layer).
5. **One deployable.** Keep Kafka but run the API and the consumer in the same JVM.

## Comparison

| Option                     | Ingestion during a PostgreSQL outage | Processor slowness becomes … | Replay after a fix             | Main cost                                                                           |
| -------------------------- | ------------------------------------ | ---------------------------- | ------------------------------ | ----------------------------------------------------------------------------------- |
| **Kafka, two deployables** | Continues                            | Consumer lag                 | Yes, within retention (7 days) | A broker to operate; at-least-once delivery end to end; a wire contract to version  |
| No broker                  | Stops                                | Request latency, then errors | No                             | Ingestion availability equals database availability; peak rate equals DB write rate |
| PostgreSQL as queue        | Stops (same database)                | Queue depth                  | Yes, while rows are kept       | High-churn table (vacuum, bloat); one primary carries both ingest and aggregation   |
| RabbitMQ quorum queues     | Continues                            | Queue depth                  | No; consumed messages are gone | No replay, so the recovery in ADR-0005 does not work                                |
| RabbitMQ streams           | Continues                            | Consumer offset gap          | Yes                            | Partitioning (super streams) and Java tooling are less established than Kafka's     |
| Redpanda                   | Continues                            | Consumer lag                 | Yes                            | Same client code; licence terms and vendor dependency                               |
| Pulsar                     | Continues                            | Backlog                      | Yes                            | Brokers plus a storage cluster: more moving parts for one topic                     |

| Deployable split | Benefit                                                                                                                                           | Cost                                                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Two (chosen)** | The API has no database driver, pool or migrations. API and processor scale separately. A processor crash or redeploy does not affect acceptance. | Two artifacts and two configs. The envelope becomes a contract between services.                                               |
| One JVM          | One artifact; no contract across processes                                                                                                        | Consumer CPU and GC affect request latency. Scaling ingestion also adds consumers, which sit idle once there are more than 12. |

## Rationale

- **A buffer is needed regardless of Stage 4 tuning.** Acceptance is roughly 7× faster than processing. Kafka turns
  the backlog into a measured quantity (lag) instead of request latency.
- **Replay** from a retained log is what the recovery procedure in ADR-0005 relies on. Quorum queues cannot provide it.
- **Keyed partitions** give the "one writer per tenant" property that ADR-0003 relies on.
- **Failure isolation:** the API depends only on the broker.
- **Honest limit of this rationale:** at the volumes measured so far (about 1,000 events/s sustained), PostgreSQL as a
  queue would probably be enough and simpler. Kafka is justified here by failure isolation, replay and the project's
  learning goal, not by throughput.

## Consequences

- Delivery is at-least-once end to end. Duplicates are removed in Stage 3 (ADR-0002, ADR-0005).
- **There is no raw event store.** After 7 days the aggregates are the only record. A new aggregation (for example
  per `sourceId`) cannot be backfilled from older data, and an aggregate corrupted more than 7 days ago cannot be
  rebuilt. See [technology-selection.md, Q6](../architecture/technology-selection.md#6-open-questions-and-recommendations).
- The API is available only while Kafka is. When the broker is down, the API returns 503 within the 2.5 s publish
  deadline. Locally the broker is a single point of failure.
- Consumer lag is the main health signal. It is measured outside the consumer by kafka-exporter.

## Reconsider when

- Raw events must be kept longer than the topic retention. Add a sink (object storage via Kafka Connect, Kafka tiered
  storage, or an OLAP table; see [ADR-0011](0011-consumer-postgres-aggregation.md)) instead of raising retention
  without limit, and revisit ADR-0005.
- The target volume stays within what one PostgreSQL primary handles with batched inserts, and operating the broker
  becomes the main cost. Then PostgreSQL as a queue is the simpler option.
- Before Stage 6 (AWS), a separate ADR chooses between Amazon MSK, self-managed Kafka and Kinesis Data Streams.

## References

- Apache Kafka documentation: https://kafka.apache.org/documentation/
- RabbitMQ streams and super streams: https://www.rabbitmq.com/docs/streams
- Redpanda documentation: https://docs.redpanda.com/
- Apache Pulsar documentation: https://pulsar.apache.org/docs/
- PostgreSQL `SELECT … FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/17/sql-select.html
- Measured rates: [00-baseline.md](../performance/00-baseline.md)
