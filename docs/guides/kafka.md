# Kafka in this project

What Apache Kafka and Spring for Apache Kafka do here, which settings matter, and what can go wrong.
Decisions are in [ADR-0002](../adr/0002-acceptance-semantics.md) (when to answer 202) and
[ADR-0003](../adr/0003-partition-key.md) (partition key); failure cases in
[delivery-guarantees.md](../architecture/delivery-guarantees.md).

## Concepts used

- **Topic `events.v1`**: an append-only log split into **partitions** (12 locally). Records with the same key go to the
  same partition, and Kafka keeps order only within one partition.
- **Key = `tenantId`**. Spreads tenants over partitions. The aggregation does _not_ need per-tenant order (additions
  commute), so the key is a load-distribution choice, not a correctness requirement. A hot tenant lands on one
  partition. Stage 4 compares other keys (ADR-0003).
- **Consumer group `event-processor`**: Kafka assigns each partition to one consumer in the group. Adding consumers
  beyond the partition count adds idle consumers. The **offset** is the position a group has committed per partition;
  **lag** is "last offset written − offset committed".
- **KRaft**: Kafka 4 runs without ZooKeeper; locally a single node is both broker and controller
  ([compose.yaml](../../infra/local/compose.yaml)). Replication factor 1 means losing the volume loses the data.

## Producer (ingestion-api)

`KafkaTemplate.send()` does **not** send. It serializes the record, appends it to an in-memory buffer and returns a
`CompletableFuture`. A background I/O thread groups buffered records into one request per partition leader and
completes the futures when the broker answers. [`EventPublisher`](../../ingestion-api/src/main/java/io/highthroughput/ingest/publish/EventPublisher.java)
sends the whole batch first and waits for all futures together, so a 500-event batch costs about one round trip.

| Setting                     | Value    | Why                                                                                                                                                                      |
| --------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `acks`                      | `all`    | The leader answers after all in-sync replicas have the record. With RF 1 that is the leader alone.                                                                       |
| `enable.idempotence`        | `true`   | Producer-internal retries cannot duplicate or reorder records within a partition. It does **not** help when the _client_ retries a batch; that is what `eventId` is for. |
| `linger.ms`                 | `5`      | Waits up to 5 ms to fill a request. Fewer, larger requests at the cost of up to 5 ms latency.                                                                            |
| `max.block.ms`              | `500`    | How long `send()` may block when it has no metadata (broker unreachable) or the buffer is full. Default 60 s would hang requests.                                        |
| `request.timeout.ms`        | `1500`   | One broker response.                                                                                                                                                     |
| `delivery.timeout.ms`       | `2000`   | Upper bound from `send()` to success/failure, including producer retries. Must be ≥ `linger.ms` + `request.timeout.ms`.                                                  |
| `ingestion.publish-timeout` | `2500ms` | The API's own deadline over the whole batch. Larger than the producer bounds, so the producer normally reports the failure first.                                        |

**What can go wrong**

- _First request after startup_: the producer is created lazily and fetches topic metadata on its first `send()`. On a
  cold JVM that took ~0.9 s, more than `max.block.ms`, so the first request after every start was a 503 with Kafka
  healthy. [`ProducerWarmUp`](../../ingestion-api/src/main/java/io/highthroughput/ingest/publish/ProducerWarmUp.java)
  fetches the metadata during startup, after topics are created and before the web server accepts requests. If the
  metadata is not available within `ingestion.kafka-warm-up-timeout` (10 s), it logs a WARN and the API starts
  anyway. A missing topic and an unreachable broker look the same to the client, so the WARN names both.
- _Broker unavailable_: `send()` fails, the API stops sending the rest of the batch and answers 503 with
  `Retry-After: 1`. When the broker is back, the next request succeeds; nothing needs a restart. Measured locally with
  100-event batches:

  | Situation                                    | Response                                                                 |
  | -------------------------------------------- | ------------------------------------------------------------------------ |
  | Broker stopped before the API starts         | API starts after the 10 s warm-up timeout; requests get 503 in 0.5–0.7 s |
  | Broker stopped while the API runs            | 503 in ~0.5 s: no partition leader, `send()` waits `max.block.ms`        |
  | Broker paused (connection open, no answers)  | First request 503 after 2.0 s (`delivery.timeout.ms`), then ~0.5 s       |
  | Topic missing (`--ingestion.topic=…`)        | API starts after the 10 s warm-up timeout; requests get 503 in ~0.65 s   |
  | Bootstrap host not resolvable (config error) | Startup fails: `No resolvable bootstrap urls given in bootstrap.servers` |

  [`KafkaUnavailableIT`](../../ingestion-api/src/test/java/io/highthroughput/ingest/KafkaUnavailableIT.java) stops
  the broker under a running API;
  [`KafkaUnreachableAtStartupIT`](../../ingestion-api/src/test/java/io/highthroughput/ingest/KafkaUnreachableAtStartupIT.java)
  starts the API without one. `/actuator/health` stays UP during an outage (there is no Kafka health indicator), so
  instances stay in rotation and clients get a fast 503 to retry instead of a connection error. The Kafka client
  logs a reconnect WARN about once per second while the broker is away, independent of traffic.
  A container that resolves the broker by service name can hit the last row while the broker container is stopped;
  starting the API only after Kafka is healthy avoids that.

- _Some records fail, others are written_: there is no rollback without Kafka transactions. The API answers 503 and the
  written records stay. A retry with the same `eventId`s writes them again
  ([`PartialPublishFailureIT`](../../ingestion-api/src/test/java/io/highthroughput/ingest/PartialPublishFailureIT.java)).
  The processor counts each `eventId` once (Stage 3 ledger), so the Kafka-side duplicates do no harm within 21 days.
- _Log noise_: spring-kafka's `LoggingProducerListener` logs every failed record at ERROR with a stack trace. It is
  switched off; `EventPublisher` logs one WARN per failed batch.

## Consumer (event-processor)

`@KafkaListener` creates a **listener container**: per consumer thread (`concurrency: 3`) a loop that calls `poll()`,
invokes the listener method for each record, then commits the offsets of that poll (`ack-mode: batch`).
Offsets are committed **after** processing, so a crash in between redelivers records (at-least-once).

| Setting              | Value                   | Why                                                                                                                                                                |
| -------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enable-auto-commit` | `false`                 | The Kafka client would otherwise commit on a timer, possibly before processing (at-most-once on crash).                                                            |
| `auto-offset-reset`  | `earliest`              | A new group reads the retained history instead of only new records.                                                                                                |
| `max-poll-records`   | `500`                   | Records handed to the container per `poll()`. Processing them must finish within `max.poll.interval.ms` (default 5 min) or the consumer is evicted from the group. |
| `value-deserializer` | `ByteArrayDeserializer` | Decoding happens in the listener, so a malformed record is a normal exception that the error handler sees.                                                         |
| `group.protocol`     | `classic`               | The established rebalance protocol; the KIP-848 protocol is a Stage 4 experiment.                                                                                  |

**Error handling** ([`KafkaErrorHandlingConfiguration`](../../event-processor/src/main/java/io/highthroughput/processor/config/KafkaErrorHandlingConfiguration.java)):
`DefaultErrorHandler` re-seeks the failed record and retries it with `ExponentialBackOff` (0.5 s doubling, capped at
30 s). How long depends on the failure: transient database failures without limit (an outage pauses the partition);
malformed records (`EnvelopeDecodingException`) and PostgreSQL data or integrity errors not at all; anything else 10
times. Records that are not retried further go to **`events.v1.DLT`** through `DeadLetterRecoverer`, which wraps
Spring Kafka's `DeadLetterPublishingRecoverer`: same key, value and partition number, plus `kafka_dlt-*` headers
(exception, original topic, partition, offset). It counts `processor.events.dead_lettered{reason}` and logs a WARN.
The publish is synchronous; if it fails, the error handler does not commit the offset and the record is tried again.
The processor's producer therefore has short timeouts (`delivery.timeout.ms` 15 s) so that a missing DLT fails
within seconds instead of the default 2 minutes. Both topics are created by `kafka-init` in the compose file;
auto-creation is off.

**What can go wrong**

- _Throughput ceiling_: one database transaction per record (two statements since Stage 3). The Stage 1 baseline
  saturates at ~1,400 events/s
  with low CPU ([00-baseline.md](../performance/00-baseline.md)).
- _Duplicates_: redelivery after a crash or rebalance is skipped by the ledger
  ([`DuplicateDeliveryIT`](../../event-processor/src/test/java/io/highthroughput/processor/DuplicateDeliveryIT.java)).
- _Poison records that are not classified_: they block their partition for about 2.5 minutes (10 retries), visible
  as growing lag on one partition (`kafka_consumergroup_lag`), then go to the DLT.

## Useful commands

```sh
# Topic configuration and partition leaders
docker compose -f infra/local/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic events.v1
# Committed offsets and lag per partition
docker compose -f infra/local/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group event-processor
# Load-test data: records retained, disk use, group state and lag; `skip` / `purge --yes` between experiments
scripts/kafka-reset.sh status   # procedure: load-tests/README.md, "Between experiments"
```
