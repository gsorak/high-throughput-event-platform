# Delivery guarantees

What each hop guarantees, what the client must do, and where duplicates come from. Stage numbers say when a row
becomes true.

## Per hop

| Hop                    | Guarantee                                                                                                                                  | Mechanism                                                              | Since |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- | ----- |
| Client → API           | At-least-once, **if the client retries** on 5xx/timeout with the same `eventId`s                                                           | Client-supplied `eventId` is the idempotency key                       | 1     |
| API → Kafka            | An event in a 202 batch is in the log (on 1 broker locally; replicated only with RF ≥ 3 and `min.insync.replicas=2`)                       | `acks=all`, idempotent producer, 202 only after every future completed | 1     |
| Kafka → processor      | At-least-once                                                                                                                              | Offsets committed after the listener returned (AckMode `BATCH`)        | 1     |
| Processor → PostgreSQL | Stage 1: **every delivery is counted** (duplicates double-count). Stage 3: each `(tenantId, eventId)` counted once within the dedup window | Stage 3: dedup insert + aggregate upsert in one transaction            | 1 / 3 |
| Processor → DLT        | A record that cannot be aggregated is in `events.v1.DLT` before its offset is committed                                                    | Synchronous publish; a failed publish throws, the offset stays         | 3     |

Not claimed: end-to-end exactly-once; ordering across tenants; deduplication outside the retention window.
The idempotent producer only removes duplicates caused by _its own internal retries_; it cannot see a client retry.

## Ambiguous publish outcomes

The API sends each event of a batch as its own Kafka record and waits for all acknowledgements. Kafka has no
multi-record atomicity without transactions (deliberately not used — see [ADR-0002](../adr/0002-acceptance-semantics.md)).
So a batch can end in four ways:

| Outcome                                                   | What the client sees    | What is in Kafka                                            |
| --------------------------------------------------------- | ----------------------- | ----------------------------------------------------------- |
| All acknowledged within the deadline                      | 202 `{"accepted": n}`   | All n events                                                |
| Some failed, some acknowledged                            | **503** + `Retry-After` | **The acknowledged subset**                                 |
| Deadline expired before every future completed            | 503 + `Retry-After`     | Unknown subset (records may still be written after the 503) |
| Client timed out / connection dropped before the response | Nothing                 | Unknown subset                                              |

In every non-202 case the only safe client action is: **retry the whole batch with the same `eventId`s.**
Regenerating ids on retry turns a duplicate into a distinct event that no later stage can detect.

"Some failed, some acknowledged" has two shapes, both covered by `PartialPublishFailureIT`:

- **Broker-side failure** (the record's future fails after it was sent): the API still waits for the other records,
  so every other event of the batch is in Kafka.
- **Immediate rejection** (`send()` fails at once: no metadata within `max.block.ms`, buffer full): the API stops
  sending, because the batch can no longer succeed. Events before the rejected one are in Kafka; later ones were never
  sent.

Consequences:

- Kafka contains both copies of the retried subset (`PartialPublishFailureIT`).
- Since Stage 3, the processor ledger makes the retry harmless within the dedup window
  ([ADR-0005](../adr/0005-deduplication-retention.md)): `DuplicateDeliveryIT.retriedBatchIsCountedOnce`, and
  `scripts/smoke-test.sh` posts the same batch twice against the running system.

## Where duplicates come from

1. Client retry after a non-202 outcome (above).
2. Consumer redelivery: a crash or rebalance after the DB commit but before the offset commit.
3. Operational replay: resetting the consumer group (e.g. after a database restore) or redriving the DLT (Stage 3).

All three are absorbed by the Stage 3 ledger **only** if they arrive within the dedup window of 21 days after the
first copy was counted. ADR-0005 derives the window from these sources and lists its limits.

## Processing in event-processor (Stage 3)

Each record is handled in one PostgreSQL transaction: insert `(tenant_id, event_id)` into `processed_events`; if the
row already exists, the record is a duplicate and nothing else happens (`processor.events.duplicate`); otherwise
upsert the minute aggregate. Either both writes commit or neither does.

When a record fails, `KafkaErrorHandlingConfiguration` decides:

| Failure                                                                                                 | Handling                                                   | Metric / test                                                                                               |
| ------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Malformed (not a valid envelope, unknown `schemaVersion`)                                               | DLT at once                                                | `dead_lettered{reason="malformed"}`, `DeadLetterIT`                                                         |
| Unprocessable (PostgreSQL SQLSTATE class 22 or 23, e.g. numeric overflow)                               | DLT at once; the ledger row is rolled back with the upsert | `dead_lettered{reason="unprocessable"}`, `DeadLetterIT`                                                     |
| Transient (connection failure, timeout, deadlock, no pooled connection, commit/rollback failed in JDBC) | Retried with back-off (0.5 s doubling to 30 s), no limit   | consumer lag, `delivery_failures{class="transient"}`; `DeadLetterIT`, `KafkaErrorHandlingConfigurationTest` |
| Anything else                                                                                           | 10 retries with the same back-off (≈ 2.5 min), then DLT    | `dead_lettered{reason="retries-exhausted"}`                                                                 |

The partition does not move past a failing record until it is aggregated or in the DLT. If publishing to the DLT
fails (topic missing, broker unavailable; at most 15 s per attempt), the offset is not committed and the record is
tried again: `DeadLetterIT.failedDeadLetterPublishLeavesTheOffsetUncommitted`.

Crash or rebalance at each point:

| Point                                           | After restart                                                                    |
| ----------------------------------------------- | -------------------------------------------------------------------------------- |
| Before the transaction committed                | Rolled back; the record is delivered again and counted once                      |
| After the commit, before the offset commit      | Delivered again; the ledger row exists, so it is skipped (`DuplicateDeliveryIT`) |
| After the DLT publish, before the offset commit | Delivered again, fails again, published again: **the DLT can hold two copies**   |

DLT records keep the original key and value and carry Spring Kafka's `kafka_dlt-*` headers (exception class and
message, original topic, partition, offset, consumer group). The topic keeps them at most 14 days.

The Stage 4 [resilience scenarios](../resilience/scenarios.md) exercise the transient path (PostgreSQL stopped under
load) and the crash table above (`kill -9` with a backlog) on the running system and check every event against
PostgreSQL.

### Redrive (manual)

There is no redrive tool. Redrive is a manual copy back to `events.v1`; the ledger makes copies of already counted
events harmless, including DLT duplicates. **Inspect every record before replaying it**, and correct it where
necessary:

- Read why it failed (step 1): `kafka_dlt-exception-cause-fqcn` and the last `Caused by:` in
  `kafka_dlt-exception-stacktrace`. `kafka_dlt-exception-fqcn` is only Spring's wrapper exception, and the original
  partition and offset headers are binary, so the console shows them as unreadable bytes.
- Fix the cause first. A record that failed because of the processor or the database (a bug, a missing migration) is
  replayed unchanged once the fix is deployed.
- A record whose content is wrong (malformed JSON, an unknown `schemaVersion`, a value that cannot be stored) fails
  again unchanged. Correct it in the dump, keeping its `eventId` and key, or leave it out and record why.
- Replay only records you have inspected.

Locally (key and value separated by a tab):

```bash
kafka() { docker compose -f infra/local/compose.yaml exec -T kafka /opt/kafka/bin/"$@" --bootstrap-server kafka:19092; }
# 1. Inspect: each record with its headers. The consumer stops after 10 s without new records (TimeoutException).
kafka kafka-console-consumer.sh --topic events.v1.DLT --from-beginning --timeout-ms 10000 \
    --formatter-property print.headers=true --formatter-property print.key=true
# 2. Dump key and value only.
kafka kafka-console-consumer.sh --topic events.v1.DLT --from-beginning --timeout-ms 10000 \
    --formatter-property print.key=true --formatter-property key.separator=$'\t' > dlt.tsv
# 3. Edit dlt.tsv: delete the records not to replay, correct the ones with wrong content. Then replay:
kafka kafka-console-producer.sh --topic events.v1 \
    --reader-property parse.key=true --reader-property key.separator=$'\t' < dlt.tsv
```

**Do not pipe the dump into the producer.** A record that still fails returns to the DLT within seconds, the running
consumer reads it again, and the pipe loops (seen locally: one malformed record became 62 DLT copies). Values must
not contain line breaks (envelopes written by ingestion-api do not). Verified locally with Kafka 4.3; older
`--property` options print a deprecation warning to stdout, which corrupts the dump.

## Timing bounds (Stage 1 configuration)

| Setting                     | Value   | Effect                                                                                  |
| --------------------------- | ------- | --------------------------------------------------------------------------------------- |
| `max.block.ms`              | 500 ms  | `send()` never blocks a request thread longer than this (metadata missing, buffer full) |
| `request.timeout.ms`        | 1500 ms | One produce request attempt                                                             |
| `delivery.timeout.ms`       | 2000 ms | Upper bound for a record's future to complete, including retries                        |
| `ingestion.publish-timeout` | 2500 ms | Deadline for the whole batch; then 503                                                  |

So with the broker down a request finishes in ≈0.5 s (no metadata) or ≈2 s (metadata cached), and never later than
the 2.5 s deadline plus response writing.
