# ADR-0002: 202 means "every event acknowledged by Kafka"

- Status: Accepted (2026-10-27). Alternatives and references added in the documentation review (2026-10-27).
- Stage: 1 (implemented)
- Assessment: **Design-time** for the decision, early acknowledgement and Kafka transactions (architecture review and
  its approval, which asked for partial publication to be specified). Per-event results and the transactional outbox
  are a **retrospective** comparison.

## Context

An event passes through three states: **accepted** (validated), **published** (Kafka acknowledged), **processed**
(aggregate committed). The response code must name one of them. Answering before the acknowledgement is faster but loses
events the client believes were delivered if the API crashes or Kafka rejects the write.

A batch holds up to 500 events, sent as separate Kafka records. Without Kafka transactions, a batch can be **partially**
published.

## Decision

1. Return **202 `{"accepted": n}` only after every record's send future completed successfully** (`acks=all`).
2. Send all records first, then wait on all futures together (`CompletableFuture.allOf`) with a deadline
   (`ingestion.publish.timeout`, 2.5 s). One round trip per batch, not per event.
3. On any failure or deadline expiry return **503 + `Retry-After`** — even if some records were written.
4. Bound every blocking point: `max.block.ms` 500 ms, `delivery.timeout.ms` 2 s. If `send()` throws synchronously
   (no metadata, buffer full) the remaining events are **not** sent; the batch fails fast.
5. The client contract: on any non-202 outcome, **retry the whole batch with the same `eventId`s**.
6. No Kafka transactions. They would make the batch atomic for _read_committed_ consumers but cost a coordinator round
   trip per batch, add producer fencing complexity, and still not cover the client-retry duplicate (client timeout after
   commit). Deduplication by `eventId` in the processor (Stage 3) is needed anyway and covers both cases.

## Alternatives considered

| Option                                                                | Durable when the client sees success                                          | Batch atomic for consumers | Latency for a 500-event batch                                | Main cost                                                                                                                |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| **Send all, wait for all acks (chosen)**                              | Yes                                                                           | No (partial publication)   | About one producer batch round trip                          | Clients must retry whole batches; duplicates until Stage 3                                                               |
| 202 after validation, publish in the background                       | **No.** A crash or rejected write loses events the client counts as delivered | No                         | Lowest                                                       | Clients cannot know what to retry; an in-memory queue in the API needs its own bound                                     |
| Wait for each event's ack before sending the next                     | Yes                                                                           | No                         | ~500 × (`linger.ms` + round trip) ≥ 2.5 s: hits the deadline | No benefit over waiting for all                                                                                          |
| Per-event results (multi-status body)                                 | Yes, per event                                                                | No                         | Same as chosen                                               | Clients must parse results and retry subsets; with Stage 3 dedup, a whole-batch retry is equally safe                    |
| Kafka transaction per batch                                           | Yes                                                                           | Yes, for `read_committed`  | Chosen + transaction coordinator round trips                 | Transactional IDs and fencing per producer; still needs dedup for client retries                                         |
| Transactional outbox (API writes to a local database, relay to Kafka) | Yes, once the database commits                                                | Yes                        | A database commit per batch                                  | A database on the ingestion path, contrary to [ADR-0006](0006-stats-endpoint-in-processor.md); every event written twice |

The chosen option is the only one that keeps the API stateless and database-free, makes 202 mean durable, and needs
nothing beyond the Stage 3 deduplication that is required in any case.

## Consequences

- Partial success + 503 is a normal, documented outcome ([delivery-guarantees.md](../architecture/delivery-guarantees.md)).
  `PartialPublishFailureIT` demonstrates it; Stage 1 counts the retried subset twice, Stage 3 does not.
- Latency includes the Kafka acknowledgement (≥ `linger.ms` + replication). That is the price of durability and is
  visible in the baseline.
- With a single local broker, "acknowledged" means written to one disk, not replicated. Real durability needs RF 3 and
  `min.insync.replicas=2` (Stage 5 / AWS).
- "Processed" is never synchronous; it is observable through `/v1/stats` and the end-to-end latency metric.

## Reconsider when

- A consumer must never see part of a batch, for example a Kafka-to-Kafka stage that emits per-batch results. Then use
  Kafka transactions for that flow.
- Clients find whole-batch retries expensive (large batches, high partial-failure rates in Stage 5). Then return
  per-event results.
- Stage 5 or AWS runs with replication factor 3. Re-measure publish latency with `min.insync.replicas=2` and check that
  the 2.5 s deadline still has margin.

## References

- Kafka message delivery semantics: https://kafka.apache.org/documentation/#semantics
- Kafka producer configuration (`acks`, `enable.idempotence`, `delivery.timeout.ms`, `max.block.ms`):
  https://kafka.apache.org/documentation/#producerconfigs
- KIP-98, exactly-once delivery and transactional messaging:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- Spring for Apache Kafka, transactions: https://docs.spring.io/spring-kafka/reference/kafka/transactions.html
- Transactional outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html
