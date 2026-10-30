# ADR-0003: Partition key = `tenantId` (provisional)

- Status: Accepted for Stages 1–4; to be re-decided with measurements in Stage 5
- Stage: 1 (implemented)
- Assessment: **Design-time.** The approval of the architecture asked for `tenantId` with the explicit statement that
  ordering is not required, and for the comparison below (planned for Stage 4, moved to Stage 5 when Stage 4 was
  re-scoped to resilience).

## Context

The key decides which partition, and therefore which consumer thread, handles an event. The current aggregation —
count and sum per `(tenant, type, minute)` — is commutative: **it gives the same result in any order. Per-tenant
ordering is not required by any current logic.** The key is therefore a load-distribution and contention choice, not a
correctness one.

## Decision

Use `tenantId` as the key for `events.v1` (12 partitions).

Why, given ordering is not needed:

- One consumer thread owns all aggregate rows of a tenant, so concurrent consumers never update the same row.
  No lock waits, no deadlocks between consumer threads.
- Simple to reason about and to explain in lag dashboards ("tenant X is behind").

## Consequences

- A tenant's throughput is capped by one partition's consumer. A hot tenant also delays the other tenants hashed to
  the same partition (noisy neighbour). Stage 2 rate limits bound this; the bound must be checked against measured
  single-partition capacity.
- If future logic needs ordering (e.g. sessionization, state machines), this ADR must be revisited — the key would
  then become a correctness constraint.

## Stage 5 experiment (required before this ADR is finalised)

| Key                                   | Expected benefit                       | Expected cost                                                                                                |
| ------------------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `tenantId`                            | No cross-thread row contention         | Skew under a hot tenant                                                                                      |
| `tenantId:sourceId`                   | Spreads a hot tenant across partitions | Same aggregate row updated from several threads → lock waits; upserts need a stable order to avoid deadlocks |
| `eventId` / null (sticky partitioner) | Even spread                            | Maximum row contention; no locality                                                                          |

Measured under uniform and hot-tenant k6 scenarios: accepted and processed events/s, per-partition rate and lag
(skew), upsert latency, PostgreSQL lock waits and deadlocks. The result updates this ADR.

**Status after Stage 5:** not run; deferred as an optional future investigation. The Stage 5 concurrency test
([01-after-stage-4.md](../performance/01-after-stage-4.md#e4--processing-capacity-consumer-concurrency-3-vs-6)) showed
the skew this key causes with 50 uniform tenants: one batch spread 1,379–5,107 events per partition (mean 3,000), and
doubling consumer threads shortened the drain by only 5%. The key stays `tenantId` (provisional).

## References

- Kafka design, partitioning and consumer groups: https://kafka.apache.org/documentation/#design
- KIP-480, sticky partitioner (behaviour with a null key):
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner
- PostgreSQL row locks and deadlocks: https://www.postgresql.org/docs/17/explicit-locking.html
