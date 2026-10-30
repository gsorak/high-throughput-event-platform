# ADR-0004: Rate-limit store outage — availability-first

- Status: Accepted (2026-10-27); implemented in Stage 2
- Stage: 2 (provisional until implemented)
- Assessment: **Design-time.** The approval of the architecture asked for availability-first versus strict quota to
  be decided explicitly. Why limits are enforced in the API with Bucket4j and Valkey at all is in
  [ADR-0014](0014-shared-rate-limit-state.md).

## Context

Stage 2 enforces per-tenant limits (events/s and a daily quota) with Bucket4j buckets in Valkey, shared by all API
instances. When Valkey is slow or down the API must choose:

| Mode                                               | Behaviour during outage                                    | Risk                                                                                  |
| -------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **Strict quota (fail closed)**                     | Reject (503/429) everything that cannot be checked         | A cache outage becomes a full ingestion outage; events are lost unless clients buffer |
| **Availability-first (fail open, local fallback)** | Keep accepting, limited by a per-instance in-memory bucket | Distributed limits are **not** enforced exactly                                       |

## Decision

**Availability-first with a local fallback limiter.**

- A Resilience4j circuit breaker wraps Valkey calls (short command timeout). When open, each instance enforces
  `plan rate / expected instance count` locally (`rate-limit.expected-instances`; burst and quota never below one
  500-event batch).
- Fallback mode is visible: a gauge/counter and a log line on every mode change.

## Consequences — stated explicitly

- **Distributed quotas can be exceeded during an outage.** The bound is roughly the sum of local limits: if the
  configured instance count is wrong (autoscaling, uneven load balancing), real acceptance can reach about
  N × the local limit. Daily quota consumption during the outage is not recorded in Valkey, so a tenant can also exceed
  the daily quota by the amount accepted while degraded.
- Billing/usage is unaffected: it is computed from PostgreSQL, not from the buckets.
- Kafka absorbs the extra load; the processor and lag alerts are the backstop.
- Stage 2 has a test that _asserts_ the over-admission is possible and bounded, so the trade-off cannot silently change.

## When to switch to strict quota

If quotas become contractual hard limits (e.g. prepaid usage, abuse protection with legal impact) or if over-admission
during an outage can overload downstream capacity. Switching means rejecting in the fallback path instead of limiting
locally: a small change, not a redesign, but Stage 2 has no flag for it.

## References

- Resilience4j CircuitBreaker: https://resilience4j.readme.io/docs/circuitbreaker
- Bucket4j (local and distributed buckets): https://bucket4j.com/
- Valkey: https://valkey.io/
