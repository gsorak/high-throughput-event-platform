# ADR-0014: Per-tenant limits in the API, with shared bucket state in Valkey

- Status: Accepted (2026-10-27); implemented in Stage 2 with Valkey 9.0.1.
- Stage: 2
- Assessment: **Partly design-time.** The proposal chose Bucket4j with Valkey via Lettuce, and Valkey instead of
  Redis because of the licence. Edge rate limiting, local-only buckets, Kafka quotas and a quota derived from the
  aggregates were not compared. That comparison is retrospective. Outage behaviour is decided separately in
  [ADR-0004](0004-rate-limit-store-outage.md).

## Problem

- Each plan has a rate (events/s with a burst) and a daily quota. The tenant comes from the JWT
  ([ADR-0013](0013-jwt-without-identity-provider.md)), its plan from configuration.
- **Limits are counted in events, not requests.** A batch of 500 events costs 500 tokens. The cost is known only
  after the body has been parsed and validated.
- Stage 2 runs two API instances behind nginx. More instances may follow. A limit enforced per instance is correct
  only if the instance count is known and nginx spreads each tenant's load evenly.
- The daily quota must survive an API restart or redeploy.
- The check is on the hot path. It must stay well below the publish latency (baseline publish p99 ≤ 168 ms).

## Decision

- Rate limiting happens in `ingestion-api`, after validation and before publishing. One Bucket4j check consumes the
  batch's event count atomically. A rejection returns 429 with `Retry-After` and states whether the rate or the quota
  was exceeded.
- Bucket state lives in Valkey and is accessed through Bucket4j's Lettuce integration. Each tenant has one bucket
  with two limits: the rate (greedy refill, capacity = burst) and the daily quota, refilled in full at UTC midnight.
- Plans and the tenant-to-plan mapping are application configuration (`rate-limit.plans`, `rate-limit.tenants`;
  unlisted tenants get `rate-limit.default-plan`). **Amended in Stage 6:** `rate-limit.tenant-patterns` maps tenants
  by a full-match regular expression, after the exact entries and before the default
  ([ADR-0015](0015-demo-gateway.md)).
- When Valkey is slow or down, a Resilience4j circuit breaker switches to local buckets
  ([ADR-0004](0004-rate-limit-store-outage.md)).

## Alternatives considered

1. **Local buckets only.** Bucket4j in memory per instance, with `plan limit / instance count`. This is exactly the
   ADR-0004 fallback, used all the time.
2. **Rate limiting at the edge.** nginx `limit_req`, Spring Cloud Gateway `RequestRateLimiter` (Redis-backed), or
   Envoy with a global rate-limit service.
3. **Kafka client quotas.** The broker throttles produce bandwidth per client or user principal.
4. **Daily quota derived from the aggregates.** `event-processor` already counts events per tenant in PostgreSQL.
   The API would read the day's total periodically and enforce it. The rate limit stays local.
5. **Another shared store for Bucket4j.** Bucket4j also supports PostgreSQL and JCache providers such as Hazelcast.

## Comparison

| Criterion                      | **In-app, Bucket4j + Valkey**  | Local buckets only                    | Edge (nginx / Gateway / Envoy)                          | Kafka quotas                         | Quota from aggregates              |
| ------------------------------ | ------------------------------ | ------------------------------------- | ------------------------------------------------------- | ------------------------------------ | ---------------------------------- |
| Charges by event count         | Yes                            | Yes                                   | No. Charges per request; the event count is in the body | No. Charges bytes/s per Kafka client | Yes                                |
| Per tenant                     | Yes                            | Yes                                   | Yes, with a key from the token or header                | **No.** The API is the only client   | Yes                                |
| Accurate across instances      | Yes                            | Only with even load and a known count | nginx: per nginx instance; Gateway/Envoy: shared        | n/a                                  | Delayed by consumer lag            |
| Daily quota survives a restart | Yes                            | **No.** Restarting resets the bucket  | Gateway/Envoy: yes                                      | n/a                                  | Yes                                |
| New infrastructure             | Valkey                         | None                                  | Gateway service or Envoy rate-limit service + Redis     | None                                 | None, but the API reads PostgreSQL |
| Hot-path cost                  | A network round trip per batch | In-memory                             | Before the body is read: cheapest rejection             | Throttling, not rejection            | Cached read                        |

## Rationale

- **Limits in events rule out the edge options.** nginx, Spring Cloud Gateway and Envoy decide before the body is
  parsed and charge a fixed cost per request. Charging by event count there would need a custom filter that parses
  the JSON body, which duplicates the validation in the API.
- **Kafka quotas cannot see tenants.** Every record is produced by the API's own producer, so the broker sees one
  client.
- **Shared state fixes two concrete problems of local buckets.** A restart or redeploy resets an in-memory daily
  quota. And with uneven balancing, or an instance count that differs from configuration, the effective limit drifts
  (see the bound in ADR-0004).
- **The API stays database-free.** Option 4 and the Bucket4j PostgreSQL backend would put PostgreSQL on the
  ingestion path, contrary to [ADR-0006](0006-stats-endpoint-in-processor.md) and
  [ADR-0008](0008-kafka-buffer-two-deployables.md).
- **Valkey rather than Redis:** Valkey is BSD-licensed and wire-compatible with Redis, so the Lettuce client and the
  Bucket4j integration work unchanged. Redis changed to source-available licences in 2024 and added an AGPLv3 option
  in Redis 8.
- **Bucket4j rather than a hand-written Lua token bucket:** Bucket4j provides atomic multi-token consumption, refill
  arithmetic and `Retry-After` computation, and supports the same API for local and remote buckets. A local and a
  distributed limiter therefore share one configuration.

## Consequences

- A new stateful service on the hot path, with its own latency, timeouts and failure modes. ADR-0004 makes an outage
  fail open.
- Requests from one hot tenant contend on one key. Bucket4j updates remote buckets with compare-and-swap, so
  contention can add retries and round trips. Stage 2 measures the limiter's latency under a single-tenant load.
- Every Valkey call is timed: `ratelimit.valkey.duration{outcome=success|timeout|error|breaker_open}` (Stage 5). The
  API connects to Valkey at startup; a lazy first connection took 1.6 s and made the first request slow. If Valkey is
  unreachable at startup the API still starts and connects on first use.
- On one laptop the 100 ms command timeout is reached under CPU saturation, not because of Valkey work: at
  1,200 events/s a PING inside the Valkey container took up to 149 ms while Valkey used ~15% of one CPU. This explains
  the breaker openings of Stage 4 ([01-after-stage-4.md](../performance/01-after-stage-4.md)).
- Valkey needs no persistence. Losing its data resets the buckets, which allows a burst of over-admission. ADR-0004
  already accepts that.
- Rejected requests cost a full parse of up to 1 MiB. Cheap rejection before parsing comes from admission control,
  not from the rate limiter (technology-selection.md, Q3).

## Reconsider when

- Quotas turn out to be soft. Then local buckets plus a quota derived from the aggregates (options 1 and 4) avoid a
  stateful dependency. See Q4 in [technology-selection.md](../architecture/technology-selection.md#6-open-questions-and-recommendations).
- Limits become request-based, or a managed gateway is introduced on AWS. Then reconsider edge limiting.
- Quotas become billing-grade. Then the daily quota needs a durable ledger, not a cache.

## References

- Bucket4j: https://bucket4j.com/ and https://github.com/bucket4j/bucket4j
- Valkey: https://valkey.io/
- Redis licensing, AGPLv3 option in Redis 8: https://redis.io/blog/agplv3/
- Lettuce: https://github.com/redis/lettuce
- nginx `limit_req`: https://nginx.org/en/docs/http/ngx_http_limit_req_module.html
- Spring Cloud Gateway: https://docs.spring.io/spring-cloud-gateway/reference/
- Envoy global rate limiting:
  https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting
- Kafka quotas: https://kafka.apache.org/documentation/#design_quotas
