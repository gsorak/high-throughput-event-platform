# Performance after Stage 4 (Stage 5): focused analysis

> Recorded 2026-10-27/28. **Single runs on one laptop** (Intel i7-4870HQ, 8 logical CPUs, 16 GB; Docker VM with 8
> CPUs) that also runs the load generator (k6 in Docker), Kafka, PostgreSQL, Valkey and the monitoring stack. Each
> experiment ran once for 30–60 s; about 6 minutes of load in total. The numbers describe this code on this machine.
> They are not a capacity statement for other hardware.

## Summary

| Question                                        | Answer                                                                                                                                                                                                    |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Why was API p99 up to 2.8 s at 1,200/s?         | **Host CPU saturation**, caused mostly by the processor path (processor + PostgreSQL + Kafka consumer traffic) running on the same machine. With the processor stopped, the same load gives p99 28–53 ms. |
| Is the API itself slow?                         | No. API process CPU stays below 10% of the machine; GC pauses ≤ 42 ms. Its tail follows the producer queue, broker request latency and Valkey latency, which all grow with the host's load average.       |
| Why did Valkey's breaker open in Stage 4?       | Valkey commands waited for CPU, not for Valkey work: `valkey-cli` PING _inside the Valkey container_ reached 43–149 ms during load (2–4 ms idle) while Valkey used 15–17% of one CPU.                     |
| Sustained end-to-end capacity (with the ledger) | **~470–500 events/s** drain rate (concurrency 3); **400 events/s sustained cleanly** for 60 s (max lag 450, end-to-end p99 1.3 s). Stage 1's 1,000/s predates the ledger and is no longer valid.          |
| Consumer concurrency 3 vs 6                     | 6 drained a 36k backlog **1.05× faster** on average (peak 15 s rate 1.42×). Below the 1.5× threshold: **concurrency stays 3**.                                                                            |
| Ambiguous responses at 2,500/s (Stage 4)        | Producer queue saturation: records waited longer than `delivery.timeout.ms` (2 s). Not reproduced here (no run above 1,200/s); explanation from the Stage 4 metrics.                                      |

## Changes made in Stage 5

| Change                                                                                                                         | Why                                                                                                    |
| ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `ratelimit.valkey.duration{outcome=success\|timeout\|error\|breaker_open}` timer in `TenantRateLimiter`                        | Valkey latency was invisible; breaker openings could not be explained.                                 |
| `ValkeyBuckets` connects and PINGs Valkey at startup (`SmartLifecycle`); warns and connects lazily if Valkey is unreachable    | Found with the new timer: the first Valkey call took 1.6 s (lazy Lettuce connect, cold path).          |
| Kafka healthcheck every 30 s instead of 5 s (`start_interval: 2s` keeps startup fast)                                          | Each check starts a JVM (`kafka-broker-api-versions.sh`); idle Kafka container CPU fell 77.6% → 24.1%. |
| Grafana: Valkey p99 by outcome + fallback; publish p99 with producer queue and request latency; load average; host CPU; GC max | Separate client, API, producer queue and host saturation on one screen.                                |
| `load-tests/run.sh`: "Latency during the load" section; `DOCKER_STATS=1` per-container CPU                                     | Compare client-observed with server-side latency and see which container uses the CPU.                 |
| `scripts/resilience/run.sh`: bounded pre-scenario backlog check (`PRE_DRAIN_TIMEOUT`); optional `--warmup`                     | A leftover backlog either drains before the clock starts or fails setup with a clear message.          |

The Valkey warm-up fixed a real test failure: `SharedRateLimitIT` (also on the Stage 4 commit) accepted only 50–400
requests in its 2 s window instead of ≥ 500, because the first request took 2.12 s. After the fix the first request
takes 0.61 s and the first Valkey call 79 ms; startup takes ~1.1 s longer. With Valkey down at startup the API still
starts, and calls are counted as `error` and served by the local fallback (ADR-0004).

## E0 — Kafka healthcheck, idle (60 s, 118 `docker stats` samples)

| Healthcheck | Kafka CPU avg (% of one CPU) | max   | samples > 50% |
| ----------- | ---------------------------- | ----- | ------------- |
| every 5 s   | 77.6                         | 270.9 | 49 / 118      |
| every 30 s  | 24.1                         | 259.5 | 12 / 118      |

Kafka was healthy 48 s after a container restart; `kafka-init` and `kafka-exporter` started as before.

## E1 — API latency at 1,200 events/s (24 req/s × 50 tenants × 1 event, 30 s)

| Metric                                   | E1a cold, full stack     | E1b warm, full stack  | E1c API only (processor stopped) |
| ---------------------------------------- | ------------------------ | --------------------- | -------------------------------- |
| k6 duration med / p95 / p99 / max (ms)   | 32.6 / 256 / 846 / 1,116 | 22.4 / 96 / 561 / 876 | 14.1 / 22 / 28 / 83              |
| k6 blocked p99 / sending p99 (ms)        | 42 / 24                  | 6 / 9                 | 3.8 / 2                          |
| server 202 p50 / p99 (ms)                | 28 / 819                 | 20 / 595              | 13 / 26                          |
| Valkey success p99 (ms); failed calls    | 151; 0                   | 61; 1                 | 8; 0                             |
| PING inside Valkey container, max (ms)   | 43–149                   | 30–65                 | 3–32                             |
| publish p99 (ms)                         | 535                      | 579                   | 16                               |
| producer queue time / request max (ms)   | 182 / 419                | 611 / 861             | 12 / 26                          |
| API GC pause max (ms)                    | 37                       | 42                    | 31                               |
| host CPU avg / load average 1m max       | 0.89 / 64                | 0.83 / 49             | 0.58 / 24                        |
| CPU % of one core: k6 / Kafka / Postgres | 144 / 132 / 95           | 104 / 79 / 74         | 40 / 37 / 7                      |
| dropped iterations                       | 0                        | 0                     | 0                                |

A repeat of E1c (`s5-e1c-apionly-2`) gave k6 p99 52.5 ms and server p99 45 ms.

**Reading.**

- Client and server agree (k6 p99 846 vs server 819 ms; 561 vs 595 ms). The tail is inside the request, not in the
  client, the network or connection setup (`blocked`, `sending` are small).
- Inside the request, the tail follows the publish (`acks=all`: producer queue + broker round trip) and Valkey. Both
  are fast when the host is not saturated (E1c) and slow when it is (E1a/b). Valkey's own CPU stays ~15%, and even a
  PING from inside its container is slow under load: the delay is CPU scheduling in the Docker VM.
- The extra load comes from processing: PostgreSQL and Kafka CPU rise 10× and 2–4× when the processor runs, and the
  load average doubles. k6 itself uses 1–1.5 cores.
- Cold vs warm: the cold run has the worse p95 (256 vs 96 ms; JIT) but both have a tail of 0.5–0.9 s. Warm-up helps
  the median, not the saturation tail. The resilience `--warmup` stays optional and off by default.

**Conclusion.** The API p99 of Stage 4 is a property of running everything on one 8-CPU laptop above the processor's
capacity, not of the ingestion path. A production-like measurement needs the processor, the database and the load
generator on separate machines (Stage 6 territory). No API code change was justified.

## E4 — Processing capacity, consumer concurrency 3 vs 6

Method: stop the processor, build a backlog with an API-only run (E1c, 36,000 events), start the processor and time
the drain to lag 0 (5 s polling). Same backlog size for both runs, same 50 tenants.

| Concurrency | Drain after health UP | Avg events/s | Peak 15 s events/s | Hikari active max / pending | Upsert p99 | Errors |
| ----------- | --------------------- | ------------ | ------------------ | --------------------------- | ---------- | ------ |
| 3           | 76 s                  | 474          | 656                | 3 / 0                       | 14 ms      | 0      |
| 6           | 72 s                  | 500          | 934                | 6 / 0                       | 27 ms      | 0      |
| ratio 6 / 3 |                       | **1.05×**    | 1.42×              |                             | 1.9×       |        |

Also from the full-stack runs: peak aggregation 587/s (E1a, cold) and 645/s (E1b) with concurrency 3, while the API
was ingesting at the same time.

**Decision: keep concurrency 3.** The average drain rate improved by 5%, far below the agreed 1.5× threshold. The
peak rate rose more (1.42×), but the drain ends at the speed of the most loaded partitions: with `tenantId` keys, the
batch was spread unevenly (largest partition 5,107 events, smallest 1,379, mean 3,000), and 6 threads finish the light
partitions early and then idle. Upsert p99 doubled with 6 threads (more concurrent writers to the same aggregate rows).
Whether a different key spreads the work better is the partition-key experiment (ADR-0003), deferred.

## E5 — Sustained end-to-end check

400 events/s (8 req/s × 50 tenants) for 60 s, warm processor at concurrency 3, ledger enabled:

| Accepted = aggregated | Max lag | Drain after load | End-to-end p50 / p99 | k6 p99  | Server p99 | Valkey p99; failures | Host CPU / load avg max |
| --------------------- | ------- | ---------------- | -------------------- | ------- | ---------- | -------------------- | ----------------------- |
| 24,050                | 450     | 0 s              | 0.14 / 1.28 s        | 47.5 ms | 35 ms      | 14 ms; 0             | 0.69 / 26               |

**Capacity with the dedup ledger on this laptop: about 400 events/s sustained, with a drain ceiling around
470–500 events/s.** The rate between 400 and 500 was not measured. Stage 1's 1,000 events/s was measured without the
ledger (one extra insert per event in the same transaction) and is superseded.

## Leftover backlogs and the resilience suite

Stage 4's verdicts could start with a leftover backlog from a previous run, whose drain counted against the scenario's
deadline. `scripts/resilience/run.sh` now waits for lag 0 _before_ the scenario clock starts, at most
`PRE_DRAIN_TIMEOUT` seconds (120 quick, 300 extended), reports the time as "pre-drain (s)" in `summary.txt`, and
otherwise stops with a setup error that names `scripts/kafka-reset.sh status|skip` as a manual option. The script never
resets offsets itself.

## Final local validation

| Check                                            | Result                                                                                                                  |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `./mvnw -o -q verify`                            | exit 0, 83 tests, 0 failures, 0 errors, 0 skipped (82 before + 1 unit test); about 3.5 min                              |
| `scripts/resilience/run.sh all` (quick), 1st run | `20261028-000547`: overload PASS, outage PASS, crash INCONCLUSIVE (killed at lag 100 < 1,000; counts exact); 3 min 31 s |
| `scripts/resilience/run.sh all` (quick), 2nd run | `20261028-000953`: **3 PASS**; overload 76 s, outage 77 s, crash 71 s; pre-drain 0 s each; 4 min 18 s wall              |

The crash scenario's 500/s was calibrated for a drain rate of ~400/s. With the measured ~500/s it no longer built a
backlog, so its quick and extended load was raised to 800/s (`CRASH_PHASES=16x50x12` / `16x50x60`). Only the offered
rate changed; the checks and verdict rules did not.

## Optional future investigations (not done, not known defects)

- Batched multi-event database transactions (the per-event transaction bounds capacity at ~500 events/s here).
- Partition-key comparison (ADR-0003); E4 shows the skew that limits extra consumer threads.
- Producer `linger.ms`, `batch.size` and compression tuning.
- Virtual threads vs platform threads in the API.
- JFR profiles and distributed tracing.
- A 3-broker Kafka cluster.
- More fault scenarios (Valkey slow/down, Kafka down, PostgreSQL slow / pool exhausted).
- Kafka-level reconciliation of ambiguous (503) responses.
- The 45-minute extended resilience suite.
- Repeating these experiments (≥ 3 runs) on hardware where the load generator and the platform do not share CPUs.

## Reproduce

```bash
# Full stack, 1,200 events/s for 30 s, with per-container CPU
DOCKER_STATS=1 load-tests/run.sh s5-e1b-warm 24 50 30s

# API only (stop the processor first); leaves a backlog
ALLOW_LAG=1 DRAIN_TIMEOUT_SECONDS=5 DOCKER_STATS=1 load-tests/run.sh s5-e1c-apionly-1 24 50 30s

# Drain with concurrency 6 (default 3)
java -jar event-processor/target/event-processor-0.1.0-SNAPSHOT.jar --spring.kafka.listener.concurrency=6

# Sustained check
DOCKER_STATS=1 load-tests/run.sh s5-e5-sustained-400 8 50 60s

# Valkey latency from inside its container during a run
docker compose -f infra/local/compose.yaml exec -T valkey valkey-cli --latency-history -i 10
```

Raw output is written to `load-tests/results/` (not committed).
