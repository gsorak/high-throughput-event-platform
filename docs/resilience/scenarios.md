# Resilience scenarios (Stage 4)

Can the platform absorb a temporary overload and recover from a database outage or a processor crash **without
losing or double-counting accepted events**? Each scenario injects one fault under load, waits for the backlog to
drain and then checks every event of the run against PostgreSQL. Throughput is not the goal here; the
[baseline](../performance/00-baseline.md) measures that.

What a PASS shows: in that run, on that machine, every event the API accepted was counted exactly once and nothing
else was counted. It is evidence for the mechanisms in [delivery-guarantees.md](../architecture/delivery-guarantees.md),
not a proof of exactly-once delivery in general. The limits are listed [at the end](#limits).

## Running

```bash
docker compose -f infra/local/compose.yaml up -d          # once; Grafana on http://localhost:3000
./mvnw package -DskipTests && java scripts/DevJwt.java keys   # jars and JWT key, if not done yet
scripts/resilience/run.sh all                             # or: overload | postgres-outage | processor-crash
scripts/resilience/run.sh processor-crash --mode extended # longer phases, 3 repetitions
```

- **Ports 8080 and 8081 must be free.** The script starts both services itself (it must be able to kill and restart
  the processor) and stops them at the end. It never touches processes or containers it did not start, except
  `docker compose stop/start postgres` in scenario B; on error or Ctrl-C it starts PostgreSQL again.
- The processor gets one extra argument, `--spring.kafka.consumer.properties.session.timeout.ms=10000`, so the broker
  notices a killed consumer after 10 s instead of 45 s. `application.yaml` is unchanged.
- Each scenario runs once. `--repeat N` repeats it; extended mode repeats 3 times by default.
- Every run uses fresh tenants (`r<8 hex>-0` … `r<8 hex>-24`, plus `r<8 hex>-hot` in the overload scenario). Nothing is
  deleted: Kafka topics, PostgreSQL rows and volumes stay. The run's rows are found by tenant.
- Results go to `load-tests/results/resilience/<stamp>/<scenario>/` (not committed). Start with `summary.txt`;
  `verify.txt` holds the raw check results, `lag.tsv` the lag timeline, `manifest.jsonl` every batch sent.
- Exit status: 0 all PASS, 1 any FAIL, 2 any INCONCLUSIVE and no FAIL, 3 setup error (nothing was judged).
- `scripts/resilience/verify.sh <scenario dir>` repeats the checks on saved results.
- **Backlog before a scenario** (Stage 5). Each scenario first waits until the consumer lag is 0, at most
  `PRE_DRAIN_TIMEOUT` seconds, _before_ its clock starts; the wait is reported as "pre-drain (s)" in `summary.txt`. If
  the lag does not reach 0 (e.g. a backlog left by an ingestion-only load test), the run stops with a setup error
  (exit 3). The script never resets offsets; `scripts/kafka-reset.sh status|skip` is the manual option.
- `--warmup` (optional, off by default) sends `WARMUP_PHASES` of load to throwaway tenants (`w<hex>-*`) after the
  services start, so the first scenario does not run on cold JVMs. It is not part of any verdict.

Settings (environment variables, defaults in `run.sh`). Phases are `requests/s x events per request x seconds`.

| Setting                                                   | quick                                       | extended                |
| --------------------------------------------------------- | ------------------------------------------- | ----------------------- |
| `OVERLOAD_PHASES`                                         | `24x50x10,2x50x10` (1,200/s, then 100/s)    | `24x50x60,2x50x60`      |
| `OVERLOAD_HOT_EVERY` (every n-th batch to the hot tenant) | 3                                           | 3                       |
| `OVERLOAD_MIN_PEAK_LAG`                                   | 5,000                                       | 20,000                  |
| `PG_PHASES`, `PG_DOWN_AT`, `PG_DOWN_FOR`                  | `10x20x35` (200/s), down at 10 s for 15 s   | `10x20x150`, 30 s, 60 s |
| `CRASH_PHASES`, `CRASH_KILL_AT`, `CRASH_DOWN_FOR`         | `16x50x12` (800/s), kill from 5 s, 5 s down | `16x50x60`, 20 s, 20 s  |
| `CRASH_MIN_BACKLOG`                                       | 1,000                                       | 5,000                   |
| `RECOVERY_TIMEOUT` (load end → lag 0)                     | 120 s                                       | 300 s                   |
| `SCENARIO_TIMEOUT` (hard limit per scenario)              | 300 s                                       | 900 s                   |
| `TENANT_COUNT`, `RETRIES`                                 | 25, 0                                       | 25, 0                   |
| `PRE_DRAIN_TIMEOUT` (lag 0 before the clock starts)       | 120 s                                       | 300 s                   |
| `WARMUP_PHASES` (only with `--warmup`)                    | `10x20x15` (200/s for 15 s)                 | same                    |

The rates are calibrated to the reference machine ([calibration](#calibration)): above the processor's capacity of
about 400 events/s, well below the API's.

## How every scenario is checked

**Traffic.** [load-tests/resilience.js](../../load-tests/resilience.js) (k6, constant arrival rate) sends batches whose
content is a pure function of the run id, phase and iteration: event ids encode them
(`<run>-<phase>-7<position>-8000-<iteration>`), types rotate over four values, values are whole numbers 1–100,
`occurredAt` is the send time. One JSON line per batch (tenant, `occurredAt`, HTTP statuses, outcome, events) goes to
`manifest.jsonl`. The outcome of a batch is

| Outcome     | When                                                                     | Expected in PostgreSQL    |
| ----------- | ------------------------------------------------------------------------ | ------------------------- |
| `accepted`  | any attempt returned 202                                                 | every event, exactly once |
| `rejected`  | 4xx (including 429), or 503 "Server busy" (sent before reading the body) | none of its events        |
| `ambiguous` | 5xx otherwise, timeout (10 s) or no response                             | each event at most once   |

`RETRIES=n` resends an ambiguous batch up to n times with the same body and event ids (default 0; the baseline script
[ingest.js](../../load-tests/ingest.js) is unchanged).

**Checks** ([verify.sh](../../scripts/resilience/verify.sh)), after the load has ended:

1. Wait until the broker reports lag 0 on all 12 partitions twice in a row (`kafka-consumer-groups`, bounded by
   `RECOVERY_TIMEOUT`). Lag 0 means every record was consumed and its offset committed after its transaction.
2. Read the ledger (`processed_events`) and the aggregates (`event_stats_minute`) of the run's tenants directly from
   PostgreSQL.
3. Every event of an accepted batch is in the ledger; no ledger entry belongs to a rejected batch or to no batch.
4. Ambiguous events found in the ledger are expected (once); ambiguous events not in the ledger are reported as
   unresolved.
5. For every (tenant, minute, type): `event_count` and `value_sum` equal the expected count and sum exactly.
   Double counting shows up here (the ledger has a primary key, so it cannot show duplicates itself).
6. No record was written to `events.v1.DLT` during the run (end offsets before and after). New DLT records are
   dumped with their headers to `dlt-records.txt`.

Neither a 202 nor zero lag counts as proof on its own: step 3 and 5 compare each event.

**Verdict.**

- **PASS**: all checks hold and the scenario's precondition (below) was met.
- **FAIL**: an accepted event is missing, an aggregate differs (too low: lost; too high: double-counted), an
  unexplained or rejected event was counted, a record was dead-lettered, a scenario-specific claim failed, or the lag
  did not return to 0 within the timeout.
- **INCONCLUSIVE**: no FAIL, but the run did not test what it was meant to (precondition not met), or ambiguous
  events could not be reconciled (not in the ledger: either never written to Kafka or lost, which the test cannot
  tell apart).

On anything but PASS, `summary.txt` adds the consumer group table and the services' WARN/ERROR lines from the
scenario; the detail files (`missing.tsv`, `unexplained.tsv`, `aggregate-diff.tsv`, `dlt-records.txt`) list the
events concerned.

**Kafka acceptance vs database persistence.** A 202 means the events are in Kafka (on this single broker), not that
they are aggregated. The time between the two is the consumer lag. During every scenario below, the API keeps
accepting while aggregation falls behind or stops; the checks run only after the lag is back to 0.

## A. Overload

**Shows:** traffic above the processor's capacity builds a backlog in Kafka instead of failing requests, the backlog
drains when traffic drops, and nothing is lost or counted twice.

**Procedure:** 1,200 events/s for 10 s (three times what the processor drains on the reference machine), then
100 events/s for 10 s, then stop. Every third batch goes to one "hot" tenant on the free plan (200 events/s), which
sends 400 events/s and gets 429s once its burst is used up: these rejected events must not appear anywhere.

**Expected:** lag rises during the first phase and falls during the second; API latency stays low (the API only
waits for Kafka); some 429s for the hot tenant, no 5xx.

**Precondition:** peak lag ≥ `OVERLOAD_MIN_PEAK_LAG` (else INCONCLUSIVE: the processor kept up, raise the rate).

## B. PostgreSQL outage

**Shows:** ingestion does not depend on PostgreSQL; the processor retries instead of dead-lettering or dropping
records; after PostgreSQL returns, the backlog drains and the aggregates are exact.

**Procedure:** 200 events/s for 35 s. At 10 s, `docker compose stop postgres` (the volume stays); 15 s later
`docker compose start postgres`.

**Expected:** every batch sent during the outage gets 202. The processor's transactions fail
(connection terminated, SQLSTATE 57P01 and 08xxx, or no pooled connection within 30 s), are classified
transient and retried with back-off up to 30 s; `processor_delivery_failures_total{class="transient"}` rises, lag
grows, nothing reaches the DLT. A record whose transaction was cut off is rolled back and counted once on retry.

**Found by this scenario:** when PostgreSQL stops, the statement (or the commit) fails with 57P01, Hikari closes the
connection, and the rollback then fails with Hikari's "Connection is closed" (no SQLSTATE). Spring throws that
rollback failure (`TransactionSystemException`) instead of the original exception, so the processor classified it as
_unclassified_: retried only 10 times, then dead-lettered. In the runs the next attempt failed differently (no
connection: transient) and was retried without limit, so nothing was lost, but a run of such failures could have
dead-lettered records during an outage. The first run showed the statement case, where Spring keeps the original as
the "application exception"; a later run showed the commit case, where Spring only logs it. Now the original decides
when it is known, and a commit or rollback that failed in JDBC is transient otherwise
(`KafkaErrorHandlingConfigurationTest.failedRollbackAfterTransientFailureIsTransient`,
`failedRollbackAfterFailedCommitIsTransient`).

**Scenario checks:** every batch sent while PostgreSQL was down was accepted, and no failed attempt was counted as
_unclassified_ (it would be dead-lettered after 10) (else FAIL); at least one batch was sent in that window and at
least one transient failure was counted (else INCONCLUSIVE).

## C. Processor crash

**Shows:** records that were aggregated but whose offsets were not yet committed are redelivered after a crash and
skipped by the dedup ledger; records not yet processed are processed after the restart.

**Procedure:** 500 events/s for 12 s. From 5 s on, as soon as the lag (Prometheus) reaches `CRASH_MIN_BACKLOG`,
at the latest when the load ends, `kill -9` the processor (no shutdown hook, no offset commit); start it again 5 s
later. The kill waits for the backlog rather than a fixed time because the Prometheus lag trails the broker by one
or two scrapes: a first run with a fixed kill at 5 s read a lag of 100 and was INCONCLUSIVE, although the lag
reached 1,950 two seconds later.

**Expected:** lag keeps growing while the processor is down (kafka-exporter still reports it). After the restart and
the rebalance (up to the 10 s session timeout of the dead members), the new instance resumes from the last committed
offsets; `processor_events_duplicate_total` counts the redelivered records; the backlog drains.

**Precondition:** lag at the kill ≥ `CRASH_MIN_BACKLOG`, and at least one duplicate skipped after the restart (else
INCONCLUSIVE: redelivery was not exercised). The individual crash points are covered deterministically by
`DuplicateDeliveryIT`; this scenario shows the same on the running system.

## Grafana

Dashboard **Overload and recovery** (http://localhost:3000, provisioned from
[infra/local/grafana](../../infra/local/grafana)): accepted vs aggregated events/s, lag (total and per partition),
rejected/failed requests by status, duplicates and dead letters, failed processing attempts by class, availability
(`pg_up`, `up` of both services), API p99, CPU and the processor's connection pool. Backlog growth is the gap between
the two lines in the first panel and the rising lag in the second.

## Calibration

Short runs to choose the scenario rates, not performance results. Reference machine as in the
[baseline](../performance/00-baseline.md): i7-4870HQ (4 cores / 8 threads), 16 GB; Docker Desktop VM with 8 CPUs and
7.75 GiB; k6, Kafka, PostgreSQL, Valkey, Prometheus, Grafana and both services on the same laptop. On other machines
the rates need to be checked again (`OVERLOAD_PHASES` etc.); the preconditions report when a run did not overload.

| What                                        | Observed                                                                                                                                                                                                    |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| API acceptance, API alone                   | ≥ 10,000 events/s accepted into Kafka, warm API, processor stopped ([baseline](../performance/00-baseline.md))                                                                                              |
| API acceptance, full stack, freshly started | 2,500 events/s offered for 15 s: 30,450 events accepted, but 40 batches got 503 after the publish timeout and 1 got no response (2,050 ambiguous events; 1,023 of them turned up aggregated, once)          |
| End-to-end (aggregated in PostgreSQL)       | about 380–400 events/s, from the lag slope while draining; processor CPU stayed below 20 % of the machine. Each event is one transaction with two writes (ledger, aggregate): bound by round trips, not CPU |
| Broker disk                                 | about 250 bytes per event without compression (1,968 KiB for 8,020 events)                                                                                                                                  |

So the API accepts far more than the processor aggregates: API throughput and end-to-end throughput are different
numbers, and the gap is the backlog. The quick rates follow from the ~400 events/s: overload at 1,200/s (three times
the drain rate, half the rate that caused 503s); the outage at 200/s, so that the backlog of a 15 s outage drains
within about 20 s; the crash at 800/s (500/s until Stage 5), above the drain rate, so a backlog builds before the kill. Why the
processor is this slow, and what helps, is a Stage 5 experiment.

Stage 5 measured it again ([01-after-stage-4.md](../performance/01-after-stage-4.md)): 470–500 events/s drain rate,
400 events/s sustained; 6 consumer threads gained 5%. The overload and outage rates stand. The crash load was raised
from 500/s to 800/s: at 500/s a warm processor kept up, the kill came at a lag of 100 and the run was INCONCLUSIVE
(`20261028-000547`; counts exact).

## Observed results

Quick mode, `scripts/resilience/run.sh all`, run `20261027-220049` on the reference machine, each scenario once. Times
are relative to the start of the load; lag from Prometheus (5 s resolution), recovery = end of the fault (overload:
end of the 1,200/s phase; outage: PostgreSQL ready again; crash: processor started) to the first lag sample of 0.

|                                           | A. Overload                     | B. PostgreSQL outage                      | C. Processor crash                                        |
| ----------------------------------------- | ------------------------------- | ----------------------------------------- | --------------------------------------------------------- |
| Verdict                                   | **PASS**                        | **PASS**                                  | **PASS**                                                  |
| Scenario time (start → verdict)           | 119 s (74 s without the drain¹) | 99 s                                      | 74 s                                                      |
| Load                                      | 19 s                            | 37 s                                      | 14 s                                                      |
| Fault                                     | 1,200/s for 10 s                | stopped +11 s, started +26 s, ready +27 s | killed at +9 s (lag ~1,500), started +15 s, healthy +28 s |
| Batches during the fault                  | –                               | 150 sent, 150 accepted                    | –                                                         |
| Events accepted / rejected / ambiguous    | 10,950 / 1,000 (429) / 0        | 7,020 / 0 / 0                             | 6,050 / 0 / 0                                             |
| `event_count` / `value_sum` (run tenants) | 10,950 / 547,375, exact         | 7,020 / 352,110, exact                    | 6,050 / 303,875, exact                                    |
| Accepted events missing / unexplained     | 0 / 0                           | 0 / 0                                     | 0 / 0                                                     |
| Duplicates skipped                        | 0                               | 0                                         | 229                                                       |
| Failed attempts transient / unclassified  | 0 / 0                           | 3 / 0                                     | 0 / 0                                                     |
| DLT records                               | 0                               | 0                                         | 0                                                         |
| Peak lag                                  | 8,941 at +14 s                  | 4,700 at +41 s                            | 2,750 at +33 s                                            |
| Recovery (fault end → lag 0)              | 30 s                            | 31 s                                      | 28 s                                                      |
| Lag 0 on the broker after the load ended  | 33 s                            | 32 s                                      | 35 s                                                      |

¹ The run started right after a deliberately interrupted one; the overload scenario first waited about 45 s for the
processor to drain that run's backlog (it waits for lag 0 before sending).

The three scenarios took 292 s together, 5 min 28 s wall time including the start of both services (~30 s) and that
extra drain. An earlier full run (`20261027-215154`) took 74 s, 82 s and 72 s (4 min 12 s wall). The outage scenario
varied between 82 s and 99 s across runs, above the 60–90 s target: after PostgreSQL returns, the processor waits
out its current back-off (up to 30 s) before it retries, so the drain starts at a different point each time. The
settings were not shortened for that.

**Observed, not expected:**

- During the 1,200/s phase the API's p99 for 202 responses rose to 2.8 s in the final run (0.09 s in an earlier
  run), with Kafka publish p99 at ~0.9 s, and the rate limiter's Valkey calls timed out often enough to open its
  circuit breaker for 5 s ([ADR-0014](../adr/0014-shared-rate-limit-state.md): local buckets meanwhile). The breaker
  opened in 4 of the 6 runs that included an overload (one of them at 2,500/s), always on a freshly started API
  sharing the machine with everything else. No 5xx and no ambiguous batch at 1,200/s; at 2,500/s the API returned
  503s (calibration). The expectation "API latency stays low" held only partly. Stage 5 found the cause: host CPU
  saturation from running the processor path, Kafka, PostgreSQL and k6 on one laptop; even a PING inside the Valkey
  container took up to 149 ms. With the processor stopped the same load gives p99 below 60 ms
  ([01-after-stage-4.md](../performance/01-after-stage-4.md)).
- Both variants of the rollback failure (statement and commit) occurred in the final outage run (2 and 1 times) and
  were counted as transient.

Earlier runs, kept for what they showed:

| Run               | Scenario (settings)            | Verdict                | What it showed                                                                                                                                                      |
| ----------------- | ------------------------------ | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `20261027-210638` | overload, 2,500/s for 15 s     | INCONCLUSIVE           | 503s and 2,050 ambiguous events (1,023 aggregated once, 1,027 not in the ledger); all accepted events exact. Rates lowered (calibration)                            |
| `20261027-211332` | overload, `24x50x15,2x50x15`   | PASS                   | 18,350 accepted, peak lag 14,095, recovery 48 s; phases then shortened to 10 s                                                                                      |
| `20261027-211641` | outage, `10x20x40`             | INCONCLUSIVE           | counts exact, but no transient failure: all 3 failed attempts were unclassified (statement case of the rollback failure). Fixed, test added; such attempts now FAIL |
| `20261027-212544` | outage, `10x20x40`             | PASS                   | 8,000 accepted, 2 transient failures, peak lag 4,298                                                                                                                |
| `20261027-212755` | crash, `16x50x15`, kill at 6 s | PASS                   | 12,050 accepted, 919 duplicates skipped, peak lag 11,359, recovery 43 s                                                                                             |
| `20261027-213122` | all                            | 2 PASS, 1 INCONCLUSIVE | crash killed at a Prometheus lag of 100 (stale reading): the kill now waits for `CRASH_MIN_BACKLOG`                                                                 |
| `20261027-215154` | all                            | 3 PASS                 | outage run had 1 unclassified attempt: the commit case of the rollback failure. Fixed, test added                                                                   |
| `20261028-000547` | all (Stage 5)                  | 2 PASS, 1 INCONCLUSIVE | crash at 500/s: kill at lag 100, below `CRASH_MIN_BACKLOG`; counts exact. Crash load raised to 800/s                                                                |
| `20261028-000953` | all (Stage 5)                  | 3 PASS                 | overload 76 s, outage 77 s, crash 71 s (9,600 accepted, killed at lag ~1,350, 392 duplicates skipped); pre-drain 0 s each; 4 min 18 s wall                          |

Results directories are not committed; the numbers above are copied from their `summary.txt`. One run per scenario
on one machine is a demonstration, not a measurement of how often recovery succeeds; extended mode (`--mode
extended`, 3 repetitions) has not been run for this stage.

## Limits

- **Retention.** The backlog lives in `events.v1`, retained 7 days. If the processor cannot catch up within that
  time (e.g. PostgreSQL down for a week), the oldest unprocessed events are deleted unread: data loss. Retries have no
  limit, so nothing alerts except the growing lag.
- **Disk.** The backlog takes broker disk: about 250 bytes per event without compression (measured: 1,968 KiB for
  8,020 events). A full disk stops the broker; the API then answers 503 and accepts nothing.
- **Single broker.** Replication factor 1 locally: a 202 means one copy on one disk. Broker loss is not tested here.
- **Dedup window 21 days** ([ADR-0005](../adr/0005-deduplication-retention.md)). A redelivery or replay of an event
  first counted more than 21 days earlier is counted again.
- **Ambiguous publishes.** When the API cannot confirm a publish in time it answers 503, but the record may still
  be in Kafka. A calibration run at 2,500 events/s produced 2,050 such events, 1,023 of which were aggregated (once);
  the other 1,027 were not in the ledger, and the test cannot tell "never written" from "lost". Such runs are
  INCONCLUSIVE, never PASS. Reading `events.v1` back to resolve them is not implemented.
- **What is not tested:** Kafka broker outage or disk full, network partitions, several processor instances,
  PostgreSQL data loss or restore, slow PostgreSQL or an exhausted pool, Valkey outage, clock skew.
- **Machine-specific.** Rates and times depend on the reference machine and on k6, both services, Kafka and
  PostgreSQL sharing it.
