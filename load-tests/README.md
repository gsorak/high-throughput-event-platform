# Load tests

[k6](https://grafana.com/docs/k6/) scripts for `POST /v1/events`. k6 runs in Docker (`grafana/k6:2.3.0`), so it does
not need to be installed. Results are written to `results/` (not committed).

## Files

- [`ingest.js`](ingest.js): the k6 script. Batches of UUIDv7 events for random tenants, at a fixed arrival rate. Each
  request carries the bearer token of one tenant, picked at random from `TOKENS`.
- [`run.sh`](run.sh): creates one token per tenant (`load-0` … `load-<TENANTS-1>`) with
  [`DevJwt.java`](../scripts/DevJwt.java), runs `ingest.js`, waits for the processor to drain the backlog, then
  queries Prometheus for the server-side view of the same window (accepted/aggregated counts, lag,
  publish/upsert/end-to-end percentiles, Hikari, CPU).
- [`resilience.js`](resilience.js): deterministic traffic for the Stage 4 resilience scenarios, with a manifest of
  every batch and its outcome. It is started by [`scripts/resilience/run.sh`](../scripts/resilience/run.sh), not by
  `run.sh`; see [docs/resilience/scenarios.md](../docs/resilience/scenarios.md).

## Usage

Start the stack and both services and create the JWT key (see the [README](../README.md#quick-start)), then:

```sh
load-tests/run.sh warmup 50 20 60s              # name, requests/s, events per request, duration
load-tests/run.sh e2e-1000 50 20 120s           # 1,000 events/s offered
ALLOW_LAG=1 DRAIN_TIMEOUT_SECONDS=1 VUS=200 TENANTS=100 load-tests/run.sh ingest-10k 200 50 120s   # processor stopped
```

Environment variables: `VUS` (pre-allocated virtual users, default 50; k6 may grow to 4×), `TENANTS` (default 50),
`PROM_URL`, `DRAIN_TIMEOUT_SECONDS` (default 180; set to 1 when the processor is stopped), `ALLOW_LAG` (set to 1 to
start despite a backlog, for ingestion-only runs), `DOCKER_STATS` (set to 1 to sample `docker stats` every ~2 s into
`results/<name>-docker.tsv` and print per-container CPU). Tokens are valid for 60 minutes, so a run including its drain must
finish within that time.

To run the script alone (from the repository root):

```sh
TOKENS=$(java scripts/DevJwt.java tokens load-0 load-1 | paste -sd, -) \
  docker run --rm -i -e TOKENS -e RATE=20 -e BATCH=10 -e DURATION=10s grafana/k6:2.3.0 run - < load-tests/ingest.js
```

## Rate limits

The load-test tenants are not listed in `rate-limit.tenants`, so each gets the `free` plan of
[application.yaml](../ingestion-api/src/main/resources/application.yaml): 200 events/s, burst 1,000, 1,000,000 events
per UTC day. Offered events/s divided by `TENANTS` must stay well below 200, or some requests get 429 and count as
failures (`http_req_failed`, `events_rejected`). The default 50 tenants cover up to about 5,000 events/s; use
`TENANTS=200` for 20,000. The daily quota lives in Valkey and is shared by all runs of the day.

## Between experiments

Ingestion-only runs (processor stopped) leave a backlog in `events.v1`. If an end-to-end run started with it, the
processor would spend the run draining old events and every end-to-end number would be wrong. `run.sh` therefore
refuses to start unless the consumer lag is 0, and records the lag at start in the results file.

[`scripts/kafka-reset.sh`](../scripts/kafka-reset.sh) removes the backlog without touching Docker volumes:

| Command       | Effect                                                                                                                                                        | Undo                                                                                  |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `status`      | Records retained, size on disk, `event-processor` group state and lag. Read-only.                                                                             | —                                                                                     |
| `skip`        | Moves the group's committed offsets to the end of the topic. The backlog is ignored, not deleted, and expires with the 7-day retention.                       | Possible while the records are retained (`kafka-consumer-groups.sh --reset-offsets`). |
| `purge --yes` | `skip`, then deletes every record up to now (Kafka `DeleteRecords`). New consumer groups and replays no longer see load-test data. Disk space is freed later. | None.                                                                                 |

Procedure:

```sh
# 1. Stop the event-processor (Ctrl-C). Kafka only moves the offsets of a group without members;
#    the script checks this and refuses otherwise.
scripts/kafka-reset.sh status
scripts/kafka-reset.sh skip            # default; purge --yes only when the data must be gone
# 2. Start the event-processor again and wait one Prometheus scrape (5 s) before the next run.
load-tests/run.sh warmup 50 20 60s
```

Not reset, on purpose:

- `docker compose … down -v` would also delete the PostgreSQL aggregates and the Prometheus history that earlier result
  files refer to, and cannot be undone. Use it only deliberately.
- PostgreSQL keeps the aggregates of the load-test tenants (`load-0` … `load-49`). They do not affect the smoke test,
  which uses a fresh tenant each time.
- Prometheus counters keep growing. `run.sh` reports differences within each run's window, so earlier runs do not
  matter.

## Why `constant-arrival-rate`

A **closed model** (N virtual users, each sending its next request when the previous one returns) automatically sends
_less_ when the server slows down. The test then reports good latency exactly when the system is struggling. This is
called _coordinated omission_.

An **open model** starts requests at a fixed rate whatever the response time, like independent clients on the
internet. A slow server shows up as higher latency, more concurrent VUs and, once k6 runs out of VUs,
`dropped_iterations`. Any dropped iteration means the offered load was not delivered, and the run is marked as failed
(`dropped_iterations: count<1`).

## Reading the output

- `http_req_duration` p50/p95/p99: latency as clients see it, including Docker Desktop port forwarding.
- `events_accepted`: events in 202 responses (custom counter).
- `http_req_failed`: any status other than 202, including client-side connection errors (status 0).
- `dropped_iterations`: requests k6 should have started but could not. Non-zero means k6 or the API saturated.
- Exit status 99: a threshold failed. `run.sh` still prints the server-side metrics.
- "Latency during the load": k6 duration, waiting, blocked and sending next to the server's 202 p50/p99, Valkey p99,
  publish p99, producer queue time, GC, host CPU and load average, all over the load window only. If k6 and the
  server agree, the time is spent inside the request; if the load average is far above the CPU count, the host is
  saturated ([Stage 5 analysis](../docs/performance/01-after-stage-4.md)).

Check the load generator before trusting a high-rate result. At 20,000 events/s offered, k6 used all 8 CPUs of the
Docker VM while the API used 0.36 cores ([baseline](../docs/performance/00-baseline.md)).
