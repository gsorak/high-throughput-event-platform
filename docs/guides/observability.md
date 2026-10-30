# Observability

What is measured, where it comes from, and the queries used to judge the system. Prometheus since Stage 1; one
Grafana dashboard and postgres-exporter since Stage 4; tracing is planned for Stage 5.

## How metrics flow

```
ingestion-api :8080/actuator/prometheus ──┐
event-processor :8081/actuator/prometheus ┤
kafka-exporter :9308/metrics ─────────────┼──► Prometheus :9090 (scrapes every 5 s) ──► Grafana :3000
postgres-exporter :9187/metrics ──────────┘
```

- **Micrometer** is the metrics API inside the services (like SLF4J for metrics). Code registers `Counter`s and
  `Timer`s; Spring Boot adds JVM, HTTP, HikariCP and Kafka-client metrics automatically. The Prometheus registry
  renders them at `/actuator/prometheus`. Names are dotted in code (`processor.upsert.duration`) and become
  `processor_upsert_duration_seconds_*` in Prometheus.
- **Every service metric carries `application="<name>"`** (`management.metrics.tags.application`).
- **Histograms, not client-side percentiles.** Timers listed under `management.metrics.distribution.percentiles-histogram`
  publish `_bucket` series, so Prometheus can compute any percentile with `histogram_quantile`, also across instances.
  Buckets end at `maximum-expected-value` (default 30 s); anything slower lands in `+Inf`. End-to-end latency is
  configured up to 30 min, because during a backlog it is minutes (see the
  [baseline](../performance/00-baseline.md#results--end-to-end), footnote 1).
- **kafka-exporter** ([danielqsj/kafka_exporter](https://github.com/danielqsj/kafka_exporter)) asks the broker for
  log-end offsets and committed group offsets. That is the **only lag source that keeps working when the consumer is
  down**; the consumer's own `records_lag_max` disappears with the consumer.

- **postgres-exporter** ([prometheus-community/postgres_exporter](https://github.com/prometheus-community/postgres_exporter))
  connects to PostgreSQL and reports `pg_up` (0 while the database is unreachable) and database statistics. It shows
  a database outage independently of the processor's own errors.
- **Grafana** (http://localhost:3000, anonymous read-only access, bound to localhost) has one provisioned dashboard,
  _Overload and recovery_, used by the [resilience scenarios](../resilience/scenarios.md#grafana). Data source and
  dashboard are files under [infra/local/grafana](../../infra/local/grafana); changes made in the UI are not saved
  back.

Configuration: [prometheus.yml](../../infra/local/prometheus/prometheus.yml). Prometheus reaches the services on the
host via `host.docker.internal`; it does not scrape the containers of the compose profile `apps`.

## Metrics by question

| Question                                         | Metric                                                                                                             | Source                |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ | --------------------- |
| How many events were accepted?                   | `ingestion_events_accepted_total`                                                                                  | `EventPublisher`      |
| How long does Kafka take to acknowledge a batch? | `ingestion_publish_duration_seconds_bucket{outcome="acknowledged\|failed"}`                                        | `EventPublisher`      |
| What do clients see?                             | `http_server_requests_seconds_bucket{uri="/v1/events",status=...}`                                                 | Spring MVC            |
| How long do rate-limit calls to Valkey take?     | `ratelimit_valkey_duration_seconds_bucket{outcome="success\|timeout\|error\|breaker_open"}`                        | `TenantRateLimiter`   |
| Is the producer struggling?                      | `kafka_producer_request_latency_avg`, `kafka_producer_record_error_total`, `kafka_producer_buffer_available_bytes` | Kafka client          |
| How fast are events aggregated?                  | `processor_events_aggregated_total`                                                                                | listener              |
| How long is one database write?                  | `processor_upsert_duration_seconds_bucket` (includes waiting for a connection)                                     | listener              |
| How old is data when it reaches the aggregate?   | `processor_end_to_end_latency_seconds_bucket` (`receivedAt` → upsert done)                                         | listener              |
| Were records dead-lettered?                      | `processor_events_dead_lettered_total{reason="malformed\|unprocessable\|retries-exhausted"}`                       | `DeadLetterRecoverer` |
| Were duplicates skipped?                         | `processor_events_duplicate_total`                                                                                 | listener              |
| Why do processing attempts fail?                 | `processor_delivery_failures_total{class="transient\|permanent\|unclassified"}` (each failed attempt)              | error handler         |
| Is PostgreSQL reachable?                         | `pg_up`                                                                                                            | postgres-exporter     |
| Is the processor behind?                         | `kafka_consumergroup_lag{consumergroup="event-processor"}` per partition (-1: no commit, see Queries)              | kafka-exporter        |
| Is the connection pool the bottleneck?           | `hikaricp_connections_pending`, `hikaricp_connections_active`, `hikaricp_connections_acquire_seconds_max`          | HikariCP              |
| Are consumers rebalancing?                       | `kafka_consumer_coordinator_rebalance_total`                                                                       | Kafka client          |
| CPU and memory                                   | `process_cpu_usage` (0–1 of all cores), `jvm_memory_used_bytes`, `jvm_gc_pause_seconds_*`                          | JVM                   |

## Queries

Paste into http://localhost:9090/query.

```promql
# Accepted vs aggregated events/s. A persistent gap means lag is growing.
sum(rate(ingestion_events_accepted_total[1m]))
sum(rate(processor_events_aggregated_total[1m]))

# Total lag, and the worst partition (skew; relevant for the partition-key comparison in Stage 5).
# kafka-exporter reports a committed offset and a lag of -1 for a partition without a commit. The raw
# sum(kafka_consumergroup_lag{...}) would subtract those; the queries below (the same as the demo overview, the
# resilience and load-test scripts and the Grafana panel) clamp the lag and add the events such a partition holds.
sum(clamp_min(kafka_consumergroup_lag{consumergroup="event-processor",topic="events.v1"}, 0)) + (sum((kafka_topic_partition_current_offset{topic="events.v1"} - kafka_topic_partition_oldest_offset{topic="events.v1"}) and on(topic, partition) (kafka_consumergroup_current_offset{consumergroup="event-processor",topic="events.v1"} == -1)) or vector(0))
max(kafka_consumergroup_lag{consumergroup="event-processor",topic="events.v1"} >= 0 or on(topic, partition) ((kafka_topic_partition_current_offset{topic="events.v1"} - kafka_topic_partition_oldest_offset{topic="events.v1"}) and on(topic, partition) (kafka_consumergroup_current_offset{consumergroup="event-processor",topic="events.v1"} == -1)))

# API p99 latency of accepted requests
histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/v1/events",status="202"}[1m])))

# Error ratio of the ingest endpoint (5xx)
sum(rate(http_server_requests_seconds_count{uri="/v1/events",status=~"5.."}[1m]))
  / sum(rate(http_server_requests_seconds_count{uri="/v1/events"}[1m]))

# Publish p99 per outcome
histogram_quantile(0.99, sum by (le, outcome) (rate(ingestion_publish_duration_seconds_bucket[1m])))

# End-to-end p99 and upsert p99
histogram_quantile(0.99, sum by (le) (rate(processor_end_to_end_latency_seconds_bucket[1m])))
histogram_quantile(0.99, sum by (le) (rate(processor_upsert_duration_seconds_bucket[1m])))

# Pool pressure: threads waiting for a connection
max(hikaricp_connections_pending{application="event-processor"})

# Valkey p99 per outcome, and the producer's queue time (ms) behind the publish p99
histogram_quantile(0.99, sum by (le, outcome) (rate(ratelimit_valkey_duration_seconds_bucket[1m])))
max(kafka_producer_record_queue_time_max)

# Host saturation: load average vs CPUs (all processes share one laptop)
max(system_load_average_1m) / max(system_cpu_count)
```

The Grafana dashboard `overload-recovery` shows these next to each other (Valkey, publish and producer queue, load
average). When API latency rises, check the load average first: in Stage 5 every slow tail coincided with host
saturation ([01-after-stage-4.md](../performance/01-after-stage-4.md)).

[load-tests/run.sh](../../load-tests/run.sh) runs the same kind of queries over a load-test window and prints the result.

The demo gateway's _System overview_ ([ADR-0015](../adr/0015-demo-gateway.md)) reads Prometheus with a fixed set of
these queries ([`SystemMetrics`](../../demo-gateway/src/main/java/io/highthroughput/demo/scenario/SystemMetrics.java)),
cached for 3 s; the browser cannot send its own query. The metrics are platform-wide: there are no per-tenant labels.

The overview keeps three kinds of figure apart. **Now** (the 30 s ingest and processing rate, the backlog) does not
depend on the selected window; after a burst the rates fall to 0/s within 30 s. Over the selected **5 or 15 minute
window** it shows totals (`sum(increase(<counter>[5m]))`, reset-safe because `sum` is applied after `increase`) and
peaks (the highest `sum(rate(<counter>[15s]))` in the chart), and both charts use the same window and the same 5 s
step, so a peak does not depend on when the dashboard refreshes. A missing metric
is shown as "–", not 0, except the HTTP 429/5xx counters, which only exist after the first such response: they are
0 without a series, and a series born inside the window also counts its first value (`increase()` alone misses the jump
from no series to the first value; a series both born and reset within one window is undercounted). The
backlog is `sum(clamp_min(lag, 0))` plus the events held by partitions whose committed offset
(`kafka_consumergroup_current_offset`) is `-1`, the exporter's marker for no commit (kafka-exporter v1.11.0, checked
against a real broker); it is unknown if the group has no committed offset at all, because the exporter then
publishes no series for the group. "Processed" counts aggregated events
only; duplicates are listed separately, so ingested minus processed is not the backlog. The PromQL is unit-tested
([testing.md](testing.md#running)).

## Caveats

- **End-to-end latency compares two clocks**: `receivedAt` is set by the API, "now" by the processor. Locally they
  share a host. On separate hosts, clock skew shows up directly in this metric.
- `rate()` needs at least two samples in the window. With a 5 s scrape interval, use `[15s]` or longer.
- `increase()` extrapolates to the window edges and returns fractional counts. For exact totals, subtract two instant
  values of the counter (as `run.sh` does).
- Metrics are lost when a service restarts (counters start at 0; `rate()` handles the reset).
- The management endpoints share the application port (Stage 1 limitation, [README](../../README.md#known-limitations)).
