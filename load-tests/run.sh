#!/usr/bin/env bash
# Runs one k6 load test and then reports the server-side view of the same time window from Prometheus.
#
#   load-tests/run.sh <name> [rate] [batch] [duration]
#   load-tests/run.sh rate-100x20 100 20 120s
#
# rate = requests/s, batch = events per request, so offered events/s = rate x batch.
# Writes load-tests/results/<name>.txt (k6 output + server metrics) and <name>.json (k6 summary).
# Needs the local stack, both services and the JWT key (java scripts/DevJwt.java keys), plus JDK 25, curl, jq and
# Docker. Each of the TENANTS tenants (load-0 ...) is on the default plan; see "Rate limits" in load-tests/README.md.
# Refuses to start while the processor has a backlog (see "Between experiments" in load-tests/README.md).
# DOCKER_STATS=1 also samples the CPU of each container during the load (docker stats, about one sample per 2 s).
set -uo pipefail
cd "$(dirname "$0")"

NAME="${1:?usage: run.sh <name> [rate] [batch] [duration]}"
export RATE="${2:-50}" BATCH="${3:-20}" DURATION="${4:-60s}" TENANTS="${TENANTS:-50}" VUS="${VUS:-50}"
PROM="${PROM_URL:-http://localhost:9090}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-180}"
mkdir -p results
out="results/$NAME.txt"

# Instant query; prints the first value rounded to 4 decimals, or "n/a" when there is no data.
prom() {
  curl -sS "$PROM/api/v1/query" --data-urlencode "query=$1" --data-urlencode "time=$2" \
    | jq -r '.data.result[0].value[1] // "n/a" | (tonumber? // .) | if type == "number" then (. * 10000 + 0.5 | floor) / 10000 else . end'
}
# Exact number of increments of a counter during the run. increase() would extrapolate to the window edges.
counter_delta() {
  local before after
  before=$(prom "sum($1)" "$start"); after=$(prom "sum($1)" "$end")
  if [[ "$before" == "n/a" ]]; then before=0; fi
  if [[ "$after" == "n/a" ]]; then echo "n/a"; else echo $(( ${after%.*} - ${before%.*} )); fi
}
# Events the processor still has to read: the lag of the committed partitions (kafka-exporter reports -1 for a partition
# without a commit, clamped to 0) plus the events held by partitions without a committed offset (committed offset -1).
# Same query as the System overview (SystemMetrics.BACKLOG; checked by LagQueryConsistencyTest). "n/a" = the group has
# no committed offset at all, so the exporter publishes nothing for it.
lag_query='sum(clamp_min(kafka_consumergroup_lag{consumergroup="event-processor",topic="events.v1"}, 0)) + (sum((kafka_topic_partition_current_offset{topic="events.v1"} - kafka_topic_partition_oldest_offset{topic="events.v1"}) and on(topic, partition) (kafka_consumergroup_current_offset{consumergroup="event-processor",topic="events.v1"} == -1)) or vector(0))'

# A backlog left by an earlier run would be processed during this one and distort every end-to-end number.
lag_at_start=$(prom "$lag_query" "$(date +%s)")
if [[ "$lag_at_start" != "0" && "${ALLOW_LAG:-0}" != "1" ]]; then
  echo "Consumer lag is '$lag_at_start' (Prometheus), not 0. Let the processor drain it, or stop the processor and run" >&2
  if [[ "$lag_at_start" == "n/a" ]]; then
    echo "('n/a': Prometheus has no consumer-group series. Is the stack up, and has the processor ever committed an offset?)" >&2
  fi
  echo "scripts/kafka-reset.sh skip (then wait one 5 s scrape). Ingestion-only runs: set ALLOW_LAG=1." >&2
  exit 1
fi

# One token per tenant, valid 60 minutes. DevJwt reads the key relative to the repository root.
TOKENS=$(cd .. && java scripts/DevJwt.java tokens $(seq -f 'load-%g' 0 $((TENANTS - 1))) | paste -sd, -)
if [[ -z "$TOKENS" ]]; then
  echo "Could not create tokens with scripts/DevJwt.java (JDK 25 on the PATH, keys in .local/jwt?)" >&2
  exit 1
fi
export TOKENS

start=$(date +%s)
stats_pid=""
if [[ "${DOCKER_STATS:-0}" == 1 ]]; then
  rm -f "results/$NAME-docker.tsv"
  ( while true; do docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}' | tr -d '%' >> "results/$NAME-docker.tsv"; done ) &
  stats_pid=$!
fi
docker run --rm -v "$PWD:/scripts" -e RATE -e BATCH -e DURATION -e VUS -e TOKENS \
  grafana/k6:2.3.0 run --quiet --summary-export "/scripts/results/$NAME.json" /scripts/ingest.js | tee "$out"
k6_status=${PIPESTATUS[0]}
load_end=$(date +%s)
[[ -n "$stats_pid" ]] && kill "$stats_pid" 2>/dev/null

# The run is over once the processor has caught up. Lag is scraped every 5 s, so wait for two zero readings.
echo "Waiting for consumer lag to drain..."
zeros=0; drained=""
while (( $(date +%s) - load_end < DRAIN_TIMEOUT_SECONDS )); do
  sleep 5
  lag=$(prom "$lag_query" "$(date +%s)")
  if [[ "$lag" == "0" ]]; then zeros=$((zeros + 1)); else zeros=0; fi
  if (( zeros >= 2 )); then drained=$(( $(date +%s) - 10 )); break; fi
done
end=$(date +%s)
w=$(( end - start ))   # Prometheus range covering the whole run, including the drain

{
  echo
  echo "== Server-side metrics (Prometheus, window ${w}s) =="
  echo "k6 exit status                 : $k6_status (99 = a threshold was crossed)"
  echo "offered events/s               : $(( RATE * BATCH ))"
  echo "consumer lag at start (events) : $lag_at_start"
  echo "events accepted (total)        : $(counter_delta ingestion_events_accepted_total)"
  echo "events aggregated (total)      : $(counter_delta processor_events_aggregated_total)"
  echo "peak accepted events/s (15s)   : $(prom "max_over_time(sum(rate(ingestion_events_accepted_total[15s]))[${w}s:5s])" "$end")"
  echo "peak aggregated events/s (15s) : $(prom "max_over_time(sum(rate(processor_events_aggregated_total[15s]))[${w}s:5s])" "$end")"
  echo "max consumer lag (events)      : $(prom "max_over_time((${lag_query})[${w}s:5s])" "$end")"
  if [[ -n "$drained" ]]; then
    echo "drain time after load (s)      : $(( drained - load_end )) (5 s resolution)"
  else
    echo "drain time after load (s)      : not drained within ${DRAIN_TIMEOUT_SECONDS}s"
  fi
  echo "publish p99 (s)                : $(prom "histogram_quantile(0.99, sum by (le) (increase(ingestion_publish_duration_seconds_bucket{outcome=\"acknowledged\"}[${w}s])))" "$end")"
  echo "end-to-end latency p50 (s)     : $(prom "histogram_quantile(0.50, sum by (le) (increase(processor_end_to_end_latency_seconds_bucket[${w}s])))" "$end")"
  echo "end-to-end latency p99 (s)     : $(prom "histogram_quantile(0.99, sum by (le) (increase(processor_end_to_end_latency_seconds_bucket[${w}s])))" "$end")"
  echo "upsert p99 (s)                 : $(prom "histogram_quantile(0.99, sum by (le) (increase(processor_upsert_duration_seconds_bucket[${w}s])))" "$end")"
  echo "max Hikari pending threads     : $(prom "max_over_time(sum(hikaricp_connections_pending{application=\"event-processor\"})[${w}s:5s])" "$end")"
  echo "max Hikari acquire time (s)    : $(prom "max_over_time(max(hikaricp_connections_acquire_seconds_max{application=\"event-processor\"})[${w}s:5s])" "$end")"
  echo "avg CPU ingestion-api (0..1)   : $(prom "avg_over_time(process_cpu_usage{application=\"ingestion-api\"}[${w}s])" "$end")"
  echo "avg CPU event-processor (0..1) : $(prom "avg_over_time(process_cpu_usage{application=\"event-processor\"}[${w}s])" "$end")"

  # Where the time goes during the load itself (not the drain): client, API, Valkey, Kafka producer, host.
  l=$(( load_end - start ))
  api="application=\"ingestion-api\""
  http_202="http_server_requests_seconds_bucket{$api,uri=\"/v1/events\",status=\"202\"}"
  echo
  echo "== Latency during the load (${l}s): client (k6) vs server =="
  for k in duration waiting blocked sending; do
    printf 'k6 %-9s med/p95/p99/max (ms): %s\n' "$k" "$(jq -r --arg k "http_req_$k" '.metrics[$k]
      | [.med, .["p(95)"], .["p(99)"], .max] | map(. * 10 | floor / 10 | tostring) | join(" / ")' "results/$NAME.json" 2>/dev/null)"
  done
  echo "k6 dropped iterations          : $(jq -r '.metrics.dropped_iterations.count // 0' "results/$NAME.json" 2>/dev/null)"
  echo "server 202 p50 / p99 (s)       : $(prom "histogram_quantile(0.50, sum by (le) (increase(${http_202}[${l}s])))" "$load_end") / $(prom "histogram_quantile(0.99, sum by (le) (increase(${http_202}[${l}s])))" "$load_end")"
  echo "Valkey success p99 (s)         : $(prom "histogram_quantile(0.99, sum by (le) (increase(ratelimit_valkey_duration_seconds_bucket{outcome=\"success\"}[${l}s])))" "$load_end")"
  echo "Valkey calls not successful    : $(prom "sum(increase(ratelimit_valkey_duration_seconds_count{outcome!=\"success\"}[${l}s]))" "$load_end") (timeout, error, breaker_open)"
  echo "publish p99 during load (s)    : $(prom "histogram_quantile(0.99, sum by (le) (increase(ingestion_publish_duration_seconds_bucket[${l}s])))" "$load_end")"
  echo "producer queue time max (ms)   : $(prom "max_over_time(max(kafka_producer_record_queue_time_max{$api})[${l}s:5s])" "$load_end")"
  echo "producer request lat. max (ms) : $(prom "max_over_time(max(kafka_producer_request_latency_max{$api})[${l}s:5s])" "$load_end")"
  echo "API GC pause max (s)           : $(prom "max_over_time(max(jvm_gc_pause_seconds_max{$api})[${l}s:5s])" "$load_end")"
  echo "host CPU avg / max (0..1)      : $(prom "avg_over_time(system_cpu_usage{$api}[${l}s])" "$load_end") / $(prom "max_over_time(system_cpu_usage{$api}[${l}s])" "$load_end")"
  echo "host load average 1m, max      : $(prom "max_over_time(system_load_average_1m{$api}[${l}s])" "$load_end") ($(prom "max(system_cpu_count{$api})" "$load_end") logical CPUs)"
  echo "avg CPU ingestion-api in load  : $(prom "avg_over_time(process_cpu_usage{$api}[${l}s])" "$load_end")"
  if [[ -s "results/$NAME-docker.tsv" ]]; then
    echo "-- docker stats during the load (% of one CPU: avg / max) --"
    awk -F'\t' '{ n[$1]++; s[$1] += $2; if ($2 > m[$1]) m[$1] = $2 }
      END { for (c in n) printf "%-31s: %.0f / %.0f (%d samples)\n", c, s[c] / n[c], m[c], n[c] }' "results/$NAME-docker.tsv" | sort
  fi
} | tee -a "$out"
