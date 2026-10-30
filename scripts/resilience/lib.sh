# Helpers shared by run.sh and verify.sh. Sourced from the repository root.

COMPOSE=infra/local/compose.yaml
PROM="${PROM_URL:-http://localhost:9090}"
TOPIC=events.v1
DLT=events.v1.DLT
GROUP=event-processor
# PromQL in variables: bash 3.2 brace-expands {a,b} inside "$(cmd "...")", even within the inner quotes.
# kafka-exporter reports a committed offset and a lag of -1 for a partition without a commit. Backlog = the lag of the
# committed partitions (-1 clamped to 0) + the events held by partitions with committed offset -1. Same expressions as
# SystemMetrics.BACKLOG and the Grafana panel (checked by LagQueryConsistencyTest). "n/a" = no consumer-group series.
LAG_SUM_QUERY='sum(clamp_min(kafka_consumergroup_lag{consumergroup="event-processor",topic="events.v1"}, 0)) + (sum((kafka_topic_partition_current_offset{topic="events.v1"} - kafka_topic_partition_oldest_offset{topic="events.v1"}) and on(topic, partition) (kafka_consumergroup_current_offset{consumergroup="event-processor",topic="events.v1"} == -1)) or vector(0))'
LAG_MAX_QUERY='max(kafka_consumergroup_lag{consumergroup="event-processor",topic="events.v1"} >= 0 or on(topic, partition) ((kafka_topic_partition_current_offset{topic="events.v1"} - kafka_topic_partition_oldest_offset{topic="events.v1"}) and on(topic, partition) (kafka_consumergroup_current_offset{consumergroup="event-processor",topic="events.v1"} == -1)))'

kafka() {
  local tool=$1; shift
  docker compose -f "$COMPOSE" exec -T kafka "/opt/kafka/bin/$tool.sh" --bootstrap-server kafka:19092 "$@"
}

psql_query() {
  docker compose -f "$COMPOSE" exec -T postgres psql -U events -d events -X -q -At -F $'\t' -v ON_ERROR_STOP=1 -c "$1"
}

# Lag of the processor's group straight from the broker (committed offsets vs log end). Prints "<sum> <unknown>",
# where unknown counts partitions without a committed offset. Prints nothing if the command fails.
group_lag() {
  kafka kafka-consumer-groups --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t { if ($6 ~ /^[0-9]+$/) lag += $6; else unknown++; seen++ }
                         END { if (seen) print lag + 0, unknown + 0 }'
}

# Sum of the end offsets of a topic: grows by one per record written.
end_offsets() {
  kafka kafka-get-offsets --topic "$1" --time latest | sort -t: -k2,2n
}

# Instant Prometheus query; prints the first value as an integer, or "n/a".
prom_int() {
  curl -sS "$PROM/api/v1/query" --data-urlencode "query=$1" \
    | jq -r '.data.result[0].value[1] // "n/a" | (tonumber? // .) | if type == "number" then floor else . end'
}

# Sum of all samples of one metric (optionally filtered by a label regex) from the processor's actuator; "n/a" if the
# processor does not answer.
processor_metric() {
  curl -sf http://localhost:8081/actuator/prometheus \
    | awk -v m="$1" -v f="${2:-}" 'index($1, m "{") == 1 && (f == "" || $1 ~ f) { s += $2 }
                                   END { if (NR == 0) print "n/a"; else printf "%d\n", s }'
}

log() {
  echo "[$(date +%H:%M:%S)] $*"
}
