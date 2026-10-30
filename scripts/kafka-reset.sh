#!/usr/bin/env bash
# Inspects or resets load-test data in the local Kafka topic between experiments. Never touches Docker volumes.
#
#   scripts/kafka-reset.sh status        read-only: records retained, size on disk, consumer group state and lag
#   scripts/kafka-reset.sh skip          move the processor's offsets to the end: the backlog is ignored, not deleted
#   scripts/kafka-reset.sh purge --yes   skip, then delete every record up to now (irreversible)
#
# skip and purge need the event-processor stopped: Kafka only changes the offsets of a group without members.
# Do not run them during a load test. TOPIC and GROUP override the defaults (events.v1, event-processor).
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC="${TOPIC:-events.v1}"
GROUP="${GROUP:-event-processor}"
COMPOSE=infra/local/compose.yaml

kafka() {
  local tool=$1; shift
  docker compose -f "$COMPOSE" exec -T kafka "/opt/kafka/bin/$tool.sh" --bootstrap-server kafka:19092 "$@"
}

# kafka-get-offsets prints one "topic:partition:offset" line per partition.
offset_sum() {
  kafka kafka-get-offsets --topic "$TOPIC" --time "$1" | awk -F: '{ sum += $3 } END { print sum + 0 }'
}

disk_mib() {
  kafka kafka-log-dirs --describe --topic-list "$TOPIC" | tail -1 \
    | jq '[.brokers[].logDirs[].partitions[].size] | add // 0 | . / 1048576 | floor'
}

# Empty = no members; Stable or *Rebalance = consumers attached; nothing printed = the group does not exist.
group_state() {
  kafka kafka-consumer-groups --describe --group "$GROUP" --state 2>/dev/null \
    | awk -v g="$GROUP" '$1 == g { print $(NF - 1) }' || true
}

group_lag() {
  kafka kafka-consumer-groups --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t && $6 ~ /^[0-9]+$/ { lag += $6 } END { print lag + 0 }' || true
}

status() {
  local earliest latest
  earliest=$(offset_sum earliest)
  latest=$(offset_sum latest)
  echo "Topic $TOPIC: $((latest - earliest)) records retained (offsets $earliest..$latest summed), $(disk_mib) MiB on disk"
  echo "Group $GROUP: state $(group_state | grep . || echo none), lag $(group_lag) records"
}

require_inactive() {
  local state
  state=$(group_state)
  if [[ -n "$state" && "$state" != "Empty" ]]; then
    echo "Group $GROUP is $state. Stop the event-processor first: Kafka refuses to move a live group's offsets." >&2
    exit 1
  fi
}

move_offsets_to_end() {
  kafka kafka-consumer-groups --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-latest --execute >/dev/null
}

case "${1:-status}" in
  status)
    status
    ;;
  skip)
    require_inactive
    move_offsets_to_end
    echo "Moved $GROUP to the end of $TOPIC. The skipped records stay until retention deletes them."
    status
    ;;
  purge)
    if [[ "${2:-}" != "--yes" ]]; then
      status
      echo "purge deletes every record in $TOPIC up to now and cannot be undone. Run: $0 purge --yes" >&2
      exit 1
    fi
    require_inactive
    # Offsets first. Otherwise the group's committed offsets stay below the new start of the log, and lag
    # (log end - committed offset) keeps reporting the deleted records until the processor restarts.
    move_offsets_to_end
    spec=$(kafka kafka-get-offsets --topic "$TOPIC" | jq -R -s '{version: 1, partitions: [split("\n")[]
      | select(length > 0) | split(":") | {topic: .[0], partition: (.[1] | tonumber), offset: (.[2] | tonumber)}]}')
    docker compose -f "$COMPOSE" exec -T kafka sh -c 'cat > /tmp/delete-records.json' <<< "$spec"
    kafka kafka-delete-records --offset-json-file /tmp/delete-records.json >/dev/null
    echo "Deleted every record of $TOPIC below the current end. Kafka frees the disk space when it removes old segments."
    status
    ;;
  *)
    echo "usage: $0 status | skip | purge --yes" >&2
    exit 2
    ;;
esac
