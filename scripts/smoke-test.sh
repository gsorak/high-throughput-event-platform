#!/usr/bin/env bash
# End-to-end smoke test of the running system, from the repository root:
#   1. a request without a token is rejected (401)
#   2. authenticated ingestion: N events for a fresh tenant are accepted (202), then the same batch again (a client
#      retry: the same eventIds reach Kafka twice), then one marker event
#   3. rate limiting: a fresh free-plan tenant sends three 500-event batches back to back; the third exceeds the
#      1000-event burst (429, limit "rate", Retry-After)
#   4. deduplication and tenant-isolated statistics: once the marker is counted, both copies of the batch have been
#      processed, and the first tenant's token sees exactly N events; another tenant's token sees only its own (empty)
#      statistics, even when it names the first tenant in X-Tenant-Id
#
#   scripts/smoke-test.sh [events]        default 100; at most 500 (one batch)
#
# Needs java, curl, jq and uuidgen. Assumes the local stack, both services and the JWT key (see README.md).
# A 503 or 429 is retried with the *same* body, i.e. the same eventIds, as the API contract requires.
set -euo pipefail

INGEST_URL="${INGEST_URL:-http://localhost:8080}"
STATS_URL="${STATS_URL:-http://localhost:8081}"
EVENTS="${1:-100}"
suffix=$(uuidgen | tr 'A-Z' 'a-z' | cut -c1-8)   # fresh tenants, so earlier runs affect neither counts nor limits
TENANT="smoke-$suffix"
OTHER_TENANT="smoke-other-$suffix"
LIMITED_TENANT="smoke-limited-$suffix"
TIMEOUT_SECONDS=30

if (( EVENTS < 1 || EVENTS > 500 )); then
  echo "events must be between 1 and 500" >&2
  exit 2
fi

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
from=$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)
to=$(date -u -v+10M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)
response=$(mktemp)
headers=$(mktemp)
trap 'rm -f "$response" "$headers"' EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

token() { java scripts/DevJwt.java token "$1"; }

# batch <size> [type]: a request body with <size> new events
batch() {
  for _ in $(seq "$1"); do uuidgen | tr 'A-Z' 'a-z'; done \
    | jq -R . | jq -s --arg at "$now" --arg type "${2:-smoke.test}" \
      '{events: map({eventId: ., type: $type, occurredAt: $at, value: 1})}'
}

# post <token> <body>: prints the status code; the response body and headers go to $response and $headers
post() {
  curl -sS -o "$response" -D "$headers" -w '%{http_code}' -X POST "$INGEST_URL/v1/events" \
    -H 'Content-Type: application/json' -H "Authorization: Bearer $1" --data-binary "$2"
}

# accept <token> <body>: POSTs until 202. A 503 or 429 is retried with the same body.
accept() {
  for attempt in 1 2 3 4 5; do
    status=$(post "$1" "$2")
    if [[ "$status" == "202" ]]; then
      return 0
    elif [[ ( "$status" == "503" || "$status" == "429" ) && $attempt -lt 5 ]]; then
      echo "   $status, retrying the same batch (attempt $attempt)"
      sleep 1
    else
      fail "unexpected status $status: $(cat "$response")"
    fi
  done
}

# stats <token> [curl options]
stats() {
  curl -sS -G "$STATS_URL/v1/stats" -H "Authorization: Bearer $1" "${@:2}" \
    --data-urlencode "from=$from" --data-urlencode "to=$to"
}

echo "1. POST without a token"
status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$INGEST_URL/v1/events" \
  -H 'Content-Type: application/json' --data-binary '{"events":[]}')
[[ "$status" == "401" ]] || fail "expected 401, got $status"
echo "   401"

echo "2. POST $EVENTS events for tenant $TENANT, the same batch again, then a marker event"
token_a=$(token "$TENANT")
body=$(batch "$EVENTS")
accept "$token_a" "$body"
echo "   202 $(cat "$response")"
accept "$token_a" "$body"
echo "   202 again: every eventId is in Kafka twice"
# Same tenant, so same partition: the marker is behind both copies.
accept "$token_a" "$(batch 1 smoke.marker)"
echo "   202 marker"

echo "3. Three 500-event batches back to back for free-plan tenant $LIMITED_TENANT (burst 1000)"
token_l=$(token "$LIMITED_TENANT")
bodies=("$(batch 500)" "$(batch 500)" "$(batch 500)")   # built first, so the requests follow each other closely
statuses=()
for b in "${bodies[@]}"; do
  statuses+=("$(post "$token_l" "$b")")
done
echo "   ${statuses[*]}"
[[ "${statuses[*]}" == "202 202 429" ]] || fail "expected 202 202 429: $(cat "$response")"
limit=$(jq -r '.limit' "$response")
retry_after=$(awk 'tolower($1) == "retry-after:" { print $2 }' "$headers" | tr -d '\r')
[[ "$limit" == "rate" && -n "$retry_after" ]] || fail "expected limit rate and Retry-After: $(cat "$response")"
echo "   429 limit=$limit Retry-After=$retry_after"

echo "4. Polling $STATS_URL/v1/stats until the marker is counted, then expecting totalCount = $EVENTS for smoke.test"
deadline=$((SECONDS + TIMEOUT_SECONDS))
marker=none
while (( SECONDS < deadline )); do
  marker=$(stats "$token_a" --data-urlencode type=smoke.marker | jq -r '.totalCount // 0')
  [[ "$marker" == "1" ]] && break
  sleep 0.5
done
[[ "$marker" == "1" ]] || fail "the marker was not counted within ${TIMEOUT_SECONDS}s (last totalCount $marker)"
count=$(stats "$token_a" --data-urlencode type=smoke.test | jq -r '.totalCount // 0')
[[ "$count" == "$EVENTS" ]] || fail "expected $EVENTS smoke.test events (each sent twice, counted once), got $count"
echo "   $TENANT sees $count events: the second copy of each was skipped"

other=$(stats "$(token "$OTHER_TENANT")" -H "X-Tenant-Id: $TENANT")
[[ "$(jq -r '.tenantId' <<<"$other")" == "$OTHER_TENANT" && "$(jq -r '.totalCount' <<<"$other")" == "0" ]] \
  || fail "$OTHER_TENANT must see only its own statistics, got $other"
echo "   $OTHER_TENANT (sending X-Tenant-Id: $TENANT) sees only its own statistics: totalCount 0"

echo "OK"
