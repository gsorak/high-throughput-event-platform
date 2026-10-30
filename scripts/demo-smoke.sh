#!/usr/bin/env bash
# Smoke test of the demo gateway (Stage 6) against the running local stack, from the repository root:
#   1. the API refuses requests without a session (401) and state changes without the X-Demo-Client header (403)
#   2. a session is started; its cookie is HttpOnly and no JWT ever appears in a response
#   3. tenant isolation: all five checks pass against the real ingestion API and statistics API
#   4. deduplication: 100 events are aggregated, the replay of the same ids leaves the count unchanged (VERIFIED)
#   5. limits: tenant B gets real 429 rate and quota rejections, and the quota stays exhausted
#   6. a small load run completes only once every accepted event is aggregated
#   7. the system overview answers from Prometheus
#   8. the session ends and is refused afterwards
#
#   scripts/demo-smoke.sh
#
# Needs curl and jq. Assumes the local stack, the ingestion API, the event processor and the demo gateway are running
# (see README.md). Uses one demo session and about 2,800 events of its budget.
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8082}"
jar=$(mktemp)
body=$(mktemp)
headers=$(mktemp)
trap 'rm -f "$jar" "$body" "$headers"' EXIT

fail() { echo "FAILED: $*" >&2; exit 1; }

# call <method> <path> [json]: prints the status code; the response goes to $body and its headers to $headers
call() {
  local args=(-sS -o "$body" -D "$headers" -w '%{http_code}' -b "$jar" -c "$jar" -X "$1" "$GATEWAY_URL$2"
    -H 'X-Demo-Client: 1')
  if [[ $# -ge 3 ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$3")
  fi
  curl "${args[@]}"
}

expect() { # expect <status> <method> <path> [json]
  local want=$1 status
  shift
  status=$(call "$@")
  [[ "$status" == "$want" ]] || fail "$1 $2: expected $want, got $status: $(cat "$body")"
  if grep -Eq 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.' "$body"; then
    fail "$1 $2: the response contains a JWT"
  fi
}

# wait_for <jq condition on the session> <timeout seconds> <description>
wait_for() {
  local deadline=$((SECONDS + $2))
  while ((SECONDS < deadline)); do
    expect 200 GET /demo/session
    if jq -e "$1" "$body" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  fail "timed out waiting for $3: $(cat "$body")"
}

echo "1. Requests without a session or without the client header"
status=$(curl -sS -o /dev/null -w '%{http_code}' "$GATEWAY_URL/demo/session")
[[ "$status" == "401" ]] || fail "GET /demo/session without a cookie: expected 401, got $status"
status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$GATEWAY_URL/demo/session")
[[ "$status" == "403" ]] || fail "POST /demo/session without X-Demo-Client: expected 403, got $status"

echo "2. Start a session"
expect 201 POST /demo/session
grep -i '^set-cookie: demo_session=' "$headers" | grep -qi 'httponly' || fail "the session cookie is not HttpOnly"
grep -i '^set-cookie: demo_session=' "$headers" | grep -qi 'samesite=strict' || fail "the session cookie is not SameSite=Strict"
tenant_a=$(jq -r '.tenants[] | select(.key == "a") | .id' "$body")
tenant_b=$(jq -r '.tenants[] | select(.key == "b") | .id' "$body")
echo "   tenants $tenant_a and $tenant_b"
expect 200 POST /demo/session
[[ "$(jq -r '.tenants[0].id' "$body")" == "$tenant_a" ]] || fail "a second start did not return the same session"

echo "3. Tenant isolation"
expect 200 POST /demo/isolation/check
jq -r '.isolation.checks[] | "   \(if .passed then "pass" else "FAIL" end)  \(.actualStatus)  \(.title)"' "$body"
jq -e '.isolation.allPassed' "$body" >/dev/null || fail "an isolation check failed"

echo "4. Deduplication"
expect 200 POST /demo/dedup/send
wait_for '.dedup.phase != "DELIVERING"' 60 "the first delivery"
jq -e '.dedup.phase == "DELIVERED"' "$body" >/dev/null || fail "the first delivery failed: $(jq -c .dedup "$body")"
expect 200 POST /demo/dedup/replay
wait_for '(.dedup.phase == "VERIFIED" or .dedup.phase == "FAILED") and (.busy | index("dedup") | not)' 90 "the replay"
jq -r '.dedup | "   counts \(.countBefore) -> \(.countAfterFirst) -> \(.countAfterReplay), replay HTTP \(.replayStatus), duplicates skipped \(.duplicatesSkipped)"' "$body"
jq -e '.dedup.phase == "VERIFIED" and .dedup.countAfterReplay == .dedup.countAfterFirst' "$body" >/dev/null \
  || fail "the replay changed the count: $(jq -c .dedup "$body")"

echo "5. Rate limits and quotas (tenant B)"
expect 200 POST /demo/limits/within
jq -e '.rateLimits.attempts[0].status == 202' "$body" >/dev/null || fail "50 events were not accepted"
expect 200 POST /demo/limits/burst
jq -e '[.rateLimits.attempts[] | select(.step == "burst" and .status == 429 and .limit == "rate")] | length > 0' "$body" \
  >/dev/null || fail "the burst was not rate limited: $(jq -c .rateLimits "$body")"
expect 200 POST /demo/limits/quota
jq -e '.rateLimits.quotaExhausted and .rateLimits.quotaRemaining == 0 and .rateLimits.attempts[0].status == 429 and .rateLimits.attempts[0].limit == "quota"' \
  "$body" >/dev/null || fail "the quota was not exhausted: $(jq -c .rateLimits "$body")"
jq -r '.rateLimits | "   accepted \(.acceptedEvents) events, then 429 quota; resets at \(.quotaResetsAt)"' "$body"
expect 200 POST /demo/limits/within
jq -e '.rateLimits.attempts[0].status == 429 and .rateLimits.attempts[0].limit == "quota"' "$body" >/dev/null \
  || fail "the quota was reset"

echo "6. A small load run (200 events/s for 3 s)"
expect 200 POST /demo/runs '{"type":"api.call","eventsPerSecond":200,"durationSeconds":3}'
wait_for '.run.phase != "SENDING" and .run.phase != "PROCESSING"' 150 "the run"
jq -r '.run | "   \(.phase): submitted \(.submitted), accepted \(.accepted), rejected \(.rejected), aggregated \(.processed), pending \(.pending)"' "$body"
jq -e '.run.phase == "COMPLETED" and .run.accepted == 600 and .run.rejected == 0 and .run.processed == .run.accepted' "$body" >/dev/null \
  || fail "the run did not complete: $(jq -c '.run | del(.timeline)' "$body")"

echo "7. System overview"
expect 200 GET /demo/overview
jq -r '"   backlog \(.backlog), ingested (\(.window)) \(.ingestedTotal), processed (\(.window)) \(.processedTotal), peak ingest \(.ingestPeakRate)/s, duplicates skipped \(.duplicatesSkipped), 429s \(.rejectedRequests)"' "$body"
jq -e '.available' "$body" >/dev/null || fail "the overview is unavailable"
expect 200 GET '/demo/overview?window=15m'
jq -e '.window == "15m" and (.series | length) > 0' "$body" >/dev/null || fail "the 15-minute overview has no series"

echo "8. End the session"
expect 204 DELETE /demo/session
expect 401 GET /demo/session

echo "OK"
