#!/usr/bin/env bash
# Stage 4 resilience scenarios: overload, PostgreSQL outage, processor crash. See docs/resilience/scenarios.md.
#
#   scripts/resilience/run.sh overload|postgres-outage|processor-crash|all [--mode quick|extended] [--repeat N] [--warmup]
#
# Needs: the local stack running (docker compose -f infra/local/compose.yaml up -d), the jars (./mvnw package),
# the JWT key (java scripts/DevJwt.java keys), JDK 25, Docker, curl and jq. Ports 8080 and 8081 must be free: the
# script starts both services itself, so it can kill and restart the processor, and stops them at the end.
#
# Each scenario: check that the consumer group has no backlog (an earlier run's backlog is drained by the processor
# for at most PRE_DRAIN_TIMEOUT, otherwise it stops with a setup error; offsets are never reset), start k6 with
# deterministic events for fresh tenants, inject the fault, wait until the consumer lag
# is 0, then scripts/resilience/verify.sh compares the manifest with PostgreSQL. Results: load-tests/results/
# resilience/<stamp>/<scenario>/ (summary.txt first). Exit status: 0 all PASS, 1 a FAIL, 2 an INCONCLUSIVE and no
# FAIL, 3 setup error. Kafka data, topics and volumes are never deleted; PostgreSQL is only stopped and started.
#
# --warmup sends WARMUP_PHASES to throwaway tenants after the services start and before the first scenario, so the
# first scenario does not measure cold JVMs. Off by default: a cold start is a case worth seeing too.
#
# Settings (environment variables; defaults per mode in defaults() below). PHASES are "requests/s x events per
# request x seconds", comma-separated:
#   TENANT_COUNT, OVERLOAD_PHASES, OVERLOAD_HOT_EVERY, OVERLOAD_MIN_PEAK_LAG,
#   PG_PHASES, PG_DOWN_AT, PG_DOWN_FOR, CRASH_PHASES, CRASH_KILL_AT (earliest kill; it waits for
#   CRASH_MIN_BACKLOG), CRASH_DOWN_FOR, CRASH_MIN_BACKLOG,
#   RECOVERY_TIMEOUT (s, fault end/load end -> lag 0), SCENARIO_TIMEOUT (s, hard limit per scenario), RETRIES,
#   PRE_DRAIN_TIMEOUT (s, existing backlog -> lag 0 before a scenario), WARMUP_PHASES
set -uo pipefail
cd "$(dirname "$0")/../.."
source scripts/resilience/lib.sh

usage() {
  echo "usage: $0 overload|postgres-outage|processor-crash|all [--mode quick|extended] [--repeat N] [--warmup]" >&2
  exit 3
}

SCENARIO="${1:-}"; shift || true
MODE=quick; REPEAT=""; WARMUP=0
while (( $# > 0 )); do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --repeat) REPEAT="${2:-}"; shift 2 ;;
    --warmup) WARMUP=1; shift ;;
    *) usage ;;
  esac
done
case "$SCENARIO" in
  overload|postgres-outage|processor-crash) SCENARIOS=("$SCENARIO") ;;
  all) SCENARIOS=(overload postgres-outage processor-crash) ;;
  *) usage ;;
esac
[[ "$MODE" == quick || "$MODE" == extended ]] || usage

defaults() {
  # Calibrated on the reference machine (docs/resilience/scenarios.md): the processor drains about 400 events/s.
  : "${TENANT_COUNT:=25}" "${RETRIES:=0}" "${OVERLOAD_HOT_EVERY:=3}"
  # 200 events/s for 15 s, only with --warmup.
  : "${WARMUP_PHASES:=10x20x15}"
  if [[ "$MODE" == quick ]]; then
    # 1,200 events/s for 10 s, then 100 events/s for 10 s.
    : "${OVERLOAD_PHASES:=24x50x10,2x50x10}" "${OVERLOAD_MIN_PEAK_LAG:=5000}"
    : "${PG_PHASES:=10x20x35}" "${PG_DOWN_AT:=10}" "${PG_DOWN_FOR:=15}"
    # Crash at 800 events/s: above the ~500 events/s drain rate (Stage 5), so a backlog builds before the kill.
    : "${CRASH_PHASES:=16x50x12}" "${CRASH_KILL_AT:=5}" "${CRASH_DOWN_FOR:=5}" "${CRASH_MIN_BACKLOG:=1000}"
    : "${RECOVERY_TIMEOUT:=120}" "${SCENARIO_TIMEOUT:=300}" "${REPEAT:=1}" "${PRE_DRAIN_TIMEOUT:=120}"
  else
    : "${OVERLOAD_PHASES:=24x50x60,2x50x60}" "${OVERLOAD_MIN_PEAK_LAG:=20000}"
    : "${PG_PHASES:=10x20x150}" "${PG_DOWN_AT:=30}" "${PG_DOWN_FOR:=60}"
    : "${CRASH_PHASES:=16x50x60}" "${CRASH_KILL_AT:=20}" "${CRASH_DOWN_FOR:=20}" "${CRASH_MIN_BACKLOG:=5000}"
    : "${RECOVERY_TIMEOUT:=300}" "${SCENARIO_TIMEOUT:=900}" "${REPEAT:=3}" "${PRE_DRAIN_TIMEOUT:=300}"
  fi
}
defaults

API_JAR=ingestion-api/target/ingestion-api-0.1.0-SNAPSHOT.jar
PROCESSOR_JAR=event-processor/target/event-processor-0.1.0-SNAPSHOT.jar
# Script-only: a killed consumer is noticed after 10 s instead of the default 45 s. application.yaml is unchanged.
PROCESSOR_ARGS=(--spring.kafka.consumer.properties.session.timeout.ms=10000)
RESULTS="load-tests/results/resilience/$(date +%Y%m%d-%H%M%S)"

API_PID=""; PROCESSOR_PID=""; SAMPLER_PID=""; K6_NAME=""; PG_STOPPED=0

# ---------------------------------------------------------------- cleanup: only what this script started or stopped
stop_pid() {
  local pid=$1
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 0
  kill "$pid" 2>/dev/null
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || return 0; sleep 1; done
  kill -9 "$pid" 2>/dev/null
}

cleanup() {
  trap - EXIT INT TERM
  [[ -n "$SAMPLER_PID" ]] && kill "$SAMPLER_PID" 2>/dev/null
  [[ -n "$K6_NAME" ]] && docker rm -f "$K6_NAME" >/dev/null 2>&1
  if (( PG_STOPPED )); then
    log "Restarting PostgreSQL (stopped by this script)"
    docker compose -f "$COMPOSE" start postgres >/dev/null 2>&1
  fi
  stop_pid "$PROCESSOR_PID"
  stop_pid "$API_PID"
}
trap cleanup EXIT
trap 'log "Interrupted"; exit 130' INT TERM

die() { log "SETUP ERROR: $*" >&2; exit 3; }

# ---------------------------------------------------------------- preflight
preflight() {
  local missing=() service state
  for c in docker jq curl java awk; do command -v "$c" >/dev/null || missing+=("$c"); done
  (( ${#missing[@]} == 0 )) || die "missing commands: ${missing[*]}"
  java -version 2>&1 | grep -q 'version "25' || die "JDK 25 is not first on the PATH"
  [[ -f "$API_JAR" && -f "$PROCESSOR_JAR" ]] || die "jars missing; run ./mvnw package -DskipTests"
  [[ -f .local/jwt/private.pem ]] || die "JWT key missing; run java scripts/DevJwt.java keys"
  for service in kafka postgres valkey kafka-exporter prometheus postgres-exporter; do
    state=$(docker compose -f "$COMPOSE" ps --format '{{.State}} {{.Health}}' "$service" 2>/dev/null)
    [[ "$state" == running* && "$state" != *unhealthy* && "$state" != *starting* ]] \
      || die "compose service $service is '${state:-not created}'; run: docker compose -f $COMPOSE up -d"
  done
  for port in 8080 8081; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      die "port $port is in use. Stop the services you started (or the compose profile apps); this script starts its own"
    fi
  done
}

# ---------------------------------------------------------------- services owned by this script
wait_healthy() {
  local port=$1 pid=$2 timeout=$3 t0=$SECONDS
  while (( SECONDS - t0 < timeout )); do
    kill -0 "$pid" 2>/dev/null || return 1
    curl -sf "http://localhost:$port/actuator/health" | grep -q '"UP"' && return 0
    sleep 1
  done
  return 1
}

start_processor() {
  java -jar "$PROCESSOR_JAR" "${PROCESSOR_ARGS[@]}" >> "$RESULTS/event-processor.log" 2>&1 &
  PROCESSOR_PID=$!
  wait_healthy 8081 "$PROCESSOR_PID" 90
}

start_services() {
  log "Starting event-processor and ingestion-api (logs in $RESULTS)"
  java -jar "$API_JAR" >> "$RESULTS/ingestion-api.log" 2>&1 &
  API_PID=$!
  start_processor || die "event-processor did not become healthy; see $RESULTS/event-processor.log"
  wait_healthy 8080 "$API_PID" 90 || die "ingestion-api did not become healthy; see $RESULTS/ingestion-api.log"
}

# ---------------------------------------------------------------- Kafka lag
# Two zero readings from the broker in a row. The broker CLI starts a JVM per call, so it only runs once Prometheus
# (kafka-exporter, a scrape behind) reads 0 or has no value. Returns 1 after $1 seconds.
wait_drained() {
  local timeout=$1 t0=$SECONDS zeros=0 lag unknown prom
  while (( SECONDS - t0 < timeout )); do
    prom=$(prom_int "$LAG_SUM_QUERY")
    if [[ "$prom" == 0 || "$prom" == n/a ]]; then
      read -r lag unknown < <(group_lag)
      if [[ "${lag:-x}" == 0 && "${unknown:-x}" == 0 ]]; then
        zeros=$((zeros + 1)); (( zeros >= 2 )) && return 0
      else
        zeros=0
      fi
    else
      zeros=0
    fi
    sleep 2
  done
  return 1
}

# Lag from Prometheus (kafka-exporter) every 2 s: epoch, total, worst partition. Keeps working while the processor
# is down. Cheap enough to run during the load, unlike the Kafka CLI.
start_sampler() {
  local file=$1
  (
    while true; do
      printf '%s\t%s\t%s\n' "$(date +%s)" "$(prom_int "$LAG_SUM_QUERY")" "$(prom_int "$LAG_MAX_QUERY")" >> "$file"
      sleep 2
    done
  ) &
  SAMPLER_PID=$!
}

# ---------------------------------------------------------------- one scenario
# --warmup: load on throwaway tenants, not verified. The first scenario's backlog check waits for it to be processed.
warm_up() {
  local dir="$RESULTS/warm-up" run_hex tenants tokens
  mkdir -p "$dir"
  run_hex=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
  tenants=$(seq -f "w${run_hex}-%g" 0 4)
  tokens=$(java scripts/DevJwt.java tokens $tenants | paste -sd, -)
  [[ -n "$tokens" ]] || die "could not create tokens with scripts/DevJwt.java"
  log "Warm-up: $WARMUP_PHASES on throwaway tenants w${run_hex}-*"
  K6_NAME="k6-warmup-$run_hex"
  docker run --rm --name "$K6_NAME" -v "$PWD/load-tests:/scripts:ro" -v "$PWD/$dir:/out" \
    -e RUN_HEX="$run_hex" -e TENANTS="$(echo "$tenants" | paste -sd, -)" -e TOKENS="$tokens" \
    -e PHASES="$WARMUP_PHASES" -e HOT_EVERY=0 -e RETRIES=0 \
    grafana/k6:2.3.0 run --quiet --log-format raw --log-output file=/out/manifest.jsonl /scripts/resilience.js \
    > "$dir/k6.log" 2>&1 || log "Warm-up: k6 exited with status $? (see $dir/k6.log); continuing"
  K6_NAME=""
}

sleep_until() { local now; now=$(date +%s); (( $1 > now )) && sleep $(( $1 - now )); return 0; }

phase_seconds() { echo "$1" | tr ',' '\n' | awk -F'x' -v n="${2:-999}" 'NR <= n { s += $3 } END { print s + 0 }'; }

run_scenario() {
  local name=$1 dir=$2
  local run_hex tenants tokens phases hot_every=0 t_start deadline t0 t_load_end t_fault_end="" t_drained=""
  local notes=() fail=() inconclusive=() lines_api lines_proc t_pre pre_drain=0
  mkdir -p "$dir"
  run_hex=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
  case "$name" in
    overload) phases=$OVERLOAD_PHASES; hot_every=$OVERLOAD_HOT_EVERY ;;
    postgres-outage) phases=$PG_PHASES ;;
    processor-crash) phases=$CRASH_PHASES ;;
  esac

  # Fresh tenants per run, so PostgreSQL rows of this run are exactly the rows of these tenants.
  tenants=$(seq -f "r${run_hex}-%g" 0 $(( TENANT_COUNT - 1 )))
  (( hot_every > 0 )) && tenants+=$'\n'"r${run_hex}-hot"
  echo "$tenants" > "$dir/tenants.txt"
  tokens=$(java scripts/DevJwt.java tokens $tenants | paste -sd, -)
  [[ -n "$tokens" ]] || die "could not create tokens with scripts/DevJwt.java"
  {
    echo "scenario=$name mode=$MODE run=$run_hex phases=$phases hot_every=$hot_every retries=$RETRIES"
    echo "tenants=$TENANT_COUNT recovery_timeout=$RECOVERY_TIMEOUT scenario_timeout=$SCENARIO_TIMEOUT pre_drain_timeout=$PRE_DRAIN_TIMEOUT warmup=$WARMUP"
    case "$name" in
      postgres-outage) echo "pg_down_at=$PG_DOWN_AT pg_down_for=$PG_DOWN_FOR" ;;
      processor-crash) echo "kill_at=$CRASH_KILL_AT down_for=$CRASH_DOWN_FOR min_backlog=$CRASH_MIN_BACKLOG" ;;
      overload) echo "min_peak_lag=$OVERLOAD_MIN_PEAK_LAG" ;;
    esac
  } > "$dir/settings.txt"

  # A backlog left by an earlier run (or the warm-up) would delay this scenario's events and blur its measurements.
  # The processor gets PRE_DRAIN_TIMEOUT to drain it; the time is reported separately, not as scenario time.
  local lag_now unknown_now
  t_pre=$(date +%s)
  read -r lag_now unknown_now < <(group_lag)
  if [[ "$lag_now $unknown_now" != "0 0" ]]; then
    log "[$name] Backlog before the scenario: lag ${lag_now:-unknown}, ${unknown_now:-?} partitions without a committed offset; waiting up to ${PRE_DRAIN_TIMEOUT}s"
    if ! wait_drained "$PRE_DRAIN_TIMEOUT"; then
      read -r lag_now unknown_now < <(group_lag)
      die "consumer group $GROUP still has a backlog after ${PRE_DRAIN_TIMEOUT}s (lag ${lag_now:-unknown}, ${unknown_now:-?} partitions without a committed offset), so $name would not start from a known state. Check that the processor consumes ($RESULTS/event-processor.log), or raise PRE_DRAIN_TIMEOUT. Skipping the backlog instead is a manual decision: scripts/kafka-reset.sh status|skip. This script never resets offsets."
    fi
    pre_drain=$(( $(date +%s) - t_pre ))
    log "[$name] Backlog drained in ${pre_drain}s"
  fi
  t_start=$(date +%s); deadline=$(( t_start + SCENARIO_TIMEOUT ))
  end_offsets "$DLT" > "$dir/dlt-start.txt"
  local dup_before failures_before unclassified_before
  dup_before=$(processor_metric processor_events_duplicate_total)
  failures_before=$(processor_metric processor_delivery_failures_total 'class="transient"')
  unclassified_before=$(processor_metric processor_delivery_failures_total 'class="unclassified"')
  lines_api=$(wc -l < "$RESULTS/ingestion-api.log"); lines_proc=$(wc -l < "$RESULTS/event-processor.log")

  log "[$name] run $run_hex: phases $phases"
  start_sampler "$dir/lag.tsv"
  K6_NAME="k6-resilience-$run_hex"
  { docker run --rm --name "$K6_NAME" -v "$PWD/load-tests:/scripts:ro" -v "$PWD/$dir:/out" \
    -e RUN_HEX="$run_hex" -e TENANTS="$(paste -sd, - < "$dir/tenants.txt")" -e TOKENS="$tokens" \
    -e PHASES="$phases" -e HOT_EVERY="$hot_every" -e RETRIES="$RETRIES" \
    grafana/k6:2.3.0 run --quiet --summary-trend-stats "avg,med,p(95),p(99),max" \
    --log-format raw --log-output file=/out/manifest.jsonl /scripts/resilience.js \
    > "$dir/k6.log" 2>&1; date +%s > "$dir/k6-end.txt"; } &
  local k6_pid=$!

  # t0 = first batch logged. Fault times below are relative to it, not to container start-up.
  while [[ ! -s "$dir/manifest.jsonl" ]]; do
    kill -0 "$k6_pid" 2>/dev/null || die "k6 exited before sending; see $dir/k6.log"
    (( $(date +%s) < deadline )) || die "k6 did not start"
    sleep 0.2
  done
  t0=$(date +%s)
  local load_seconds; load_seconds=$(phase_seconds "$phases")

  case "$name" in
    overload)
      t_fault_end=$(( t0 + $(phase_seconds "$phases" 1) ))
      notes+=("overload phase ended at +$(( t_fault_end - t0 ))s; traffic reduced, then stopped at +${load_seconds}s")
      ;;
    postgres-outage)
      sleep_until $(( t0 + PG_DOWN_AT ))
      log "[$name] Stopping PostgreSQL"
      PG_STOPPED=1
      docker compose -f "$COMPOSE" stop postgres >/dev/null 2>&1
      local t_down; t_down=$(date +%s)
      sleep_until $(( t_down + PG_DOWN_FOR ))
      local t_up_begin; t_up_begin=$(date +%s)
      log "[$name] Starting PostgreSQL"
      docker compose -f "$COMPOSE" start postgres >/dev/null 2>&1
      until docker compose -f "$COMPOSE" exec -T postgres pg_isready -q -U events -d events 2>/dev/null; do
        (( $(date +%s) < deadline )) || { fail+=("PostgreSQL did not come back"); break; }
        sleep 1
      done
      PG_STOPPED=0
      t_fault_end=$(date +%s)
      echo "$t_down $t_up_begin" > "$dir/outage-window.txt"
      notes+=("PostgreSQL down from +$(( t_down - t0 ))s to +$(( t_up_begin - t0 ))s (accepting again at +$(( t_fault_end - t0 ))s)")
      ;;
    processor-crash)
      # Kill once the backlog is built: not before CRASH_KILL_AT, then as soon as the Prometheus lag reaches
      # CRASH_MIN_BACKLOG, at the latest when the load ends. The reading trails the broker by a scrape or two; while
      # the lag is rising that makes it a lower bound.
      sleep_until $(( t0 + CRASH_KILL_AT ))
      local lag_at_kill; lag_at_kill=$(prom_int "$LAG_SUM_QUERY")
      while [[ "$lag_at_kill" == n/a ]] || (( lag_at_kill < CRASH_MIN_BACKLOG )); do
        (( $(date +%s) < t0 + load_seconds )) || break
        sleep 1
        lag_at_kill=$(prom_int "$LAG_SUM_QUERY")
      done
      log "[$name] kill -9 event-processor (pid $PROCESSOR_PID), lag ~$lag_at_kill"
      kill -9 "$PROCESSOR_PID"; wait "$PROCESSOR_PID" 2>/dev/null
      local t_kill; t_kill=$(date +%s)
      sleep "$CRASH_DOWN_FOR"
      log "[$name] Restarting event-processor"
      echo "---- restarted by run.sh after kill -9 ----" >> "$RESULTS/event-processor.log"
      local t_restart; t_restart=$(date +%s)
      start_processor || fail+=("event-processor did not restart")
      t_fault_end=$t_restart
      dup_before=0; failures_before=0; unclassified_before=0   # counters start at 0 in the new process
      echo "$lag_at_kill" > "$dir/lag-at-kill.txt"
      notes+=("killed at +$(( t_kill - t0 ))s with lag ~$lag_at_kill (Prometheus), restarted at +$(( t_restart - t0 ))s, healthy at +$(( $(date +%s) - t0 ))s")
      if [[ "$lag_at_kill" == n/a ]] || (( lag_at_kill < CRASH_MIN_BACKLOG )); then
        inconclusive+=("backlog at kill ($lag_at_kill) below CRASH_MIN_BACKLOG=$CRASH_MIN_BACKLOG")
      fi
      ;;
  esac

  while kill -0 "$k6_pid" 2>/dev/null; do
    if (( $(date +%s) >= deadline )); then
      fail+=("scenario timeout (${SCENARIO_TIMEOUT}s) during load"); docker rm -f "$K6_NAME" >/dev/null 2>&1; break
    fi
    sleep 1
  done
  wait "$k6_pid" 2>/dev/null
  K6_NAME=""
  # When k6 ended, not when this loop noticed: the crash scenario waits for the restarted processor first.
  t_load_end=$(cat "$dir/k6-end.txt" 2>/dev/null || date +%s)

  log "[$name] Load finished after $(( t_load_end - t0 ))s; waiting for lag 0 (at most ${RECOVERY_TIMEOUT}s)"
  local budget=$(( deadline - $(date +%s) )); (( budget > RECOVERY_TIMEOUT )) && budget=$RECOVERY_TIMEOUT
  if (( budget > 0 )) && wait_drained "$budget"; then
    t_drained=$(date +%s)
  else
    fail+=("lag did not return to 0 within ${budget}s after the load (recovery timeout)")
  fi
  sleep 3   # one more sample after the drain
  kill "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=""

  # ---- correctness (manifest vs PostgreSQL, DLT)
  log "[$name] Verifying against PostgreSQL"
  scripts/resilience/verify.sh "$dir" > /dev/null
  local verify_status=$?
  (( verify_status == 3 )) && fail+=("verify.sh could not run (missing manifest?)")
  value() { awk -F'\t' -v k="$1" '$1 == k { print $2; exit }' "$dir/verify.txt" 2>/dev/null; }

  # ---- did the scenario do what it is meant to show?
  local peak_lag peak_at recovered_at=""
  read -r peak_lag peak_at < <(awk -F'\t' '$2 ~ /^[0-9]+$/ && $2 + 0 >= max { max = $2 + 0; at = $1 } END { print max + 0, at + 0 }' "$dir/lag.tsv")
  recovered_at=$(awk -F'\t' -v t="$t_fault_end" '$1 >= t && $2 == "0" { print $1; exit }' "$dir/lag.tsv")
  local dup_after failures_after unclassified_after duplicates transient_failures unclassified_failures
  dup_after=$(processor_metric processor_events_duplicate_total)
  failures_after=$(processor_metric processor_delivery_failures_total 'class="transient"')
  unclassified_after=$(processor_metric processor_delivery_failures_total 'class="unclassified"')
  duplicates=$(( ${dup_after/n\/a/0} - ${dup_before/n\/a/0} ))
  transient_failures=$(( ${failures_after/n\/a/0} - ${failures_before/n\/a/0} ))
  unclassified_failures=$(( ${unclassified_after/n\/a/0} - ${unclassified_before/n\/a/0} ))

  case "$name" in
    overload)
      (( peak_lag >= OVERLOAD_MIN_PEAK_LAG )) \
        || inconclusive+=("peak lag $peak_lag below OVERLOAD_MIN_PEAK_LAG=$OVERLOAD_MIN_PEAK_LAG: not overloaded")
      ;;
    postgres-outage)
      # Ingestion must not depend on PostgreSQL: every batch sent while it was down is accepted (429 excepted).
      if [[ -s "$dir/outage-window.txt" ]]; then
        read -r w_from w_to < "$dir/outage-window.txt"
        jq -R -r --argjson from "$w_from" --argjson to "$w_to" 'fromjson? | select(type == "object" and has("outcome"))
            | select((.occurredAt | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) as $t | $t >= $from and $t < $to)
            | [.outcome, (.statuses | map(tostring) | join(","))] | @tsv' "$dir/manifest.jsonl" \
          | sort | uniq -c > "$dir/outage-batches.txt"
        local sent_during accepted_during
        sent_during=$(awk '{ s += $1 } END { print s + 0 }' "$dir/outage-batches.txt")
        accepted_during=$(awk '$2 == "accepted" { s += $1 } END { print s + 0 }' "$dir/outage-batches.txt")
        notes+=("batches sent while PostgreSQL was down: $sent_during, accepted: $accepted_during")
        if (( sent_during == 0 )); then
          inconclusive+=("no batch was sent while PostgreSQL was down")
        elif (( accepted_during != sent_during )); then
          fail+=("$(( sent_during - accepted_during )) batches sent during the outage were not accepted (outage-batches.txt)")
        fi
      fi
      (( transient_failures > 0 )) \
        || inconclusive+=("no transient processing failure counted: the processor did not hit the outage")
      # An unclassified failure is dead-lettered after 10 attempts; a database outage must not be one.
      (( unclassified_failures == 0 )) \
        || fail+=("$unclassified_failures failed attempts were classified unclassified, not transient (event-processor.log)")
      ;;
    processor-crash)
      (( duplicates > 0 )) || inconclusive+=("no duplicate was skipped after the restart: redelivery was not exercised")
      ;;
  esac

  # ---- verdict and summary
  local verdict status
  if (( verify_status == 1 || ${#fail[@]} > 0 )); then verdict=FAIL; status=1
  elif (( verify_status == 2 || ${#inconclusive[@]} > 0 )); then verdict=INCONCLUSIVE; status=2
  else verdict=PASS; status=0
  fi
  local t_end; t_end=$(date +%s)
  {
    echo "verdict                  : $verdict"
    for r in ${fail[@]+"${fail[@]}"} ${inconclusive[@]+"${inconclusive[@]}"}; do echo "reason                   : $r"; done
    awk -F'\t' '$1 == "reason" { print "reason (verify)          : " $2 }' "$dir/verify.txt" 2>/dev/null
    echo "scenario                 : $name ($MODE), run $run_hex, $(cat "$dir/settings.txt" | head -1 | sed 's/.*phases=//')"
    echo "duration (s)             : $(( t_end - t_start )) total, load $(( t_load_end - t0 ))"
    echo "pre-drain (s)            : $pre_drain (earlier backlog drained before the scenario; not in the duration)"
    for n in ${notes[@]+"${notes[@]}"}; do echo "fault                    : $n"; done
    echo "events accepted          : $(value events_accepted)"
    echo "events rejected (4xx)    : $(value events_rejected)"
    echo "events ambiguous         : $(value events_ambiguous) (in ledger: $(value ambiguous_delivered), unresolved: $(value ambiguous_unresolved))"
    echo "HTTP statuses (batches)  : $(awk -F'\t' '$1 ~ /^http_/ { sub("http_", "", $1); printf "%s=%s ", $1, $2 }' "$dir/verify.txt" 2>/dev/null)"
    echo "expected count / sum     : $(value expected_count) / $(value expected_sum)"
    echo "actual count / sum       : $(value actual_count) / $(value actual_sum)"
    echo "ledger rows              : $(value ledger_rows) (missing $(value missing), unexplained $(value unexplained))"
    echo "aggregate mismatches     : $(value aggregate_mismatches)"
    echo "dead-lettered (DLT)      : $(value dlt_new_records)"
    echo "duplicates skipped       : $duplicates"
    echo "transient DB failures    : $transient_failures"
    echo "unclassified failures    : $unclassified_failures"
    if (( peak_at > 0 )); then
      echo "peak lag (events)        : $peak_lag at +$(( peak_at - t0 ))s"
    else
      echo "peak lag (events)        : no Prometheus samples (lag.tsv)"
    fi
    if [[ -n "$recovered_at" ]]; then
      echo "recovery (s)             : $(( recovered_at - t_fault_end )) from fault end to lag 0 (Prometheus, 5 s resolution)"
    else
      echo "recovery (s)             : lag never 0 in Prometheus samples after the fault"
    fi
    [[ -n "$t_drained" ]] && echo "drained (broker)         : +$(( t_drained - t_load_end ))s after the load ended"
    echo "final lag (broker)       : $(value final_lag)"
  } > "$dir/summary.txt"
  if (( status != 0 )); then
    {
      echo; echo "== consumer group (lag-final.txt)"; cat "$dir/lag-final.txt" 2>/dev/null
      echo; echo "== ingestion-api WARN/ERROR during the scenario"
      tail -n +"$(( lines_api + 1 ))" "$RESULTS/ingestion-api.log" | grep -E ' (WARN|ERROR) ' | tail -20
      echo; echo "== event-processor WARN/ERROR during the scenario"
      tail -n +"$(( lines_proc + 1 ))" "$RESULTS/event-processor.log" | grep -E ' (WARN|ERROR) ' | tail -20
    } >> "$dir/summary.txt"
  fi
  sed 's/^/    /' "$dir/summary.txt" | head -25
  echo "$name	$verdict	$(( t_end - t_start ))s	$dir" >> "$RESULTS/summary.tsv"
  return $status
}

# ---------------------------------------------------------------- main
mkdir -p "$RESULTS"
preflight
start_services
(( WARMUP )) && warm_up
overall=0
for name in "${SCENARIOS[@]}"; do
  for rep in $(seq 1 "$REPEAT"); do
    dir="$RESULTS/$name"; (( REPEAT > 1 )) && dir="$RESULTS/$name-$rep"
    run_scenario "$name" "$dir"
    status=$?
    if (( status == 1 )); then overall=1; elif (( status == 2 && overall == 0 )); then overall=2; fi
  done
done
echo
column -t -s $'\t' "$RESULTS/summary.tsv"
log "Results: $RESULTS"
exit $overall
