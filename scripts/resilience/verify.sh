#!/usr/bin/env bash
# Checks one resilience run against Kafka and PostgreSQL. run.sh calls it after the backlog has drained; it can be
# re-run on saved results as long as the data is still there.
#
#   scripts/resilience/verify.sh load-tests/results/resilience/<stamp>/<scenario>
#
# Needs in that directory: manifest.jsonl (one line per batch, from load-tests/resilience.js), tenants.txt (the run's
# tenants) and dlt-start.txt (end offsets of events.v1.DLT before the run). Writes verify.txt and the detail files
# named there. Exit status: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 missing input.
#
# Expected = every event of an accepted batch (HTTP 202), plus events of ambiguous batches (5xx, timeout) that the
# ledger shows were processed. Rejected batches (4xx, 429) must leave no trace. A 202 or zero lag alone proves
# nothing; the per-event ledger and the per-minute aggregates are compared with the manifest.
set -uo pipefail
cd "$(dirname "$0")/../.."
source scripts/resilience/lib.sh

DIR="${1:?usage: verify.sh <scenario results directory>}"
for f in manifest.jsonl tenants.txt dlt-start.txt; do
  if [[ ! -s "$DIR/$f" ]]; then echo "verify.sh: $DIR/$f is missing or empty" >&2; exit 3; fi
done

fail=()
inconclusive=()

# 1. Kafka: every record of events.v1 was consumed and committed (committed offset = log end on every partition).
kafka kafka-consumer-groups --describe --group "$GROUP" > "$DIR/lag-final.txt" 2>&1
read -r lag unknown < <(awk -v t="$TOPIC" '$2 == t { if ($6 ~ /^[0-9]+$/) lag += $6; else unknown++; seen++ }
                                          END { if (seen) print lag + 0, unknown + 0 }' "$DIR/lag-final.txt")
if [[ -z "${lag:-}" ]]; then
  fail+=("could not read the consumer lag (lag-final.txt)")
elif (( lag != 0 || unknown != 0 )); then
  fail+=("consumer lag is $lag ($unknown partitions without committed offset): not drained")
fi

# 2. Manifest, one line per event: tenant, eventId, minute (UTC), type, value, outcome of its batch.
jq -R -r 'fromjson? | select(type == "object" and has("outcome")) | . as $b | $b.events[]
          | [$b.tenant, .eventId, $b.occurredAt[0:16], .type, .value, $b.outcome] | @tsv' \
  "$DIR/manifest.jsonl" > "$DIR/events.tsv"
jq -R -r 'fromjson? | select(type == "object" and has("statuses")) | .statuses[]' "$DIR/manifest.jsonl" \
  | sort | uniq -c | awk '{ print $2 "\t" $1 }' > "$DIR/http-statuses.tsv"

# 3. PostgreSQL, restricted to this run's tenants (they exist only in this run).
tenants="{$(paste -sd, - < "$DIR/tenants.txt")}"
if ! psql_query "SELECT tenant_id, event_id FROM processed_events WHERE tenant_id = ANY('$tenants'::text[])" \
    > "$DIR/ledger.tsv" \
  || ! psql_query "SELECT tenant_id, to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI'), event_type,
                          event_count, value_sum
                   FROM event_stats_minute WHERE tenant_id = ANY('$tenants'::text[])" > "$DIR/aggregates.tsv"; then
  fail+=("PostgreSQL query failed")
fi

# 4. Compare. Aggregates per (tenant, minute, type): count and sum must match exactly (values are whole numbers).
: > "$DIR/missing.tsv"; : > "$DIR/unexplained.tsv"; : > "$DIR/ambiguous-unresolved.tsv"; : > "$DIR/aggregate-diff.tsv"
awk -v ledger="$DIR/ledger.tsv" -v events="$DIR/events.tsv" -v aggregates="$DIR/aggregates.tsv" \
    -v missingFile="$DIR/missing.tsv" -v unexplainedFile="$DIR/unexplained.tsv" \
    -v unresolvedFile="$DIR/ambiguous-unresolved.tsv" -v diffFile="$DIR/aggregate-diff.tsv" '
  BEGIN { FS = OFS = "\t" }
  FILENAME == ledger { inLedger[$1 SUBSEP $2] = 1; ledgerRows++; next }
  FILENAME == events {
    id = $1 SUBSEP $2
    if (id in outcome) manifestDuplicates++
    outcome[id] = $6
    counted = 0
    if ($6 == "accepted") {
      accepted++; counted = 1
      if (!(id in inLedger)) { missing++; print $1, $2 > missingFile }
    } else if ($6 == "ambiguous") {
      ambiguous++
      if (id in inLedger) { ambiguousDelivered++; counted = 1 } else { ambiguousUnresolved++; print $1, $2 > unresolvedFile }
    } else {
      rejected++
    }
    if (counted) { k = $1 OFS $3 OFS $4; expCount[k]++; expSum[k] += $5; expectedCount++; expectedSum += $5 }
    next
  }
  FILENAME == aggregates { k = $1 OFS $2 OFS $3; actCount[k] = $4; actSum[k] = $5; actualCount += $4; actualSum += $5 }
  END {
    for (id in inLedger) {
      if (!(id in outcome) || outcome[id] == "rejected") {
        unexplained++; split(id, p, SUBSEP); print p[1], p[2], ((id in outcome) ? "rejected" : "not in manifest") > unexplainedFile
      }
    }
    print "tenant", "minute", "type", "expected_count", "expected_sum", "actual_count", "actual_sum" > diffFile
    for (k in expCount) {
      if (!(k in actCount) || actCount[k] + 0 != expCount[k] || actSum[k] + 0 != expSum[k]) {
        mismatches++; print k, expCount[k], expSum[k], actCount[k] + 0, actSum[k] + 0 > diffFile
      }
    }
    for (k in actCount) if (!(k in expCount)) { mismatches++; print k, 0, 0, actCount[k], actSum[k] > diffFile }
    printf "events_accepted\t%d\nevents_rejected\t%d\nevents_ambiguous\t%d\nambiguous_delivered\t%d\n", accepted, rejected, ambiguous, ambiguousDelivered
    printf "ambiguous_unresolved\t%d\nmanifest_duplicate_ids\t%d\nledger_rows\t%d\nmissing\t%d\nunexplained\t%d\n", ambiguousUnresolved, manifestDuplicates, ledgerRows, missing, unexplained
    printf "expected_count\t%d\nexpected_sum\t%.0f\nactual_count\t%d\nactual_sum\t%.0f\naggregate_mismatches\t%d\n", expectedCount, expectedSum, actualCount, actualSum, mismatches
  }' "$DIR/ledger.tsv" "$DIR/events.tsv" "$DIR/aggregates.tsv" > "$DIR/compare.tsv"

value() { awk -F'\t' -v k="$1" '$1 == k { print $2 }' "$DIR/compare.tsv"; }
(( $(value events_accepted) > 0 )) || inconclusive+=("no accepted events: nothing was tested")
(( $(value manifest_duplicate_ids) == 0 )) || inconclusive+=("$(value manifest_duplicate_ids) duplicate eventIds in the manifest (test bug)")
(( $(value missing) == 0 )) || fail+=("$(value missing) accepted events are not in the ledger (missing.tsv)")
(( $(value unexplained) == 0 )) || fail+=("$(value unexplained) ledger entries are not accepted events (unexplained.tsv)")
(( $(value aggregate_mismatches) == 0 )) \
  || fail+=("$(value aggregate_mismatches) (tenant, minute, type) aggregates differ from the manifest (aggregate-diff.tsv)")
(( $(value ambiguous_unresolved) == 0 )) \
  || inconclusive+=("$(value ambiguous_unresolved) events of ambiguous batches are not in the ledger: not published, or lost")

# 5. Dead letters: nothing may have been written to events.v1.DLT during the run.
end_offsets "$DLT" > "$DIR/dlt-end.txt"
awk -F: 'FNR == NR { start[$2] = $3; next } $3 - start[$2] > 0 { print $2, start[$2], $3 - start[$2] }' \
  "$DIR/dlt-start.txt" "$DIR/dlt-end.txt" > "$DIR/dlt-new.txt"
dlt_new=$(awk '{ s += $3 } END { print s + 0 }' "$DIR/dlt-new.txt")
if (( dlt_new > 0 )); then
  fail+=("$dlt_new records were dead-lettered during the run (dlt-records.txt)")
  while read -r partition offset count; do
    kafka kafka-console-consumer --topic "$DLT" --partition "$partition" --offset "$offset" --max-messages "$count" \
      --timeout-ms 10000 --formatter-property print.headers=true --formatter-property print.key=true \
      >> "$DIR/dlt-records.txt" 2>/dev/null
  done < "$DIR/dlt-new.txt"
fi

if (( ${#fail[@]} > 0 )); then verdict=FAIL; status=1
elif (( ${#inconclusive[@]} > 0 )); then verdict=INCONCLUSIVE; status=2
else verdict=PASS; status=0
fi

{
  echo "verdict	$verdict"
  for reason in ${fail[@]+"${fail[@]}"} ${inconclusive[@]+"${inconclusive[@]}"}; do echo "reason	$reason"; done
  echo "final_lag	${lag:-n/a}"
  echo "dlt_new_records	$dlt_new"
  cat "$DIR/compare.tsv"
  awk -F'\t' '{ printf "http_%s\t%s\n", $1, $2 }' "$DIR/http-statuses.tsv"
} > "$DIR/verify.txt"
cat "$DIR/verify.txt"
exit $status
