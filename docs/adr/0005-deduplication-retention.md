# ADR-0005: Deduplication retention derived from the replay policy

- Status: Accepted (2026-10-27); amended and implemented in Stage 3 (see [Amendment](#amendment-stage-3))
- Stage: 3
- Assessment: **Design-time.** The approval of the architecture asked for D to follow from the replay policy rather
  than from the timestamp rule.

## Context

Stage 3 records `(tenant_id, event_id, processed_at)` in a ledger and counts an event only if the insert succeeds.
The ledger grows with every event, so it needs a retention period **D**. A duplicate arriving after its ledger row
was deleted is counted again, silently.

The 7-day `occurredAt` validation rule is _not_ a sufficient bound: duplicates also come from replays and redrives
that ignore `occurredAt` (see [delivery-guarantees.md](../architecture/delivery-guarantees.md)).

## Decision

D must cover the **largest window in which the same event can be delivered again**, measured from the first copy's
`processed_at`:

| Source of a second delivery                                           | Window                                                                                   | Stage 1 values                  |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------- |
| Kafka replay (consumer-group reset to earliest, rebalance redelivery) | `retention.ms` + `segment.ms` (a segment is deleted only when its newest record expires) | 7 d + 1 d = 8 d                 |
| Client retry of a non-202 batch                                       | `occurredAt` window (7 d back + 5 min ahead) + maximum consumer lag                      | ≈ 7 d + lag                     |
| DLT redrive after a fix (Stage 3)                                     | DLT `retention.ms` + `segment.ms`                                                        | 13 d + 1 d = 14 d               |
| Restore of PostgreSQL from backup, then replay from the restore point | Detection time + replay                                                                  | must be ≤ topic retention (8 d) |

**D = max(8 d, 7 d + lag, 14 d) + 7 d margin = 21 days**, measured from `processed_at`. Implemented as an
unpartitioned table whose rows older than 21 days are deleted in batches (originally: daily partitions, see the
amendment).

## Rules that keep D valid

1. Topic retention + segment time, DLT retention and the client retry window must each stay **below D**; changing any
   of them requires revisiting this ADR.
2. Replays of data older than D are **prohibited** in normal operation. If unavoidable, they require the documented
   rebuild procedure: rebuild the affected aggregates from the topic into empty tables, not on top of existing rows.
3. Restoring the database from a backup requires resetting consumer offsets to the restore point; this only works
   while the topic still holds that range, so the restore must happen within topic retention.

## Consequences

- Ledger size ≈ 21 days × daily event volume (UUIDv7 keeps index inserts append-friendly). Sized in Stage 4.
- Duplicates outside D are counted twice; this is a documented, tested boundary rather than an accident
  (`DuplicateDeliveryIT.duplicatesAreSkippedOnlyWhileTheLedgerRowIsRetained`).

## Amendment (Stage 3)

**Change:** `processed_events` is one unpartitioned table with primary key `(tenant_id, event_id)` and an index on
`processed_at` (`V2__create_processed_events.sql`). `LedgerRetentionJob` deletes rows with `processed_at` older than
21 days, in batches of 10,000, hourly in every processor instance. D, the rules above and the 21 days are unchanged.

**Why:** the daily partitions above cannot enforce the decision. PostgreSQL requires a unique constraint on a
partitioned table to include the partition key, so a ledger partitioned by `processed_at` could only reject a copy
that arrives on the same day as the original. Partitioning by a hash of the key would keep uniqueness but no longer
let whole days be dropped.

**Cost:** deleting rows creates dead tuples that autovacuum must clean up, instead of a cheap `DROP` of a
partition. At local volumes this is negligible; Stage 4 measures it, and partitioning by `event_id` time (UUIDv7)
remains an option if the delete load matters.

**DLT window:** `events.v1.DLT` keeps records 13 days plus one daily segment, so at most 14 days, the value in the
table above.

**Known limits of the window**, documented rather than engineered away:

- Rows are deleted no earlier than D after processing; a late or missed job run only keeps them longer.
- Two windows can add up. A record redriven from the DLT on day 14 is back in `events.v1` for another 8 days; a
  consumer-group reset in that time delivers it up to 22 days after the original was counted. Redrive soon after a
  fix, and treat a replay that includes redriven records like rule 2.
- The same `eventId` with different content (a client bug) is a duplicate too: the first copy counted wins.

## References

- Kafka topic configuration (`retention.ms`, `segment.ms`): https://kafka.apache.org/documentation/#topicconfigs
- PostgreSQL table partitioning, including the unique-constraint limitation:
  https://www.postgresql.org/docs/17/ddl-partitioning.html
- UUID version 7 (time-ordered): https://www.rfc-editor.org/rfc/rfc9562
