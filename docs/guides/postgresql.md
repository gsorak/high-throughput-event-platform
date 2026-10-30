# PostgreSQL access: Flyway, JdbcClient, HikariCP

The processor writes per-minute aggregates and serves `GET /v1/stats` from them. Why plain SQL instead of JPA:
[ADR-0007](../adr/0007-spring-jdbc-over-jpa.md). Why the stats endpoint shares the processor's pool:
[ADR-0006](../adr/0006-stats-endpoint-in-processor.md).

## Flyway: schema migrations

Flyway applies the versioned SQL files in `event-processor/src/main/resources/db/migration` at startup, in order
(`V1__...`, `V2__...`), and records each in `flyway_schema_history`. A migration that was applied is never edited;
changes go into a new file. Flyway refuses to start if an applied file's checksum changed.

Spring Boot runs Flyway before the rest of the application, so the listener never sees a missing table. PostgreSQL
support is a separate artifact since Flyway 10 (`flyway-database-postgresql`).

## The table and the upsert

[`V1__create_event_stats_minute.sql`](../../event-processor/src/main/resources/db/migration/V1__create_event_stats_minute.sql):
one row per `(tenant_id, bucket_start, event_type)`, which is also the primary key. The `GET /v1/stats` queries filter
on `tenant_id` and a `bucket_start` range, which is a prefix of that key, so they use the primary-key index.

Each event runs one statement ([`EventStatsRepository`](../../event-processor/src/main/java/io/highthroughput/processor/aggregation/EventStatsRepository.java)):

```sql
INSERT INTO event_stats_minute (tenant_id, bucket_start, event_type, event_count, value_sum)
VALUES (:tenantId, :bucketStart, :type, 1, :value)
ON CONFLICT (tenant_id, bucket_start, event_type) DO UPDATE
   SET event_count = event_stats_minute.event_count + 1,
       value_sum   = event_stats_minute.value_sum + EXCLUDED.value_sum
```

`ON CONFLICT ... DO UPDATE` is atomic per row: two consumers adding to the same bucket at the same time both take
effect, because the second waits for the first's row lock. `EXCLUDED` is the row that would have been inserted.

**What can go wrong**

- _Throughput_: each event is its own transaction (since Stage 3: ledger insert and upsert), so each event costs
  network round trips and a WAL flush. That capped the Stage 1 processor at ~1,400 events/s locally
  ([baseline](../performance/00-baseline.md)).
- _Row contention_: all events of one tenant, minute and type update the same row. With `tenantId` as the partition
  key, one tenant's events are handled by one consumer thread, so these updates rarely wait for each other. A key that
  spreads a tenant over partitions would make them contend. Stage 4 measures this (ADR-0003).
- _Duplicates_: the upsert alone is not idempotent. Since Stage 3 each event first inserts `(tenant_id, event_id)`
  into `processed_events` in the same transaction (`INSERT … ON CONFLICT DO NOTHING`); if no row was inserted, the
  event was counted before and the upsert is skipped. `LedgerRetentionJob` deletes rows older than 21 days in batches
  of 10,000 ([ADR-0005](../adr/0005-deduplication-retention.md)).

## JdbcClient

`JdbcClient` (Spring Framework 6.1+) is a fluent wrapper over `JdbcTemplate`: `jdbc.sql(...).param(...).update()` or
`.query(mapper).list()`. Named parameters (`:tenantId`) are bound with `PreparedStatement`, never concatenated, so
input cannot change the SQL. Timestamps are bound as `OffsetDateTime` in UTC, which the PostgreSQL driver maps to
`timestamptz` without guessing a time zone.

`spring.jdbc.template.query-timeout: 5s` sets `Statement.setQueryTimeout` on every statement. A slow stats query is
cancelled and returns its connection instead of holding it while consumers wait.

## HikariCP: the connection pool

Opening a PostgreSQL connection costs milliseconds and a server process, so connections are pooled. HikariCP keeps up
to `maximum-pool-size` (10) open. A thread that needs one while all are in use **waits** (up to
`connection-timeout`, default 30 s), then fails.

Sizing: at least listener concurrency (3), plus headroom for stats queries. Watch:

- `hikaricp_connections_pending`: threads waiting right now. Should be 0. Sustained values mean the pool is a
  bottleneck.
- `hikaricp_connections_acquire_seconds_max`: worst wait in the last interval.
- `hikaricp_connections_active`: connections in use.

The baseline never saw a pending thread; the limit was round trips per event, not pool size.

## Useful commands

```sh
docker compose -f infra/local/compose.yaml exec postgres psql -U events -d events
```

```sql
SELECT * FROM flyway_schema_history;
SELECT tenant_id, sum(event_count) FROM event_stats_minute GROUP BY tenant_id ORDER BY 2 DESC LIMIT 10;
SELECT pid, state, wait_event_type, wait_event, query FROM pg_stat_activity WHERE datname = 'events';
```
