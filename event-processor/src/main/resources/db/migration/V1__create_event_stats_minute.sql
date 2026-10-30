-- Per-minute aggregates, one row per (tenant, minute, event type).
--
-- Primary key order (tenant_id, bucket_start, event_type) serves both access paths with one index:
--   * the upsert looks up the exact key;
--   * GET /v1/stats scans one tenant's time range (optionally filtered by type) in bucket order.
-- Every query includes tenant_id, so one tenant's rows are never read for another.
CREATE TABLE event_stats_minute (
    tenant_id    text             NOT NULL,
    bucket_start timestamptz      NOT NULL,  -- occurredAt truncated to the minute (UTC)
    event_type   text             NOT NULL,
    event_count  bigint           NOT NULL,  -- every delivered event; duplicates count again until Stage 3
    value_sum    double precision NOT NULL,  -- sum of "value"; events without a value add 0
    CONSTRAINT event_stats_minute_pk PRIMARY KEY (tenant_id, bucket_start, event_type)
);
