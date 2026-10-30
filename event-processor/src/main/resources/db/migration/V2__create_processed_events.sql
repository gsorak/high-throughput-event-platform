-- Deduplication ledger (ADR-0005): one row per counted event. An event is added to event_stats_minute only in the
-- transaction that inserts its row here, so a redelivered copy finds the row and is skipped.
--
-- Not partitioned: in PostgreSQL a unique key on a partitioned table must include the partition column, so daily
-- partitions by processed_at could not reject a copy that arrives on a later day.
CREATE TABLE processed_events (
    tenant_id    text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT processed_events_pk PRIMARY KEY (tenant_id, event_id)
);

-- Retention deletes rows older than 21 days (LedgerRetentionJob); a copy arriving after that is counted again.
CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);
