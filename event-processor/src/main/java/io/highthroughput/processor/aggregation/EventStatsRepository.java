package io.highthroughput.processor.aggregation;

import io.highthroughput.contract.EventEnvelope;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * SQL for {@code event_stats_minute} and the deduplication ledger
 * {@code processed_events}, written by hand with
 * Spring's {@link JdbcClient} (ADR-0007): the upsert is the hot path and should
 * be visible exactly as PostgreSQL
 * executes it.
 *
 * <p>
 * Timestamps are bound as {@link OffsetDateTime} in UTC, the type the
 * PostgreSQL driver maps to
 * {@code timestamptz}.
 */
@Repository
public class EventStatsRepository {

    /**
     * Inserts nothing, and so returns 0 rows, when the event was counted before
     * (ADR-0005).
     */
    private static final String CLAIM = """
            INSERT INTO processed_events (tenant_id, event_id)
            VALUES (:tenantId, :eventId)
            ON CONFLICT (tenant_id, event_id) DO NOTHING
            """;

    // PostgreSQL's DELETE has no LIMIT; the subquery keeps each batch, and so each
    // transaction, small.
    private static final String DELETE_PROCESSED_BEFORE = """
            DELETE FROM processed_events
             WHERE (tenant_id, event_id) IN (SELECT tenant_id, event_id
                                               FROM processed_events
                                              WHERE processed_at < :cutoff
                                              LIMIT :limit)
            """;

    /**
     * Insert the bucket row, or add to it if it exists.
     * {@code ON CONFLICT ... DO UPDATE} is atomic per row, so
     * concurrent consumers updating the same bucket do not lose increments.
     * {@code EXCLUDED} is the row that failed
     * to insert.
     */
    private static final String UPSERT = """
            INSERT INTO event_stats_minute (tenant_id, bucket_start, event_type, event_count, value_sum)
            VALUES (:tenantId, :bucketStart, :type, 1, :value)
            ON CONFLICT (tenant_id, bucket_start, event_type) DO UPDATE
               SET event_count = event_stats_minute.event_count + 1,
                   value_sum   = event_stats_minute.value_sum + EXCLUDED.value_sum
            """;

    // Two fixed statements instead of one with "(:type IS NULL OR event_type =
    // :type)": each gets a plan that
    // matches its filter, and the SQL stays readable.
    private static final String BUCKETS = """
            SELECT bucket_start, event_type, event_count, value_sum
              FROM event_stats_minute
             WHERE tenant_id = :tenantId AND bucket_start >= :from AND bucket_start < :to
             ORDER BY bucket_start, event_type
             LIMIT :limit
            """;

    private static final String BUCKETS_OF_TYPE = """
            SELECT bucket_start, event_type, event_count, value_sum
              FROM event_stats_minute
             WHERE tenant_id = :tenantId AND bucket_start >= :from AND bucket_start < :to AND event_type = :type
             ORDER BY bucket_start
             LIMIT :limit
            """;

    private static final String TOTALS = """
            SELECT COALESCE(SUM(event_count), 0) AS total_count, COALESCE(SUM(value_sum), 0) AS total_value_sum
              FROM event_stats_minute
             WHERE tenant_id = :tenantId AND bucket_start >= :from AND bucket_start < :to
            """;

    private static final String TOTALS_OF_TYPE = TOTALS.stripTrailing() + " AND event_type = :type\n";

    private final JdbcClient jdbc;

    public EventStatsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Adds the event to its minute bucket unless the ledger already holds its
     * {@code (tenantId, eventId)}. Both
     * statements commit or roll back together: if the upsert fails, the ledger row
     * is gone too, and a later delivery
     * counts the event.
     *
     * @return false if the event was a duplicate and nothing changed
     */
    @Transactional
    public boolean incrementOnce(EventEnvelope event) {
        int claimed = jdbc.sql(CLAIM)
                .param("tenantId", event.tenantId())
                .param("eventId", event.eventId())
                .update();
        if (claimed == 0) {
            return false;
        }
        jdbc.sql(UPSERT)
                .param("tenantId", event.tenantId())
                .param("bucketStart", utc(event.occurredAt().truncatedTo(ChronoUnit.MINUTES)))
                .param("type", event.type())
                .param("value", event.value() == null ? 0.0 : event.value())
                .update();
        return true;
    }

    /**
     * Deletes up to {@code limit} ledger rows processed before {@code cutoff} and
     * returns how many it deleted.
     */
    public int deleteProcessedBefore(Instant cutoff, int limit) {
        return jdbc.sql(DELETE_PROCESSED_BEFORE)
                .param("cutoff", utc(cutoff))
                .param("limit", limit)
                .update();
    }

    /**
     * Buckets with {@code from <= bucketStart < to}, oldest first, at most
     * {@code limit}. {@code type} may be null.
     */
    public List<MinuteBucket> findBuckets(String tenantId, Instant from, Instant to, String type, int limit) {
        var statement = jdbc.sql(type == null ? BUCKETS : BUCKETS_OF_TYPE)
                .param("tenantId", tenantId)
                .param("from", utc(from))
                .param("to", utc(to))
                .param("limit", limit);
        if (type != null) {
            statement = statement.param("type", type);
        }
        return statement.query(EventStatsRepository::toBucket).list();
    }

    /** Totals over the same range as {@link #findBuckets}, never truncated. */
    public Totals totals(String tenantId, Instant from, Instant to, String type) {
        var statement = jdbc.sql(type == null ? TOTALS : TOTALS_OF_TYPE)
                .param("tenantId", tenantId)
                .param("from", utc(from))
                .param("to", utc(to));
        if (type != null) {
            statement = statement.param("type", type);
        }
        return statement.query((rs, row) -> new Totals(rs.getLong("total_count"), rs.getDouble("total_value_sum")))
                .single();
    }

    private static MinuteBucket toBucket(ResultSet rs, int row) throws SQLException {
        return new MinuteBucket(
                rs.getObject("bucket_start", OffsetDateTime.class).toInstant(),
                rs.getString("event_type"),
                rs.getLong("event_count"),
                rs.getDouble("value_sum"));
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    public record MinuteBucket(Instant bucketStart, String type, long count, double valueSum) {
    }

    public record Totals(long count, double valueSum) {
    }
}
