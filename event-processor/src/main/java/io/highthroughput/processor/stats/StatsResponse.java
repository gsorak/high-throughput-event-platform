package io.highthroughput.processor.stats;

import io.highthroughput.processor.aggregation.EventStatsRepository.MinuteBucket;

import java.time.Instant;
import java.util.List;

/**
 * Body of {@code GET /v1/stats}. Totals cover the whole range even when
 * {@code truncated} is true.
 *
 * @param type null when not filtered by type
 */
public record StatsResponse(String tenantId, Instant from, Instant to, String type,
        long totalCount, double totalValueSum, List<Bucket> buckets, boolean truncated) {

    public record Bucket(Instant bucketStart, String type, long count, double valueSum) {

        static Bucket from(MinuteBucket bucket) {
            return new Bucket(bucket.bucketStart(), bucket.type(), bucket.count(), bucket.valueSum());
        }
    }
}
