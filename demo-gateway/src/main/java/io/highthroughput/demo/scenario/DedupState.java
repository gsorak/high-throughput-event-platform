package io.highthroughput.demo.scenario;

import java.time.Instant;
import java.util.List;

/**
 * Latest state of the deduplication scenario of a session.
 *
 * @param phase             where the scenario stands
 * @param batchSize         events in the batch
 * @param sampleEventIds    a few of the batch's event ids, to show that the replay reuses them
 * @param countBefore       aggregated {@code dedup.check} events before the first delivery
 * @param countAfterFirst   aggregated {@code dedup.check} events once the first delivery was processed
 * @param replayStatus      HTTP status of the replay; 202 means only that Kafka accepted the copies
 * @param countAfterReplay  aggregated {@code dedup.check} events once the replay was processed
 * @param duplicatesSkipped increase of the platform-wide duplicate counter during the replay, if Prometheus reported
 *                          it in time; other traffic can add to it
 * @param message           outcome in plain words
 */
public record DedupState(
        Phase phase,
        int batchSize,
        List<String> sampleEventIds,
        Long countBefore,
        Long countAfterFirst,
        Integer replayStatus,
        Long countAfterReplay,
        Long duplicatesSkipped,
        String message,
        Instant updatedAt) {

    public enum Phase {
        DELIVERING, DELIVERED, REPLAYING, VERIFIED, FAILED
    }

    DedupState with(Phase newPhase, String newMessage, Instant at) {
        return new DedupState(newPhase, batchSize, sampleEventIds, countBefore, countAfterFirst, replayStatus,
                countAfterReplay, duplicatesSkipped, newMessage, at);
    }
}
