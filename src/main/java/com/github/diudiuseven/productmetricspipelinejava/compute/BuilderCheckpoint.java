package com.github.diudiuseven.productmetricspipelinejava.compute;

import java.time.Instant;

/**
 * Durable resume position for one (metric, version) builder.
 */
public record BuilderCheckpoint(
        String metricId,
        int version,
        long logSeq,
        long lastCommitId,
        Instant updatedAt
) {
}
