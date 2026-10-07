package com.github.diudiuseven.productmetricspipelinejava.verify;

import java.time.Instant;

/**
 * One persisted verification result. {@code diffDetail} is a bounded JSON array
 * of {scope, key, field, expected, actual} entries localised to a concrete user
 * or a concrete (bucket|cohort, dims) group.
 */
public record ConsistencyReport(
        long reportId,
        String metricId,
        int version,
        long commitId,
        long logSeq,
        VerificationStatus status,
        ConsistencyFailureReason reason,
        long diffCount,
        String diffDetail,
        Instant createdAt
) {
}
