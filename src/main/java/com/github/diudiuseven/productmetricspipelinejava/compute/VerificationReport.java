package com.github.diudiuseven.productmetricspipelinejava.compute;

import java.time.Instant;

/**
 * One persisted consistency-check outcome. {@code diffs} is a JSON document
 * locating divergences down to the concrete user and dimension group.
 */
public record VerificationReport(long reportId, String metricId, int version, Instant checkedAt,
                                 long logSeq, VerificationStatus status, String diffs, Long commitId) {
}
