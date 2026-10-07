package com.github.diudiuseven.productmetricspipelinejava.verify;

import java.time.Instant;

/**
 * Publish gate for one (metric, version). While BLOCKED, no new incremental
 * result is published; a full recompute is still allowed and re-verified, and
 * only a passing verification re-opens the gate.
 */
public record MetricGate(
        String metricId,
        int version,
        boolean blocked,
        ConsistencyFailureReason reason,
        Instant updatedAt
) {
    public static MetricGate open(String metricId, int version) {
        return new MetricGate(metricId, version, false, null, Instant.EPOCH);
    }
}
