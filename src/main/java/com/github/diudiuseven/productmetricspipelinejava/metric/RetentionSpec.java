package com.github.diudiuseven.productmetricspipelinejava.metric;

import java.time.Duration;
import java.util.List;

/**
 * Retention definition: users are anchored on their FIRST occurrence of
 * {@link #anchorEventType}; they are retained for period i if they perform
 * {@link #returnEventType} within the corresponding window.
 */
public record RetentionSpec(
        String anchorEventType,
        String returnEventType,
        List<Duration> periods,
        Duration returnWindow,
        String cohortGranularity,
        List<String> dimensions
) {
    public RetentionSpec {
        periods = periods == null ? List.of() : List.copyOf(periods);
        dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
    }
}
