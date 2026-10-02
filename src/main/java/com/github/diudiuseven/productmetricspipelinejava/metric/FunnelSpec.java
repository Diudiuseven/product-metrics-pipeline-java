package com.github.diudiuseven.productmetricspipelinejava.metric;

import java.time.Duration;
import java.util.List;

/**
 * Funnel definition: ordered {@link #steps} that must occur within
 * {@link #window} (measured from the first step or the previous step).
 */
public record FunnelSpec(
        List<StepDefinition> steps,
        Duration window,
        WindowFrom windowFrom,
        List<String> dimensions,
        String bucketGranularity
) {
    public enum WindowFrom { FIRST_STEP, PREVIOUS_STEP }

    public FunnelSpec {
        steps = steps == null ? List.of() : List.copyOf(steps);
        dimensions = dimensions == null ? List.of() : List.copyOf(dimensions);
    }
}
