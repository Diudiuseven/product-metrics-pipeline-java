package com.github.diudiuseven.productmetricspipelinejava.metric;

import java.util.Map;

/**
 * One funnel step. {@code dimFilters} restricts which events count as this
 * step (event dims must equal all given entries).
 */
public record StepDefinition(
        String id,
        String eventType,
        Map<String, String> dimFilters
) {
    public StepDefinition {
        dimFilters = dimFilters == null ? Map.of() : Map.copyOf(dimFilters);
    }
}
