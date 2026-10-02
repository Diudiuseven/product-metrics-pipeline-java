package com.github.diudiuseven.productmetricspipelinejava.store;

import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;

import java.util.List;

/**
 * Versioned metric-definition store. All versions of every metric id are kept
 * and remain queryable; each id also has an active version pointer.
 */
public interface DefinitionsRepository {

    /**
     * Validate and persist a new definition. Same (id, version) with a changed
     * payload is rejected with field-located problems; lower versions are
     * rejected; the active pointer moves forward only with monotonically higher
     * versions.
     */
    MetricDefinition register(MetricDefinition definition) throws DefinitionException;

    /** All versions of one metric id, ascending. */
    List<MetricDefinition> versions(String metricId);

    MetricDefinition get(String metricId, int version);

    MetricDefinition active(String metricId);

    List<MetricDefinition> allActive();
}
