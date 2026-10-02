package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.stereotype.Component;

/**
 * Incremental builder for one (metric, version). Re-evaluates affected users
 * from their full event history, so its published snapshot is always identical
 * to a full replay over the same input.
 */
@Component
public class IncrementalBuilder {

    private final ReplayEngine engine;
    private final AggregateStore store;

    public IncrementalBuilder(ReplayEngine engine, AggregateStore store) {
        this.engine = engine;
        this.store = store;
    }

    /**
     * Runs one incremental tick for the metric. No-op if nothing new arrived.
     * Returns the new commit id, or 0 if no commit was produced.
     */
    public long runIncremental(MetricDefinition def) {
        // TODO implement
        return 0L;
    }
}
