package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.stereotype.Service;

/**
 * Full recompute: builds a scratch snapshot from the whole event log and
 * publishes it atomically. New events arriving during recompute are logged and
 * applied afterwards; nothing is lost.
 */
@Service
public class RecomputeService {

    private final ReplayEngine engine;
    private final AggregateStore store;

    public RecomputeService(ReplayEngine engine, AggregateStore store) {
        this.engine = engine;
        this.store = store;
    }

    /** Recomputes the metric from scratch and returns the new commit id. */
    public long recompute(MetricDefinition def, String trigger) {
        // TODO implement
        return 0L;
    }

    /** True while a recompute task for the metric is running. */
    public boolean isRecomputing(String metricId, int version) {
        // TODO implement
        return false;
    }
}
