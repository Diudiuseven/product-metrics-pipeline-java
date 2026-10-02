package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;


/**
 * Mutable in-memory state of one (metric id, version) aggregator.
 *
 * <p>Per-user rows store all events relevant to this metric sorted by
 * (eventTime, seq). Keeping the raw rows makes every correction a pure
 * function of the durable log, which is what makes incremental results exactly
 * equal to a from-scratch recomputation.
 */
public abstract class AggregatorState {

    protected final MetricDefinition definition;
    protected long commitSeq = -1L;

    protected AggregatorState(MetricDefinition definition) {
        this.definition = definition;
    }

    public final MetricDefinition definition() {
        return definition;
    }

    public final long commitSeq() {
        return commitSeq;
    }

    /** Apply one already-deduplicated, log-ordered record. */
    public abstract void apply(
            com.github.diudiuseven.productmetricspipelinejava.store.EventRecord record);

    /** Number of per-user rows (for resource-bound accounting). */
    public abstract int userCount();

    /** Number of stored events (for resource-bound accounting). */
    public abstract long eventCount();

}
