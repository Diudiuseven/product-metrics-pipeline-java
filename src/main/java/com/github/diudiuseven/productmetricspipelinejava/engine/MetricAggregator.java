package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;


/**
 * Owns one versioned metric: its state, durable catch-up from the event log and
 * atomic snapshot commits. Every drain batch is processed deterministically and
 * committed as a single self-consistent whole; queries only ever read committed
 * snapshots, never this in-flight state.
 */
public abstract class MetricAggregator {

    protected final MetricDefinition definition;
    protected final EventLog eventLog;
    protected final SnapshotStore snapshots;
    protected AggregatorState state;
    private final Object commitLock = new Object();

    protected MetricAggregator(MetricDefinition definition, EventLog eventLog, SnapshotStore snapshots) {
        this.definition = definition;
        this.eventLog = eventLog;
        this.snapshots = snapshots;
    }

    public final MetricDefinition definition() {
        return definition;
    }

    public final String metricId() {
        return definition.id();
    }

    public final int version() {
        return definition.version();
    }

    public final Object commitLock() {
        return commitLock;
    }

    /** Restore from the last checkpoint or start empty. */
    public abstract void open();

    /**
     * Pull and process one batch of new events; commit exactly once per batch.
     * Returns the number of events applied (0 = caught up).
     */
    public abstract int drainOnce(int maxEvents) throws java.io.IOException;

    /**
     * Build fresh state by replaying the whole log up to {@code targetSeq}.
     * Used by background recomputation; does not touch the live state.
     */
    public abstract AggregatorState rebuildUpTo(long targetSeq, int batchSize);

    /** Atomic switch to recomputed state (called under coordination). */
    public abstract void adopt(AggregatorState rebuilt) throws java.io.IOException;

    /** Latest committed seq for this metric. */
    public abstract long committedSeq();

    /** Immutable committed snapshot for queries. */
    public abstract CommittedView view();

}

