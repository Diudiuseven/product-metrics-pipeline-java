package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelAggregator;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionAggregator;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;

/** Creates the right aggregator for a definition type. */
public final class AggregatorFactory {

    private AggregatorFactory() {
    }

    public static MetricAggregator create(MetricDefinition def, EventLog log, SnapshotStore snapshots) {
        return switch (def.type()) {
            case FUNNEL -> new FunnelAggregator(def, log, snapshots);
            case RETENTION -> new RetentionAggregator(def, log, snapshots);
        };
    }
}
