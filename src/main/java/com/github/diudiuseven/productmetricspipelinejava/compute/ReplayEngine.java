package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Deterministic per-user state machine. Every result is derived from the event
 * log alone, so incremental results are always row-identical to a full replay.
 */
@Component
public class ReplayEngine {

    /** Full replay: all events -> aggregates + per-user contributions. */
    public ReplayResult replay(MetricDefinition def, List<StoredEvent> events) {
        // TODO implement
        return new ReplayResult(Map.of(), Map.of(), Map.of(), Map.of());
    }

    /** Evaluates a single user's funnel from their full event history. */
    public FunnelContribution evaluateFunnel(MetricDefinition def, List<StoredEvent> userEvents) {
        // TODO implement
        return null;
    }

    /** Evaluates a single user's retention from their full event history. */
    public RetentionContribution evaluateRetention(MetricDefinition def, List<StoredEvent> userEvents) {
        // TODO implement
        return null;
    }
}
