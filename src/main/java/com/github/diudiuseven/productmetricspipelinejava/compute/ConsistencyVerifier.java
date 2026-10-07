package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Continuously checks that the incrementally maintained snapshot is
 * row-identical to a full replay over the same committed input prefix
 * ({@code log_seq <= toSeq}). The comparison covers per-user contributions
 * and per-(bucket, dims) aggregate rows; differences are reported down to the
 * concrete user and dimension group.
 *
 * <p>Cost is bounded by {@code pipeline.consistency-max-events}: beyond that
 * bound the check is skipped (recorded as {@link VerificationStatus#SKIPPED_BOUNDS})
 * instead of consuming unbounded resources.
 */
@Component
public class ConsistencyVerifier {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyVerifier.class);

    /** Verification outcome: status plus a JSON document locating the diffs. */
    public record Outcome(VerificationStatus status, String diffsJson, long totalDiffs) {
    }

    private final ReplayEngine engine;
    private final AggregateStore store;
    private final EventLog eventLog;
    private final PipelineProperties props;
    private final ObjectMapper objectMapper;
    private final MetricBuildLocks buildLocks;

    public ConsistencyVerifier(ReplayEngine engine, AggregateStore store, EventLog eventLog,
                               PipelineProperties props, ObjectMapper objectMapper,
                               MetricBuildLocks buildLocks) {
        this.engine = engine;
        this.store = store;
        this.eventLog = eventLog;
        this.props = props;
        this.objectMapper = objectMapper;
        this.buildLocks = buildLocks;
    }

    /** True when a full-replay verification stays within the configured cost bound. */
    public boolean isWithinBounds(long activeEvents) {
        return activeEvents <= props.getConsistencyMaxEvents();
    }

    /** Outcome used when verification is skipped to respect the cost bound. */
    public Outcome skippedBounds(long activeEvents) {
        return new Outcome(VerificationStatus.SKIPPED_BOUNDS,
                toJson(Map.of("reason", "active events " + activeEvents
                        + " exceed pipeline.consistency-max-events "
                        + props.getConsistencyMaxEvents())), 0);
    }

    /**
     * Compares a candidate funnel snapshot (not yet published) against a full
     * replay of the committed input prefix ending at {@code toSeq}.
     */
    public Outcome verifyFunnel(MetricDefinition def, long toSeq,
                                Map<String, FunnelContribution> candidateUsers,
                                Map<String, FunnelAggregate> candidateAgg) {
        long active = eventLog.countActiveUpTo(toSeq);
        if (active > props.getConsistencyMaxEvents()) {
            return new Outcome(VerificationStatus.SKIPPED_BOUNDS,
                    toJson(Map.of("reason", "active events " + active + " exceed pipeline.consistency-max-events "
                            + props.getConsistencyMaxEvents())), 0);
        }
        List<StoredEvent> events = eventLog.findAllActiveOrderedUpTo(toSeq);
        Outcome horizonProblem = checkHorizon(events);
        if (horizonProblem != null) {
            return horizonProblem;
        }
        ReplayResult replay = engine.replay(def, events);
        Map<String, FunnelContribution> expectedUsers = ReplayEngine.capFunnelDims(
                replay.funnelUsers(), store.loadCurrentFunnelDims(def.getId(), def.getVersion()),
                props.getMaxDimCombinations());
        Map<String, FunnelAggregate> expectedAgg =
                ReplayEngine.aggregateFunnel(expectedUsers, def.getFunnel().steps().size());

        List<String> diffs = new ArrayList<>();
        long[] total = {0};
        diffMaps("user", candidateUsers, expectedUsers, diffs, total);
        diffMaps("group", candidateAgg, expectedAgg, diffs, total);
        return diffs.isEmpty()
                ? new Outcome(VerificationStatus.VERIFIED, null, 0)
                : new Outcome(VerificationStatus.MISMATCH, toJson(Map.of(
                        "totalDiffs", total[0], "diffs", diffs)), total[0]);
    }

    /** Retention counterpart of {@link #verifyFunnel}. */
    public Outcome verifyRetention(MetricDefinition def, long toSeq,
                                   Map<String, RetentionContribution> candidateUsers,
                                   Map<String, RetentionAggregate> candidateAgg) {
        long active = eventLog.countActiveUpTo(toSeq);
        if (active > props.getConsistencyMaxEvents()) {
            return new Outcome(VerificationStatus.SKIPPED_BOUNDS,
                    toJson(Map.of("reason", "active events " + active + " exceed pipeline.consistency-max-events "
                            + props.getConsistencyMaxEvents())), 0);
        }
        List<StoredEvent> events = eventLog.findAllActiveOrderedUpTo(toSeq);
        Outcome horizonProblem = checkHorizon(events);
        if (horizonProblem != null) {
            return horizonProblem;
        }
        ReplayResult replay = engine.replay(def, events);
        Map<String, RetentionContribution> expectedUsers = ReplayEngine.capRetentionDims(
                replay.retentionUsers(), store.loadCurrentRetentionDims(def.getId(), def.getVersion()),
                props.getMaxDimCombinations());
        Map<String, RetentionAggregate> expectedAgg =
                ReplayEngine.aggregateRetention(expectedUsers, def.getRetention().periods().size());

        List<String> diffs = new ArrayList<>();
        long[] total = {0};
        diffMaps("user", candidateUsers, expectedUsers, diffs, total);
        diffMaps("group", candidateAgg, expectedAgg, diffs, total);
        return diffs.isEmpty()
                ? new Outcome(VerificationStatus.VERIFIED, null, 0)
                : new Outcome(VerificationStatus.MISMATCH, toJson(Map.of(
                        "totalDiffs", total[0], "diffs", diffs)), total[0]);
    }

    /**
     * Verifies the currently committed snapshot of a metric against a full
     * replay up to its checkpoint, persists a report and returns the status.
     * Used for idle metrics (time passing without new events) and on-demand
     * checks. Does not modify the published snapshot.
     */
    public VerificationStatus verifyCommitted(MetricDefinition def) {
        // serialized with builds of the same metric: the check reads several
        // tables and must see one consistent committed generation
        synchronized (buildLocks.of(def.getId(), def.getVersion())) {
            return doVerifyCommitted(def);
        }
    }

    private VerificationStatus doVerifyCommitted(MetricDefinition def) {
        long toSeq = store.loadCheckpoint(def.getId(), def.getVersion())
                .map(BuilderCheckpoint::logSeq).orElse(0L);
        Outcome outcome;
        if (def.getType() == MetricType.FUNNEL) {
            outcome = verifyFunnel(def, toSeq,
                    store.loadAllFunnelUsers(def.getId(), def.getVersion()),
                    store.loadCurrentFunnel(def.getId(), def.getVersion()));
        } else {
            outcome = verifyRetention(def, toSeq,
                    store.loadAllRetentionUsers(def.getId(), def.getVersion()),
                    store.loadCurrentRetention(def.getId(), def.getVersion()));
        }
        store.saveVerificationReport(def.getId(), def.getVersion(), toSeq, outcome.status(),
                outcome.diffsJson(), null);
        if (outcome.status() != VerificationStatus.VERIFIED) {
            log.warn("consistency check for {} v{}: {} ({})", def.getId(), def.getVersion(),
                    outcome.status(), outcome.diffsJson());
        }
        return outcome.status();
    }

    // --------------------------- helpers ---------------------------

    /** The decision horizon is derived from event_time; a null event_time
     *  would make verdicts underivable. */
    private Outcome checkHorizon(List<StoredEvent> events) {
        List<String> offenders = new ArrayList<>();
        for (StoredEvent se : events) {
            if (se.event().eventTime() == null) {
                if (offenders.size() < props.getConsistencyMaxDiffs()) {
                    offenders.add("event " + se.event().eventId() + " (user "
                            + se.event().userId() + ") has no event_time");
                }
            }
        }
        if (offenders.isEmpty()) {
            return null;
        }
        return new Outcome(VerificationStatus.HORIZON_UNDETERMINED,
                toJson(Map.of("reason", "decision horizon underivable", "diffs", offenders)),
                offenders.size());
    }

    /** Records diffs between actual and expected rows, keyed by user id or
     *  by the (bucket ␀ dims) group key, in deterministic key order. */
    private <V> void diffMaps(String kind, Map<String, V> actual, Map<String, V> expected,
                              List<String> diffs, long[] total) {
        java.util.Set<String> keys = new TreeSet<>(actual.keySet());
        keys.addAll(expected.keySet());
        for (String key : keys) {
            V a = actual.get(key);
            V e = expected.get(key);
            if (java.util.Objects.equals(a, e)) {
                continue;
            }
            total[0]++;
            if (diffs.size() < props.getConsistencyMaxDiffs()) {
                diffs.add(kind + " " + key + ": expected=" + e + " actual=" + a);
            }
        }
    }

    private String toJson(Map<String, Object> doc) {
        try {
            return objectMapper.writeValueAsString(doc);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
