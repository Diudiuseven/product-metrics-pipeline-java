package com.github.diudiuseven.productmetricspipelinejava.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.BuilderCheckpoint;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayResult;
import com.github.diudiuseven.productmetricspipelinejava.compute.RetentionAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.RetentionContribution;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Continuously compares the published snapshot of a metric against a
 * from-scratch replay of the same committed input prefix [1, checkpointSeq].
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Read-only towards the snapshot: verification never blocks ingestion,
 *       incremental builds or queries, and never publishes anything itself.</li>
 *   <li>On MISMATCH the metric gate is set to BLOCKED so no new incremental
 *       result is published until a recompute heals and re-verifies.</li>
 *   <li>Resource use is bounded by the existing configuration
 *       ({@code recompute-max-events}, {@code verification-max-diffs},
 *       {@code verification-interval}); one metric's verification runs on its
 *       own worker and never blocks other metrics.</li>
 * </ul>
 */
@Component
public class ConsistencyVerifier {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyVerifier.class);

    /** One localised difference between the replay and the published snapshot. */
    public record DiffEntry(String scope, String key, String field, String expected, String actual) {
    }

    private final AggregateStore store;
    private final EventLog eventLog;
    private final ReplayEngine engine;
    private final PipelineProperties props;
    private final ObjectMapper objectMapper;

    public ConsistencyVerifier(AggregateStore store, EventLog eventLog, ReplayEngine engine,
                               PipelineProperties props, ObjectMapper objectMapper) {
        this.store = store;
        this.eventLog = eventLog;
        this.engine = engine;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    /**
     * Runs one verification for the metric if it is due: no report yet, the
     * checkpoint moved since the last report, or the re-check interval elapsed
     * (covers the "time passes without new events" case).
     *
     * @return the report produced, or {@code null} when verification was not due
     *         or a concurrent publish made this run stale
     */
    public ConsistencyReport verifyIfDue(MetricDefinition def) {
        Optional<BuilderCheckpoint> cp = store.loadCheckpoint(def.getId(), def.getVersion());
        Optional<ConsistencyReport> last = store.latestReport(def.getId(), def.getVersion());
        boolean due = last.isEmpty()
                || (cp.isPresent() && cp.get().logSeq() != last.get().logSeq())
                || Duration.between(last.get().createdAt(), Instant.now())
                        .compareTo(props.getVerificationInterval()) > 0;
        return due ? verifyNow(def) : null;
    }

    /**
     * Verifies the published snapshot against a full replay bounded to the
     * committed checkpoint sequence. Always runs (not interval-gated).
     *
     * @return the report, or {@code null} when a concurrent publish advanced
     *         the checkpoint mid-check (the next run verifies the new state)
     */
    public ConsistencyReport verifyNow(MetricDefinition def) {
        Optional<BuilderCheckpoint> cpOpt = store.loadCheckpoint(def.getId(), def.getVersion());
        if (cpOpt.isEmpty() || cpOpt.get().lastCommitId() == 0) {
            // No committed input: the decision basis cannot be determined.
            return persist(new ConsistencyReport(0, def.getId(), def.getVersion(), 0, 0,
                    VerificationStatus.NO_COMMITTED_SNAPSHOT,
                    ConsistencyFailureReason.DECISION_BASIS_UNDETERMINED, 0, null, Instant.now()));
        }
        BuilderCheckpoint cp = cpOpt.get();
        List<StoredEvent> events = eventLog.findAllActiveOrderedUpTo(cp.logSeq());
        if (events.size() > props.getRecomputeMaxEvents()) {
            return persist(new ConsistencyReport(0, def.getId(), def.getVersion(),
                    cp.lastCommitId(), cp.logSeq(), VerificationStatus.SKIPPED_BOUNDS,
                    ConsistencyFailureReason.BOUNDS_EXCEEDED, 0, null, Instant.now()));
        }

        boolean funnel = def.getType() == MetricType.FUNNEL;
        ReplayResult replayed = engine.replay(def, events);
        List<DiffEntry> diffs = new ArrayList<>();
        if (funnel) {
            Map<String, FunnelContribution> committedUsers =
                    store.loadAllFunnelUsers(def.getId(), def.getVersion());
            Map<String, FunnelContribution> expectedUsers = ReplayEngine.capFunnelDimsSticky(
                    replayed.funnelUsers(), committedUsers,
                    store.loadCurrentFunnelDims(def.getId(), def.getVersion()),
                    props.getMaxDimCombinations());
            Map<String, FunnelAggregate> expectedAgg =
                    ReplayEngine.aggregateFunnel(expectedUsers, def.getFunnel().steps().size());
            diffUsers(expectedUsers, committedUsers, "FUNNEL_USER", diffs);
            diffAggregates(expectedAgg, store.loadCurrentFunnel(def.getId(), def.getVersion()),
                    "FUNNEL_AGG", diffs);
        } else {
            Map<String, RetentionContribution> committedUsers =
                    store.loadAllRetentionUsers(def.getId(), def.getVersion());
            Map<String, RetentionContribution> expectedUsers = ReplayEngine.capRetentionDimsSticky(
                    replayed.retentionUsers(), committedUsers,
                    store.loadCurrentRetentionDims(def.getId(), def.getVersion()),
                    props.getMaxDimCombinations());
            Map<String, RetentionAggregate> expectedAgg =
                    ReplayEngine.aggregateRetention(expectedUsers, def.getRetention().periods().size());
            diffUsers(expectedUsers, committedUsers, "RETENTION_USER", diffs);
            diffAggregates(expectedAgg, store.loadCurrentRetention(def.getId(), def.getVersion()),
                    "RETENTION_AGG", diffs);
        }

        // Self-consistency: if a publish raced us, this comparison is stale.
        Optional<BuilderCheckpoint> after = store.loadCheckpoint(def.getId(), def.getVersion());
        if (after.isEmpty() || after.get().logSeq() != cp.logSeq()
                || after.get().lastCommitId() != cp.lastCommitId()) {
            log.debug("verification of {} v{} discarded: checkpoint advanced during check",
                    def.getId(), def.getVersion());
            return null;
        }

        VerificationStatus status = diffs.isEmpty() ? VerificationStatus.VERIFIED : VerificationStatus.MISMATCH;
        ConsistencyReport report = new ConsistencyReport(0, def.getId(), def.getVersion(),
                cp.lastCommitId(), cp.logSeq(), status,
                status == VerificationStatus.MISMATCH ? ConsistencyFailureReason.CONSISTENCY_MISMATCH : null,
                diffs.size(), diffs.isEmpty() ? null : toJson(diffs), Instant.now());
        return persist(report);
    }

    // --------------------------- internals ---------------------------

    private ConsistencyReport persist(ConsistencyReport report) {
        long id = store.insertReport(report);
        if (report.status() == VerificationStatus.MISMATCH) {
            store.saveGate(new MetricGate(report.metricId(), report.version(), true,
                    ConsistencyFailureReason.CONSISTENCY_MISMATCH, Instant.now()));
            log.warn("consistency MISMATCH for {} v{} at seq {} ({} diffs); gate BLOCKED, report {}",
                    report.metricId(), report.version(), report.logSeq(), report.diffCount(), id);
        } else if (report.status() == VerificationStatus.VERIFIED) {
            store.saveGate(new MetricGate(report.metricId(), report.version(), false, null, Instant.now()));
        }
        return new ConsistencyReport(id, report.metricId(), report.version(), report.commitId(),
                report.logSeq(), report.status(), report.reason(), report.diffCount(),
                report.diffDetail(), report.createdAt());
    }

    private <T> void diffUsers(Map<String, T> expected, Map<String, T> actual, String scope,
                               List<DiffEntry> diffs) {
        for (String userId : new TreeSet<>(union(expected, actual))) {
            T e = expected.get(userId);
            T a = actual.get(userId);
            if (!java.util.Objects.equals(e, a)) {
                diffs.add(new DiffEntry(scope, userId, "contribution",
                        String.valueOf(e), String.valueOf(a)));
            }
        }
    }

    private void diffAggregates(Map<String, ? extends Object> expected, Map<String, ?> actual,
                                String scope, List<DiffEntry> diffs) {
        for (String key : new TreeSet<>(union(expected, actual))) {
            Object e = zeroToNull(expected.get(key));
            Object a = zeroToNull(actual.get(key));
            if (!java.util.Objects.equals(e, a)) {
                diffs.add(new DiffEntry(scope, key, "aggregate",
                        String.valueOf(e), String.valueOf(a)));
            }
        }
    }

    /** All-zero aggregate rows are treated as absent (they carry no users). */
    private static Object zeroToNull(Object agg) {
        if (agg instanceof FunnelAggregate f && f.entered() == 0 && f.converted() == 0
                && f.skipped() == 0 && f.missing() == 0 && f.outOfWindow() == 0 && f.pending() == 0
                && allZero(f.stepCounts())) {
            return null;
        }
        if (agg instanceof RetentionAggregate r && r.cohortSize() == 0 && allZero(r.retained())) {
            return null;
        }
        return agg;
    }

    private static boolean allZero(long[] values) {
        for (long v : values) {
            if (v != 0) {
                return false;
            }
        }
        return true;
    }

    private static java.util.Set<String> union(Map<String, ?> a, Map<String, ?> b) {
        java.util.Set<String> keys = new TreeSet<>(a.keySet());
        keys.addAll(b.keySet());
        return keys;
    }

    private String toJson(List<DiffEntry> diffs) {
        try {
            int bound = Math.max(1, props.getVerificationMaxDiffs());
            return objectMapper.writeValueAsString(
                    diffs.size() > bound ? diffs.subList(0, bound) : diffs);
        } catch (Exception e) {
            throw new IllegalStateException("cannot serialize diff detail", e);
        }
    }
}
