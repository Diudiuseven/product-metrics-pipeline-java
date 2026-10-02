package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Incremental builder for one (metric, version). Re-evaluates affected users
 * from their full event history, so its published snapshot is always identical
 * to a full replay over the same input.
 */
@Component
public class IncrementalBuilder {

    private static final Logger log = LoggerFactory.getLogger(IncrementalBuilder.class);

    private final ReplayEngine engine;
    private final AggregateStore store;
    private final EventLog eventLog;
    private final PipelineProperties props;

    public IncrementalBuilder(ReplayEngine engine, AggregateStore store, EventLog eventLog,
                              PipelineProperties props) {
        this.engine = engine;
        this.store = store;
        this.eventLog = eventLog;
        this.props = props;
    }

    /**
     * Runs one incremental tick for the metric. No-op if nothing new arrived.
     * Returns the new commit id, or 0 if no commit was produced.
     */
    public long runIncremental(MetricDefinition def) {
        BuilderCheckpoint cp = store.loadCheckpoint(def.getId(), def.getVersion())
                .orElse(new BuilderCheckpoint(def.getId(), def.getVersion(), 0L, 0L, java.time.Instant.now()));
        List<StoredEvent> newEvents = eventLog.findAfterSeq(cp.logSeq(), props.getMaxBatchEvents());
        if (newEvents.isEmpty()) {
            return 0L;
        }
        long toSeq = newEvents.get(newEvents.size() - 1).logSeq();
        long fromSeq = cp.logSeq();

        List<String> affectedUsers = newEvents.stream()
                .map(e -> e.event().userId()).distinct().toList();
        if (affectedUsers.size() > props.getMaxAffectedUsers()) {
            log.info("metric {} v{}: affected users {} > bound {}, triggering recompute",
                    def.getId(), def.getVersion(), affectedUsers.size(), props.getMaxAffectedUsers());
            return -1L; // signal: caller should trigger recompute
        }

        boolean funnel = def.getType() == MetricType.FUNNEL;

        // 1. re-evaluate each affected user from their FULL event history
        Map<String, FunnelContribution> newFunnelUsers = new LinkedHashMap<>();
        Map<String, RetentionContribution> newRetentionUsers = new LinkedHashMap<>();
        List<String> deletedUsers = new ArrayList<>();
        for (String userId : affectedUsers) {
            List<StoredEvent> userEvents = eventLog.findActiveByUser(userId);
            if (userEvents.size() > props.getMaxEventsPerUser()) {
                log.info("metric {} v{}: user {} events {} > bound {}, triggering recompute",
                        def.getId(), def.getVersion(), userId, userEvents.size(),
                        props.getMaxEventsPerUser());
                return -1L;
            }
            if (funnel) {
                FunnelContribution c = userEvents.isEmpty() ? null : engine.evaluateFunnel(def, userEvents);
                if (c == null) {
                    deletedUsers.add(userId);
                } else {
                    newFunnelUsers.put(userId, c);
                }
            } else {
                RetentionContribution c = userEvents.isEmpty() ? null : engine.evaluateRetention(def, userEvents);
                if (c == null) {
                    deletedUsers.add(userId);
                } else {
                    newRetentionUsers.put(userId, c);
                }
            }
        }

        // 1b. cap dimension cardinality (safety fuse)
        if (funnel) {
            Set<String> existingDims = store.loadCurrentFunnel(def.getId(), def.getVersion()).keySet().stream()
                    .map(k -> k.substring(k.indexOf('\u0000') + 1))
                    .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
            newFunnelUsers = ReplayEngine.capFunnelDims(newFunnelUsers, existingDims, props.getMaxDimCombinations());
        } else {
            Set<String> existingDims = store.loadCurrentRetention(def.getId(), def.getVersion()).keySet().stream()
                    .map(k -> k.substring(k.indexOf('\u0000') + 1))
                    .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
            newRetentionUsers = ReplayEngine.capRetentionDims(newRetentionUsers, existingDims, props.getMaxDimCombinations());
        }

        // 2. load previous contributions for affected users
        Map<String, FunnelContribution> oldFunnelUsers = funnel
                ? store.loadFunnelUsers(def.getId(), def.getVersion(), affectedUsers)
                : Map.of();
        Map<String, RetentionContribution> oldRetentionUsers = funnel
                ? Map.of()
                : store.loadRetentionUsers(def.getId(), def.getVersion(), affectedUsers);

        // 3. apply diffs to current aggregate rows
        Map<String, FunnelAggregate> funnelAgg = funnel
                ? new LinkedHashMap<>(store.loadCurrentFunnel(def.getId(), def.getVersion()))
                : Map.of();
        Map<String, RetentionAggregate> retentionAgg = funnel
                ? Map.of()
                : new LinkedHashMap<>(store.loadCurrentRetention(def.getId(), def.getVersion()));

        List<FunnelAggregate> funnelAggUpserts = new ArrayList<>();
        List<RetentionAggregate> retentionAggUpserts = new ArrayList<>();
        Map<String, FunnelContribution> funnelUserUpserts = new LinkedHashMap<>();
        Map<String, RetentionContribution> retentionUserUpserts = new LinkedHashMap<>();

        if (funnel) {
            for (String userId : affectedUsers) {
                FunnelContribution oldC = oldFunnelUsers.get(userId);
                FunnelContribution newC = newFunnelUsers.get(userId);
                if (java.util.Objects.equals(oldC, newC)) {
                    continue;
                }
                if (newC != null) {
                    funnelUserUpserts.put(userId, newC);
                }
                if (oldC != null) {
                    applyFunnelDelta(funnelAgg, oldC, -1, def.getFunnel().steps().size());
                }
                if (newC != null) {
                    applyFunnelDelta(funnelAgg, newC, +1, def.getFunnel().steps().size());
                }
            }
            for (FunnelAggregate a : funnelAgg.values()) {
                funnelAggUpserts.add(a);
            }
        } else {
            for (String userId : affectedUsers) {
                RetentionContribution oldC = oldRetentionUsers.get(userId);
                RetentionContribution newC = newRetentionUsers.get(userId);
                if (java.util.Objects.equals(oldC, newC)) {
                    continue;
                }
                if (newC != null) {
                    retentionUserUpserts.put(userId, newC);
                }
                if (oldC != null) {
                    applyRetentionDelta(retentionAgg, oldC, -1, def.getRetention().periods().size());
                }
                if (newC != null) {
                    applyRetentionDelta(retentionAgg, newC, +1, def.getRetention().periods().size());
                }
            }
            for (RetentionAggregate a : retentionAgg.values()) {
                retentionAggUpserts.add(a);
            }
        }

        // 4. publish atomically
        long commitId = store.publishIncremental(def, fromSeq, toSeq, newEvents.size(),
                funnelAggUpserts, retentionAggUpserts, funnelUserUpserts, retentionUserUpserts,
                affectedUsers.size(), "INCREMENTAL");
        if (!deletedUsers.isEmpty()) {
            if (funnel) {
                store.deleteFunnelUsers(def.getId(), def.getVersion(), deletedUsers);
            } else {
                store.deleteRetentionUsers(def.getId(), def.getVersion(), deletedUsers);
            }
        }
        log.debug("metric {} v{}: incremental commit {} ({} -> {}, {} events, {} users)",
                def.getId(), def.getVersion(), commitId, fromSeq, toSeq, newEvents.size(),
                affectedUsers.size());
        return commitId;
    }

    private void applyFunnelDelta(Map<String, FunnelAggregate> agg, FunnelContribution c,
                                   int sign, int steps) {
        String key = c.bucket() + "\u0000" + c.dims();
        FunnelAggregate prev = agg.get(key);
        long[] stepCounts = prev == null ? new long[steps] : prev.stepCounts().clone();
        for (int k = 0; k < steps; k++) {
            if ((c.stepMask() & (1 << k)) != 0) {
                stepCounts[k] += sign;
            }
        }
        long entered = (prev == null ? 0 : prev.entered()) + sign;
        long converted = (prev == null ? 0 : prev.converted())
                + (c.verdict() == FunnelVerdict.CONVERTED ? sign : 0);
        long skipped = (prev == null ? 0 : prev.skipped())
                + (c.verdict() == FunnelVerdict.SKIPPED_STEP ? sign : 0);
        long missing = (prev == null ? 0 : prev.missing())
                + (c.verdict() == FunnelVerdict.MISSING ? sign : 0);
        long oow = (prev == null ? 0 : prev.outOfWindow())
                + (c.verdict() == FunnelVerdict.OUT_OF_WINDOW ? sign : 0);
        long pending = (prev == null ? 0 : prev.pending())
                + (c.verdict() == FunnelVerdict.PENDING ? sign : 0);
        agg.put(key, new FunnelAggregate(c.bucket(), c.dims(), entered, converted, skipped,
                missing, oow, pending, stepCounts));
    }

    private void applyRetentionDelta(Map<String, RetentionAggregate> agg, RetentionContribution c,
                                     int sign, int periods) {
        String key = c.cohort() + "\u0000" + c.dims();
        RetentionAggregate prev = agg.get(key);
        long[] retained = prev == null ? new long[periods] : prev.retained().clone();
        for (int i = 0; i < periods; i++) {
            if ((c.mask() & (1 << i)) != 0) {
                retained[i] += sign;
            }
        }
        long size = (prev == null ? 0 : prev.cohortSize()) + sign;
        agg.put(key, new RetentionAggregate(c.cohort(), c.dims(), size, retained));
    }
}
