package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Full recompute: builds a scratch snapshot from the whole event log and
 * publishes it atomically. New events arriving during recompute are logged and
 * applied afterwards; nothing is lost.
 */
@Service
public class RecomputeService {

    private static final Logger log = LoggerFactory.getLogger(RecomputeService.class);

    private final ReplayEngine engine;
    private final AggregateStore store;
    private final EventLog eventLog;
    private final PipelineProperties props;
    private final Set<String> running = ConcurrentHashMap.newKeySet();

    public RecomputeService(ReplayEngine engine, AggregateStore store, EventLog eventLog,
                            PipelineProperties props) {
        this.engine = engine;
        this.store = store;
        this.eventLog = eventLog;
        this.props = props;
    }

    /** Recomputes the metric from scratch and returns the new commit id. */
    public long recompute(MetricDefinition def, String trigger) {
        String key = def.getId() + ":" + def.getVersion();
        if (!running.add(key)) {
            log.info("recompute already running for {}", key);
            return 0L;
        }
        long taskId = 0L;
        try {
            long toSeq = eventLog.maxSeq();
            BuilderCheckpoint cp = store.loadCheckpoint(def.getId(), def.getVersion()).orElse(null);
            long fromSeq = cp == null ? 0L : cp.logSeq();
            taskId = store.createRecomputeTask(def.getId(), def.getVersion(), fromSeq, toSeq);

            List<StoredEvent> events = eventLog.findAllActiveOrdered();
            if (events.size() > props.getRecomputeMaxEvents()) {
                log.warn("recompute {}: active events {} exceed bound {}", key, events.size(),
                        props.getRecomputeMaxEvents());
            }
            ReplayResult result = engine.replay(def, events);

            // cap dimension cardinality (safety fuse) and re-aggregate
            if (def.getType() == com.github.diudiuseven.productmetricspipelinejava.metric.MetricType.FUNNEL) {
                Map<String, FunnelContribution> cappedUsers = ReplayEngine.capFunnelDims(
                        result.funnelUsers(), java.util.Set.of(), props.getMaxDimCombinations());
                result = new ReplayResult(
                        ReplayEngine.aggregateFunnel(cappedUsers, def.getFunnel().steps().size()),
                        result.retention(),
                        cappedUsers,
                        result.retentionUsers());
            } else {
                Map<String, RetentionContribution> cappedUsers = ReplayEngine.capRetentionDims(
                        result.retentionUsers(), java.util.Set.of(), props.getMaxDimCombinations());
                result = new ReplayResult(
                        result.funnel(),
                        ReplayEngine.aggregateRetention(cappedUsers, def.getRetention().periods().size()),
                        result.funnelUsers(),
                        cappedUsers);
            }

            // build scratch
            for (Map.Entry<String, FunnelContribution> e : result.funnelUsers().entrySet()) {
                store.writeFunnelScratch(taskId, e.getKey(), e.getValue());
            }
            for (Map.Entry<String, RetentionContribution> e : result.retentionUsers().entrySet()) {
                store.writeRetentionScratch(taskId, e.getKey(), e.getValue());
            }
            for (FunnelAggregate a : result.funnel().values()) {
                store.writeFunnelAggScratch(taskId, a);
            }
            for (RetentionAggregate a : result.retention().values()) {
                store.writeRetentionAggScratch(taskId, a);
            }

            long commitId = store.publishRecompute(def, taskId, fromSeq, toSeq, events.size(),
                    result, trigger);
            log.info("recompute {}: published commit {} ({} events, {} users)", key, commitId,
                    events.size(), result.funnelUsers().size() + result.retentionUsers().size());
            return commitId;
        } catch (RuntimeException e) {
            if (taskId != 0L) {
                store.finishRecomputeTask(taskId, RecomputeTaskStatus.FAILED, e.getMessage());
                store.deleteScratch(taskId);
            }
            throw e;
        } finally {
            running.remove(key);
        }
    }

    public boolean isRecomputing(String metricId, int version) {
        return running.contains(metricId + ":" + version);
    }
}
