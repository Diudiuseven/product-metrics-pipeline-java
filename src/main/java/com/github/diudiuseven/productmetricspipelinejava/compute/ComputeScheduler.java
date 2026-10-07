package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyReport;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyVerifier;
import com.github.diudiuseven.productmetricspipelinejava.verify.VerificationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Drives per-metric builders on a bounded thread pool. Each metric is
 * isolated: a slow metric occupies only its own worker and never blocks others.
 *
 * <p>Every worker turn ends with a consistency verification (interval-gated):
 * the published snapshot is compared row-by-row against a full replay of the
 * same committed input. A mismatch blocks further incremental publishes via
 * the metric gate and triggers a healing recompute; only a passing
 * verification re-opens the gate.
 */
@Component
public class ComputeScheduler {

    private static final Logger log = LoggerFactory.getLogger(ComputeScheduler.class);

    private final DefinitionRegistry registry;
    private final IncrementalBuilder builder;
    private final RecomputeService recomputeService;
    private final ConsistencyVerifier verifier;
    private final AggregateStore store;
    private final Executor builderExecutor;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public ComputeScheduler(DefinitionRegistry registry, IncrementalBuilder builder,
                            RecomputeService recomputeService, ConsistencyVerifier verifier,
                            AggregateStore store, Executor builderExecutor) {
        this.registry = registry;
        this.builder = builder;
        this.recomputeService = recomputeService;
        this.verifier = verifier;
        this.store = store;
        this.builderExecutor = builderExecutor;
    }

    @Scheduled(fixedDelayString = "${pipeline.batch-interval}")
    public void tick() {
        for (MetricDefinition def : registry.listActive()) {
            String key = def.getId() + ":" + def.getVersion();
            if (recomputeService.isRecomputing(def.getId(), def.getVersion())) {
                continue;
            }
            if (!inFlight.add(key)) {
                continue; // previous tick still running: skip, retry next interval
            }
            builderExecutor.execute(() -> {
                try {
                    runTurn(def);
                } catch (Exception e) {
                    log.warn("build/verify turn failed for {}", key, e);
                } finally {
                    inFlight.remove(key);
                }
            });
        }
    }

    /** One worker turn for a metric: incremental build, then verification. */
    private void runTurn(MetricDefinition def) {
        boolean gated = store.loadGate(def.getId(), def.getVersion())
                .map(g -> g.blocked()).orElse(false);
        if (gated) {
            // No new incremental result may be published while a mismatch is
            // unresolved. The healing recompute is triggered by the verifier.
            return;
        }
        long commitId = builder.runIncremental(def);
        if (commitId < 0) {
            // bound exceeded: escalate to full recompute
            recomputeService.recompute(def, "BOUNDS");
        }
        ConsistencyReport report = verifier.verifyIfDue(def);
        if (report != null && report.status() == VerificationStatus.MISMATCH) {
            // heal: replace the divergent snapshot with a full recompute, then
            // re-verify; the gate re-opens only when verification passes.
            recomputeService.recompute(def, "AUTO_HEAL");
            verifier.verifyNow(def);
        }
    }

    /** Submits a recompute for a metric (used by admin API / recovery). */
    public void requestRecompute(String metricId, Integer version, String trigger) {
        MetricDefinition def = version == null
                ? registry.getLatest(metricId).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId))
                : registry.get(metricId, version).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId + " v" + version));
        builderExecutor.execute(() -> {
            try {
                recomputeService.recompute(def, trigger);
                verifier.verifyNow(def);
            } catch (Exception e) {
                log.warn("recompute failed for {}", metricId, e);
            }
        });
    }
}
