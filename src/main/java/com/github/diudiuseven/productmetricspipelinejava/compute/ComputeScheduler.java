package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
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
 */
@Component
public class ComputeScheduler {

    private static final Logger log = LoggerFactory.getLogger(ComputeScheduler.class);

    private final DefinitionRegistry registry;
    private final IncrementalBuilder builder;
    private final RecomputeService recomputeService;
    private final Executor builderExecutor;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public ComputeScheduler(DefinitionRegistry registry, IncrementalBuilder builder,
                            RecomputeService recomputeService, Executor builderExecutor) {
        this.registry = registry;
        this.builder = builder;
        this.recomputeService = recomputeService;
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
                    long commitId = builder.runIncremental(def);
                    if (commitId < 0) {
                        // bound exceeded: escalate to full recompute
                        recomputeService.recompute(def, "BOUNDS");
                    }
                } catch (Exception e) {
                    log.warn("incremental build failed for {}", key, e);
                } finally {
                    inFlight.remove(key);
                }
            });
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
            } catch (Exception e) {
                log.warn("recompute failed for {}", metricId, e);
            }
        });
    }
}
