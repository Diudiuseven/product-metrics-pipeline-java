package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;

/**
 * Drives per-metric builders on a bounded thread pool. Each metric is
 * isolated: a slow metric occupies only its own worker.
 */
@Component
public class ComputeScheduler {

    private final DefinitionRegistry registry;
    private final IncrementalBuilder builder;
    private final RecomputeService recomputeService;
    private final Executor builderExecutor;

    public ComputeScheduler(DefinitionRegistry registry, IncrementalBuilder builder,
                            RecomputeService recomputeService, Executor builderExecutor) {
        this.registry = registry;
        this.builder = builder;
        this.recomputeService = recomputeService;
        this.builderExecutor = builderExecutor;
    }

    @Scheduled(fixedDelayString = "${pipeline.batch-interval}")
    public void tick() {
        // TODO implement
    }

    /** Submits a recompute for a metric (used by admin API / recovery). */
    public void requestRecompute(String metricId, int version, String trigger) {
        // TODO implement
    }
}
