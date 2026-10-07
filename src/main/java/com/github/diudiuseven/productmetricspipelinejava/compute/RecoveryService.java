package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Startup recovery: interrupted recompute tasks are marked INTERRUPTED and
 * their scratch is discarded; builders resume from the last committed
 * checkpoint. Committed results and unfinished tasks remain distinguishable.
 */
@Component
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final DefinitionRegistry registry;
    private final AggregateStore store;
    private final ComputeScheduler scheduler;

    public RecoveryService(DefinitionRegistry registry, AggregateStore store, ComputeScheduler scheduler) {
        this.registry = registry;
        this.store = store;
        this.scheduler = scheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        // 1. load definitions (idempotent; never overrides existing versions)
        registry.init();

        // 2. distinguish committed results from unfinished tasks:
        //    anything left RUNNING was interrupted by the crash -> mark and clean.
        List<Long> running = store.findRunningTasks();
        for (Long taskId : running) {
            store.finishRecomputeTask(taskId, RecomputeTaskStatus.INTERRUPTED,
                    "process restarted before task finished");
            store.deleteScratch(taskId);
            log.warn("interrupted recompute task {} marked INTERRUPTED; scratch discarded", taskId);
        }
        if (!running.isEmpty()) {
            log.info("recovery: {} interrupted task(s) cleaned; committed snapshots untouched", running.size());
        }

        // 3. blocked publish gates survive restarts: resume the healing
        //    recompute so the metric converges back to a verified state.
        for (com.github.diudiuseven.productmetricspipelinejava.verify.MetricGate gate
                : store.findBlockedGates()) {
            log.warn("recovery: metric {} v{} gate still BLOCKED ({}); scheduling healing recompute",
                    gate.metricId(), gate.version(), gate.reason());
            registry.get(gate.metricId(), gate.version()).ifPresent(def ->
                    scheduler.requestRecompute(gate.metricId(), gate.version(), "RECOVERY_HEAL"));
        }
        // 4. builders resume from their last committed checkpoint on the next tick.
    }
}
