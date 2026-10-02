package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Startup recovery: interrupted recompute tasks are marked INTERRUPTED and
 * their scratch is discarded; builders resume from the last committed
 * checkpoint. Committed results and unfinished tasks remain distinguishable.
 */
@Component
public class RecoveryService {

    private final DefinitionRegistry registry;
    private final AggregateStore store;
    private final RecomputeService recomputeService;

    public RecoveryService(DefinitionRegistry registry, AggregateStore store,
                           RecomputeService recomputeService) {
        this.registry = registry;
        this.store = store;
        this.recomputeService = recomputeService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        // TODO implement
    }
}
