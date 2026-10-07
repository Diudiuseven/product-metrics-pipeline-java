package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Drives per-metric builders on a bounded thread pool. Each metric is
 * isolated: a slow metric occupies only its own worker and never blocks others.
 *
 * <p>Besides incremental builds, the scheduler keeps the consistency check
 * running continuously: idle metrics (no new events, only time passing) are
 * re-verified against a full replay at most every
 * {@code pipeline.consistency-interval}; a failed check escalates to an
 * authoritative recompute so the published state converges back.
 */
@Component
public class ComputeScheduler {

    private static final Logger log = LoggerFactory.getLogger(ComputeScheduler.class);

    private final DefinitionRegistry registry;
    private final IncrementalBuilder builder;
    private final RecomputeService recomputeService;
    private final ConsistencyVerifier verifier;
    private final PipelineProperties props;
    private final Executor builderExecutor;
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    private final Map<String, Instant> lastVerified = new ConcurrentHashMap<>();

    public ComputeScheduler(DefinitionRegistry registry, IncrementalBuilder builder,
                            RecomputeService recomputeService, ConsistencyVerifier verifier,
                            PipelineProperties props, Executor builderExecutor) {
        this.registry = registry;
        this.builder = builder;
        this.recomputeService = recomputeService;
        this.verifier = verifier;
        this.props = props;
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
                    if (commitId == -1L) {
                        // bound exceeded: escalate to full recompute
                        recomputeService.recompute(def, "BOUNDS");
                    } else if (commitId == -2L) {
                        // consistency check rejected the candidate: the
                        // authoritative recompute republishes a verified state
                        recomputeService.recompute(def, "CONSISTENCY");
                    } else if (commitId == 0L) {
                        verifyIfDue(def, key);
                    }
                } catch (Exception e) {
                    log.warn("incremental build failed for {}", key, e);
                } finally {
                    inFlight.remove(key);
                }
            });
        }
    }

    /** Re-verifies an idle metric when the consistency interval has elapsed. */
    private void verifyIfDue(MetricDefinition def, String key) {
        Instant last = lastVerified.get(key);
        if (last != null
                && last.plus(props.getConsistencyInterval()).isAfter(Instant.now())) {
            return;
        }
        lastVerified.put(key, Instant.now());
        VerificationStatus status = verifier.verifyCommitted(def);
        if (status == VerificationStatus.MISMATCH
                || status == VerificationStatus.HORIZON_UNDETERMINED) {
            log.warn("idle consistency check for {} failed: {}; escalating to recompute", key, status);
            recomputeService.recompute(def, "CONSISTENCY");
        }
    }

    /** Submits a recompute for a metric (used by admin API / recovery). */
    public void requestRecompute(String metricId, Integer version, String trigger) {
        MetricDefinition def = resolve(metricId, version);
        builderExecutor.execute(() -> {
            try {
                recomputeService.recompute(def, trigger);
            } catch (Exception e) {
                log.warn("recompute failed for {}", metricId, e);
            }
        });
    }

    /** Submits an on-demand consistency check (used by admin API). */
    public void requestVerification(String metricId, Integer version) {
        MetricDefinition def = resolve(metricId, version);
        builderExecutor.execute(() -> {
            try {
                verifier.verifyCommitted(def);
            } catch (Exception e) {
                log.warn("verification failed for {}", metricId, e);
            }
        });
    }

    private MetricDefinition resolve(String metricId, Integer version) {
        return version == null
                ? registry.getLatest(metricId).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId))
                : registry.get(metricId, version).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId + " v" + version));
    }
}
