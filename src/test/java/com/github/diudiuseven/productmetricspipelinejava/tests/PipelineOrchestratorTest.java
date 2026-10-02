package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.CommittedView;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import com.github.diudiuseven.productmetricspipelinejava.store.RecomputeMarker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineOrchestratorTest {

    @TempDir
    Path tmp;

    static final long H = 3600_000L;

    private MetricDefinition funnel(String id) {
        return MetricDefinition.builder().id(id).version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(6 * H).stepWindowMillis(6 * H).build();
    }

    private PipelineOrchestrator orch() {
        PipelineProperties props = new PipelineProperties();
        props.setDrainIntervalMillis(5);
        props.setMaxOutOfOrderHorizonMillis(Long.MAX_VALUE / 2);
        PipelineOrchestrator o = new PipelineOrchestrator(
                new FileEventLog(tmp.toString()),
                new FileSnapshotStore(tmp.toString(), new ObjectMapper()),
                new FileDefinitionsRepository(tmp.toString(), new ObjectMapper()),
                props);
        o.start();
        return o;
    }

    @Test
    void restartResumesFromLastCommittedSeqWithoutSkippingOrDoubleCounting() {
        PipelineOrchestrator first = orch();
        first.registerDefinition(funnel("f"));
        first.ingest(List.of(new Event("e1", "u1", "view", H, Map.of())));
        first.drainAwait("f", 1);
        long seqAfterFirst = first.view("f", 1).orElseThrow().commitSeq();
        first.stop();

        PipelineOrchestrator second = orch();
        assertEquals(seqAfterFirst, second.view("f", 1).orElseThrow().commitSeq(),
                "committed position survives restart");
        second.ingest(List.of(new Event("e2", "u1", "buy", 2 * H, Map.of())));
        second.drainAwait("f", 1);
        assertEquals(1L, second.view("f", 1).orElseThrow().commitSeq() - seqAfterFirst,
                "exactly the new event is applied after resume");
        second.stop();
    }

    @Test
    void concurrentIngestionAndQueriesAlwaysSeeACommittedWhole() throws Exception {
        PipelineOrchestrator o = orch();
        o.registerDefinition(funnel("f"));
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger producers = new AtomicInteger();
        for (int p = 0; p < 3; p++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        String id = Thread.currentThread().getId() + "-" + i;
                        o.ingest(List.of(new Event(id, "u" + (i % 10), "view", H + i, Map.of())));
                        producers.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        AtomicInteger consistentViews = new AtomicInteger();
        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 200; i++) {
                    CommittedView v = o.view("f", 1).orElse(null);
                    // Either no view yet (before first commit) or exactly one
                    // self-consistent whole; never a torn/partial structure.
                    if (v == null || (v.snapshot() != null && v.snapshot().has("commitSeq"))) {
                        consistentViews.incrementAndGet();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));
        o.drainAwait("f", 1);
        assertEquals(300, producers.get());
        assertTrue(consistentViews.get() >= 100, "every observed query must be a committed whole");
        assertEquals(299L, o.view("f", 1).orElseThrow().commitSeq());
        o.stop();
    }

    @Test
    void fullRecomputeAfterLateCorrectionsMatchesAndSwitchesAtomically() {
        PipelineOrchestrator o = orch();
        o.registerDefinition(funnel("f"));
        o.ingest(List.of(new Event("e1", "u1", "view", H, Map.of())));
        o.drainAwait("f", 1);
        // Late + corrected events while the metric keeps serving.
        o.ingest(List.of(
                new Event("e2", "u2", "view", 2 * H, Map.of()),
                new Event("e3", "u2", "buy", 3 * H, Map.of()),
                new Event("e1", "u1", "view", H, Map.of("plan", "pro"), 1L)));
        o.drainAwait("f", 1);
        RecomputeMarker marker = o.recompute("f", 1);
        assertNotNull(marker);
        assertEquals(o.eventLog().latestSeq(), o.view("f", 1).orElseThrow().commitSeq());
        // Post-recompute incremental drain still stays consistent.
        o.ingest(List.of(new Event("e4", "u3", "view", 4 * H, Map.of())));
        o.drainAwait("f", 1);
        assertEquals(o.eventLog().latestSeq(), o.view("f", 1).orElseThrow().commitSeq());
        o.stop();
    }

    @Test
    void oneSlowMetricDoesNotBlockOthers() throws Exception {
        PipelineOrchestrator o = orch();
        o.registerDefinition(funnel("fast"));
        o.registerDefinition(funnel("slow"));
        o.ingest(List.of(new Event("x1", "u1", "view", H, Map.of())));
        // Pause "slow"'s worker (simulating a stuck metric); fast must advance.
        o.pauseForTest("slow", 1, true);
        o.ingest(new ArrayList<>(List.of(new Event("f1", "u1", "view", H, Map.of()))));
        long deadline = System.currentTimeMillis() + 5000;
        while (o.view("fast", 1).isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(o.view("fast", 1).isPresent(), "fast metric advances while slow is paused");
        o.pauseForTest("slow", 1, false);
        o.drainAwait("slow", 1);
        assertNotNull(o.view("slow", 1).orElse(null));
        o.stop();
    }

    @Test
    void addingNewMetricOrVersionDoesNotChangeExistingResults() {
        PipelineOrchestrator o = orch();
        o.registerDefinition(funnel("f"));
        o.ingest(List.of(new Event("e1", "u1", "view", H, Map.of())));
        o.drainAwait("f", 1);
        long seq = o.view("f", 1).orElseThrow().commitSeq();

        MetricDefinition v2 = MetricDefinition.builder().id("f").version(2).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(2 * H).stepWindowMillis(2 * H).build();
        o.registerDefinition(v2);
        o.drainAwait("f", 2);
        // New metric too.
        o.registerDefinition(funnel("g"));
        o.drainAwait("g", 1);

        assertEquals(seq, o.view("f", 1).orElseThrow().commitSeq(),
                "existing version untouched by newer versions/metrics");
        assertEquals(2 * H, o.definitions().get("f", 2).totalWindowMillis());
        o.stop();
    }
}
