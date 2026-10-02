package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileRecomputeMarkerStore;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import com.github.diudiuseven.productmetricspipelinejava.store.RecomputeMarker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failure recovery: a marker left by a recomputation that never finished must
 * be distinguishable from committed work. After restart the pipeline reports
 * it as aborted, keeps the previous checkpoint, and a subsequent successful
 * recompute clears the marker.
 */
class RecomputeRecoveryTest {

    @TempDir
    Path tmp;

    static final long H = 3600_000L;

    private MetricDefinition def() {
        return MetricDefinition.builder().id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(6 * H).stepWindowMillis(6 * H).build();
    }

    private PipelineOrchestrator orch() {
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tmp.toString());
        props.setMaxOutOfOrderHorizonMillis(Long.MAX_VALUE / 2);
        PipelineOrchestrator o = new PipelineOrchestrator(
                new FileEventLog(tmp.toString()),
                new FileSnapshotStore(tmp.toString(), new ObjectMapper()),
                new FileDefinitionsRepository(tmp.toString(), new ObjectMapper()), props);
        o.start();
        return o;
    }

    @Test
    void unfinishedMarkerOnRestartIsAbortedAndCommittedResultStaysAuthoritative() {
        PipelineOrchestrator first = orch();
        first.registerDefinition(def());
        first.ingest(List.of(new Event("e1", "u1", "view", H, Map.of())));
        first.drainAwait("f", 1);
        long committed = first.view("f", 1).orElseThrow().commitSeq();
        first.stop();

        // Simulate a crash mid-recompute: marker exists, pointer still old.
        FileRecomputeMarkerStore markers = new FileRecomputeMarkerStore(tmp.toString());
        markers.begin(new RecomputeMarker("f", 1, committed + 5, RecomputeMarker.BUILDING));
        assertTrue(markers.find("f", 1).isPresent());

        PipelineOrchestrator second = orch();
        assertEquals(1, second.abortedRecomputesOnStartup().size());
        assertEquals("f", second.abortedRecomputesOnStartup().get(0).metricId());
        // Old committed result is intact and queryable.
        assertEquals(committed, second.view("f", 1).orElseThrow().commitSeq());
        // Marker has been cleared: unfinished work no longer masquerades as state.
        assertFalse(new FileRecomputeMarkerStore(tmp.toString()).find("f", 1).isPresent());
        second.stop();
    }

    @Test
    void successfulRecomputeClearsMarker() {
        PipelineOrchestrator o = orch();
        o.registerDefinition(def());
        o.ingest(List.of(new Event("e1", "u1", "view", H, Map.of())));
        o.drainAwait("f", 1);
        o.recompute("f", 1);
        assertFalse(new FileRecomputeMarkerStore(tmp.toString()).find("f", 1).isPresent());
        o.stop();
    }
}
