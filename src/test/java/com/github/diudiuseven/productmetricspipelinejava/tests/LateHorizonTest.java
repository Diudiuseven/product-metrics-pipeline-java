package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Events older than the configured late horizon are rejected, not counted. */
class LateHorizonTest {

    @TempDir
    Path tmp;

    @Test
    void eventsBeyondHorizonAreReportedAndNotAppended() {
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tmp.toString());
        props.setMaxOutOfOrderHorizonMillis(1000); // 1 second
        PipelineOrchestrator o = new PipelineOrchestrator(
                new FileEventLog(tmp.toString()),
                new FileSnapshotStore(tmp.toString(), new ObjectMapper()),
                new FileDefinitionsRepository(tmp.toString(), new ObjectMapper()), props);

        long now = System.currentTimeMillis();
        Event fresh = new Event("fresh", "u1", "view", now, Map.of());
        Event ancient = new Event("old", "u2", "view", now - 10 * 60 * 1000L, Map.of());
        var result = o.ingest(List.of(fresh, ancient));

        assertEquals(1, result.appended().size());
        assertTrue(result.duplicateIds().contains("old"));
        assertEquals(0L, o.eventLog().latestSeq());
    }
}
