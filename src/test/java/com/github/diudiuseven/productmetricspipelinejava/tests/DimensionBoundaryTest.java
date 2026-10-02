package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelReport;
import com.github.diudiuseven.productmetricspipelinejava.query.Page;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryRequest;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryService;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DimensionBoundaryTest {

    @TempDir
    Path tmp;

    static final long H = 3600_000L;

    private PipelineOrchestrator orch(int maxGroups) {
        PipelineProperties props = new PipelineProperties();
        props.setDataDir(tmp.toString());
        props.setMaxOutOfOrderHorizonMillis(Long.MAX_VALUE / 2);
        PipelineOrchestrator o = new PipelineOrchestrator(
                new FileEventLog(tmp.toString()),
                new FileSnapshotStore(tmp.toString(), new ObjectMapper()),
                new FileDefinitionsRepository(tmp.toString(), new ObjectMapper()), props);
        MetricDefinition def = MetricDefinition.builder().id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(6 * H).stepWindowMillis(6 * H)
                .dimensionKeys(List.of("city")).maxDimensionGroups(maxGroups).build();
        o.start();
        o.registerDefinition(def);
        return o;
    }

    private Page<FunnelReport> report(QueryService qs) {
        return qs.funnelReport(new QueryRequest("f", 1, 0, 100 * H, null, 100,
                System.nanoTime() + 60_000_000_000L));
    }

    @Test
    void exactlyKDistinctGroupsDoNotProduceOtherBucket() {
        int k = 3;
        PipelineOrchestrator o = orch(k);
        List<Event> batch = new ArrayList<>();
        String[] cities = {"bj", "sh", "gz"}; // exactly K
        for (int i = 0; i < cities.length; i++) {
            batch.add(new Event("v" + i, "u" + i, "view", H, Map.of("city", cities[i])));
        }
        o.ingest(batch);
        o.drainAwait("f", 1);
        Page<FunnelReport> page = report(new QueryService(o));
        assertFalse(page.rows().stream().anyMatch(r -> r.dimensionValues().equals(QueryService.OTHER_GROUP)),
                "exactly K distinct groups must not collapse anything");
        o.stop();
    }

    @Test
    void moreThanKGroupsCollapseIntoOther() {
        int k = 3;
        PipelineOrchestrator o = orch(k);
        List<Event> batch = new ArrayList<>();
        String[] cities = {"bj", "sh", "gz", "sz"}; // K+1
        for (int i = 0; i < cities.length; i++) {
            batch.add(new Event("v" + i, "u" + i, "view", H, Map.of("city", cities[i])));
        }
        o.ingest(batch);
        o.drainAwait("f", 1);
        Page<FunnelReport> page = report(new QueryService(o));
        List<FunnelReport> rows = page.rows();
        assertTrue(rows.stream().anyMatch(r -> r.dimensionValues().equals(QueryService.OTHER_GROUP)));
        long entered = rows.stream().mapToLong(FunnelReport::entered).sum();
        org.junit.jupiter.api.Assertions.assertEquals(4, entered, "all four users still counted once");
        o.stop();
    }
}
