package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelReport;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelUserResult;
import com.github.diudiuseven.productmetricspipelinejava.query.Page;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryDeadlineException;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryRequest;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.RetentionReport;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryServiceTest {

    @TempDir
    Path tmp;

    static final long H = 3600_000L;

    private Harness funnelWithDimensions(int maxGroups) {
        FileEventLog log = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        FileDefinitionsRepository defs = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        PipelineProperties props = new PipelineProperties();
        props.setMaxOutOfOrderHorizonMillis(Long.MAX_VALUE / 2);
        MetricDefinition def = MetricDefinition.builder().id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(6 * H).stepWindowMillis(6 * H)
                .dimensionKeys(List.of("city")).maxDimensionGroups(maxGroups)
                .build();
        defs.register(def);
        PipelineOrchestrator orch = new PipelineOrchestrator(log, snaps, defs, props);
        orch.start();
        return new Harness(orch, log, new QueryService(orch), def);
    }

    record Harness(PipelineOrchestrator orch, FileEventLog log, QueryService qs, MetricDefinition def) {
    }

    private QueryRequest req(String cursor, int pageSize, long deadlineOffsetNanos) {
        return new QueryRequest("f", 1, 0, 100 * H, cursor, pageSize,
                System.nanoTime() + deadlineOffsetNanos);
    }

    @Test
    void pagingReturnsAllRowsAcrossPagesAndThenCompletes() {
        Harness h = funnelWithDimensions(100);
        List<Event> batch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            batch.add(new Event("v" + i, "u" + i, "view", H, Map.of()));
        }
        h.log.ingest(batch);
        h.orch.drainAwait("f", 1); // package-visible drain helper

        Page<FunnelUserResult> p1 = h.qs.funnelUsers(req(null, 2, 60_000_000_000L));
        assertEquals(2, p1.rows().size());
        assertFalse(p1.complete());
        assertNotNull(p1.nextFrom());

        Page<FunnelUserResult> p2 = h.qs.funnelUsers(req(p1.nextFrom(), 2, 60_000_000_000L));
        assertEquals(2, p2.rows().size());

        Page<FunnelUserResult> p3 = h.qs.funnelUsers(req(p2.nextFrom(), 2, 60_000_000_000L));
        assertEquals(1, p3.rows().size());
        assertTrue(p3.complete());
        assertNull(p3.nextFrom());
    }

    @Test
    void deadlineThrowsAndNeverReturnsHalfPage() {
        Harness h = funnelWithDimensions(100);
        List<Event> batch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            batch.add(new Event("v" + i, "u" + i, "view", H, Map.of()));
        }
        h.log.ingest(batch);
        h.orch.drainAwait("f", 1);
        QueryRequest expired = new QueryRequest("f", 1, 0, 100 * H, null, 100,
                System.nanoTime() - 1);
        assertThrows(QueryDeadlineException.class, () -> h.qs.funnelUsers(expired));
    }

    @Test
    void highCardinalityDimensionsCollapseIntoOtherButCoreCountsStayExact() throws Exception {
        Harness h = funnelWithDimensions(3); // tiny bound
        List<Event> batch = new ArrayList<>();
        String[] cities = {"bj", "sh", "gz", "sz", "hz"};
        for (int i = 0; i < cities.length; i++) {
            batch.add(new Event("v" + i, "u" + i, "view", H, Map.of("city", cities[i])));
            if (i % 2 == 0) {
                batch.add(new Event("b" + i, "u" + i, "buy", 2 * H, Map.of("city", cities[i])));
            }
        }
        h.log.ingest(batch);
        h.orch.drainAwait("f", 1);

        // First page big enough to capture every retained group + OTHER.
        Page<FunnelReport> page = h.qs.funnelReport(
                new QueryRequest("f", 1, 0, 100 * H, null, 100, System.nanoTime() + 60_000_000_000L));
        List<FunnelReport> rows = page.rows();
        long entered = rows.stream().mapToLong(FunnelReport::entered).sum();
        long converted = rows.stream().mapToLong(FunnelReport::converted).sum();
        assertEquals(5, entered, "every user counted exactly once across retained+OTHER groups");
        assertEquals(3, converted, "u0,u2,u4 converted; collapsed group still counted");
        assertTrue(rows.stream().anyMatch(r -> r.dimensionValues().equals(QueryService.OTHER_GROUP)),
                "overflow must collapse into __OTHER__");
        h.orch.stop();
    }

    @Test
    void retentionPageShapeAndCohortCounts() {
        FileEventLog log = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        FileDefinitionsRepository defs = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        PipelineProperties props = new PipelineProperties();
        props.setMaxOutOfOrderHorizonMillis(Long.MAX_VALUE / 2);
        long day = 24L * H;
        MetricDefinition def = MetricDefinition.builder().id("r").version(1).type(MetricType.RETENTION)
                .anchorEvent("signup").retainedEvent("active")
                .retentionPeriodsMillis(List.of(day, 2 * day)).cohortBucketMillis(day)
                .dimensionKeys(List.of("city")).maxDimensionGroups(10).build();
        defs.register(def);
        PipelineOrchestrator orch = new PipelineOrchestrator(log, snaps, defs, props);
        orch.start();
        log.ingest(List.of(
                new Event("s1", "u1", "signup", day, Map.of("city", "bj")),
                new Event("a1", "u1", "active", day + 1000, Map.of("city", "bj"))));
        orch.drainAwait("r", 1);

        QueryService qs = new QueryService(orch);
        Page<RetentionReport> page = qs.retention(
                new QueryRequest("r", 1, 0, 10 * day, null, 100, System.nanoTime() + 60_000_000_000L));
        assertEquals(1, page.rows().size());
        RetentionReport r = page.rows().get(0);
        assertEquals(day, r.cohortStart());
        assertEquals(1, r.cohortSize());
        assertEquals(List.of(1L, 0L), r.retainedCounts());
        orch.stop();
    }
}
