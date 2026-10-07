package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecomputeService;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.RetentionSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelQueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import com.github.diudiuseven.productmetricspipelinejava.query.RetentionQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class PipelineIntegrationTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired RecomputeService recomputeService;
    @Autowired ReplayEngine engine;
    @Autowired AggregateStore store;
    @Autowired FunnelQueryService funnelQuery;
    @Autowired RetentionQueryService retentionQuery;
    @Autowired com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry registry;
    @Autowired org.springframework.jdbc.core.JdbcTemplate jdbc;

    private final Instant t0 = Instant.parse("2026-10-01T00:00:00Z");
    private MetricDefinition funnelDef;
    private MetricDefinition retentionDef;

    @BeforeEach
    void setUp() {
        // clean slate per test (shared in-memory DB)
        jdbc.execute("DELETE FROM event_log");
        jdbc.execute("DELETE FROM metric_def");
        jdbc.execute("DELETE FROM snapshot_commit");
        jdbc.execute("DELETE FROM funnel_agg");
        jdbc.execute("DELETE FROM funnel_user");
        jdbc.execute("DELETE FROM retention_agg");
        jdbc.execute("DELETE FROM retention_user");
        jdbc.execute("DELETE FROM builder_checkpoint");
        jdbc.execute("DELETE FROM recompute_task");
        jdbc.execute("DELETE FROM funnel_agg_scratch");
        jdbc.execute("DELETE FROM funnel_user_scratch");
        jdbc.execute("DELETE FROM retention_agg_scratch");
        jdbc.execute("DELETE FROM retention_user_scratch");

        funnelDef = new MetricDefinition();
        funnelDef.setId("signup_funnel");
        funnelDef.setType(MetricType.FUNNEL);
        funnelDef.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "add_cart", Map.of()),
                        new StepDefinition("s3", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP,
                List.of("country"), "DAY"));
        funnelDef.setVersion(1);

        retentionDef = new MetricDefinition();
        retentionDef.setId("app_retention");
        retentionDef.setType(MetricType.RETENTION);
        retentionDef.setRetention(new RetentionSpec("app_open", "app_open",
                List.of(Duration.ofDays(1), Duration.ofDays(7)), null, "DAY", List.of()));
        retentionDef.setVersion(1);

        if (registry.get("signup_funnel", 1).isEmpty()) {
            registry.register(funnelDef);
        }
        if (registry.get("app_retention", 1).isEmpty()) {
            registry.register(retentionDef);
        }
    }

    private Event evt(String id, String user, String type, Instant t, String country) {
        return new Event(id, user, type, t, t, Map.of("country", country == null ? "__none__" : country),
                null);
    }

    @Test
    void funnelAggregationAndLateCorrection() {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(10)), "US"));
        ingest.ingest(evt("e3", "u1", "purchase", t0.plus(Duration.ofMinutes(20)), "US"));
        ingest.ingest(evt("e4", "u2", "page_view", t0, "US"));
        ingest.ingest(evt("e5", "u2", "add_cart", t0.plus(Duration.ofMinutes(70)), "US")); // oow
        ingest.ingest(evt("e6", "u3", "page_view", t0, "US"));
        ingest.ingest(evt("e7", "u3", "purchase", t0.plus(Duration.ofMinutes(10)), "US")); // skipped
        ingest.ingest(evt("e8", "u4", "page_view", t0, "DE"));
        // u4's decision basis advances past the 1h window -> MISSING
        ingest.ingest(evt("e8b", "u4", "heartbeat", t0.plus(Duration.ofHours(2)), "DE"));

        builder.runIncremental(funnelDef);

        QueryPage<FunnelQueryService.FunnelRow> page = funnelQuery.query("signup_funnel", 1,
                "2026-10-01", "2026-10-01", null, 100, null);
        // no dim filter -> one row per dimension tuple (US, DE)
        assertEquals(2, page.rows().size());
        FunnelQueryService.FunnelRow usRow = page.rows().stream()
                .filter(r -> r.dims().equals("country=US")).findFirst().orElseThrow();
        FunnelQueryService.FunnelRow deRow = page.rows().stream()
                .filter(r -> r.dims().equals("country=DE")).findFirst().orElseThrow();
        assertEquals(3, usRow.entered());
        assertEquals(1, usRow.converted());
        assertEquals(1, usRow.outOfWindow());
        assertEquals(1, usRow.skipped());
        assertEquals(1, deRow.entered());
        assertEquals(0, deRow.converted());
        assertEquals(1, deRow.missing());
        // totals across dims
        long totalEntered = page.rows().stream().mapToLong(FunnelQueryService.FunnelRow::entered).sum();
        long totalConverted = page.rows().stream().mapToLong(FunnelQueryService.FunnelRow::converted).sum();
        assertEquals(4, totalEntered);
        assertEquals(1, totalConverted);

        // dimension split
        QueryPage<FunnelQueryService.FunnelRow> us = funnelQuery.query("signup_funnel", 1,
                null, null, "{\"country\":\"US\"}", 100, null);
        assertEquals(1, us.rows().size());
        assertEquals(3, us.rows().get(0).entered());
        QueryPage<FunnelQueryService.FunnelRow> de = funnelQuery.query("signup_funnel", 1,
                null, null, "{\"country\":\"DE\"}", 100, null);
        assertEquals(1, de.rows().size());
        assertEquals(1, de.rows().get(0).entered());

        // late events correct the already-committed snapshot
        ingest.ingest(evt("e9", "u4", "add_cart", t0.plus(Duration.ofMinutes(5)), "DE"));
        ingest.ingest(evt("e10", "u4", "purchase", t0.plus(Duration.ofMinutes(9)), "DE"));
        builder.runIncremental(funnelDef);

        page = funnelQuery.query("signup_funnel", 1, "2026-10-01", "2026-10-01", null, 100, null);
        assertEquals(2, page.rows().size());
        FunnelQueryService.FunnelRow deAfter = page.rows().stream()
                .filter(r -> r.dims().equals("country=DE")).findFirst().orElseThrow();
        assertEquals(1, deAfter.entered());
        assertEquals(1, deAfter.converted());
        assertEquals(0, deAfter.missing());
        long totalEnteredAfter = page.rows().stream().mapToLong(FunnelQueryService.FunnelRow::entered).sum();
        long totalConvertedAfter = page.rows().stream().mapToLong(FunnelQueryService.FunnelRow::converted).sum();
        assertEquals(4, totalEnteredAfter);
        assertEquals(2, totalConvertedAfter);

        // corrected result must be row-identical to a full replay
        List<StoredEvent> all = eventLog.findAllActiveOrdered();
        Map<String, FunnelAggregate> replay = engine.replay(funnelDef, all).funnel();
        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("signup_funnel", 1);
        assertEquals(replay.size(), stored.size());
        for (Map.Entry<String, FunnelAggregate> e : replay.entrySet()) {
            assertEquals(e.getValue(), stored.get(e.getKey()), "mismatch for " + e.getKey());
        }
    }

    @Test
    void duplicateEventsAreIdempotent() {
        Event e = evt("dup1", "u1", "page_view", t0, "US");
        long seq1 = ingest.ingest(e).logSeq();
        long seq2 = ingest.ingest(e).logSeq();
        assertEquals(seq1, seq2);
        assertEquals(1, eventLog.countActive());

        // correction with changed payload appends a new record
        Event corrected = new Event("dup1", "u1", "page_view", t0, t0,
                Map.of("country", "CA"), null);
        long seq3 = ingest.ingest(corrected).logSeq();
        assertNotEquals(seq1, seq3);
        assertEquals(1, eventLog.countActive());

        // delete tombstones the event
        Event deleted = new Event("dup1", "u1", "page_view", t0, t0,
                Map.of("country", "CA"), com.github.diudiuseven.productmetricspipelinejava.event.EventOp.DELETE);
        ingest.ingest(deleted);
        assertEquals(0, eventLog.countActive());
    }

    @Test
    void retentionCohortsAndReproducibility() {
        ingest.ingest(evt("r1", "u1", "app_open", t0, null));
        ingest.ingest(evt("r2", "u1", "app_open", t0.plus(Duration.ofDays(1)).plus(Duration.ofHours(2)), null));
        ingest.ingest(evt("r3", "u1", "app_open", t0.plus(Duration.ofDays(7)), null));
        ingest.ingest(evt("r4", "u2", "app_open", t0, null));

        builder.runIncremental(retentionDef);

        QueryPage<RetentionQueryService.RetentionRow> page = retentionQuery.query(
                "app_retention", 1, null, null, null, 100, null);
        assertEquals(1, page.rows().size());
        RetentionQueryService.RetentionRow row = page.rows().get(0);
        assertEquals(2, row.cohortSize());
        assertArrayEquals(new long[]{1, 1}, row.retained());
        assertEquals(0.5, row.retentionRates()[0], 1e-9);

        // full replay equivalence
        Map<String, com.github.diudiuseven.productmetricspipelinejava.compute.RetentionAggregate> replay =
                engine.replay(retentionDef, eventLog.findAllActiveOrdered()).retention();
        Map<String, com.github.diudiuseven.productmetricspipelinejava.compute.RetentionAggregate> stored =
                store.loadCurrentRetention("app_retention", 1);
        assertEquals(replay, stored);
    }

    @Test
    void fullRecomputePublishesAtomicallyAndMatchesIncremental() {
        for (int i = 0; i < 50; i++) {
            String user = "u" + i;
            ingest.ingest(evt("b" + i + "v1", user, "page_view", t0.plus(Duration.ofHours(i % 5)), "US"));
            if (i % 2 == 0) {
                ingest.ingest(evt("b" + i + "v2", user, "add_cart",
                        t0.plus(Duration.ofHours(i % 5)).plus(Duration.ofMinutes(5)), "US"));
            }
        }
        builder.runIncremental(funnelDef);
        Map<String, FunnelAggregate> before = new LinkedHashMap<>(store.loadCurrentFunnel("signup_funnel", 1));

        long commitId = recomputeService.recompute(funnelDef, "MANUAL");
        assertTrue(commitId > 0);

        Map<String, FunnelAggregate> after = store.loadCurrentFunnel("signup_funnel", 1);
        assertEquals(before.size(), after.size());
        for (Map.Entry<String, FunnelAggregate> e : before.entrySet()) {
            assertEquals(e.getValue(), after.get(e.getKey()));
        }
        // checkpoint advanced to the recompute commit
        var cp = store.loadCheckpoint("signup_funnel", 1).orElseThrow();
        assertEquals(commitId, cp.lastCommitId());
    }
}
