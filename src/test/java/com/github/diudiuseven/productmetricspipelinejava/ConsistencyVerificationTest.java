package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution;
import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecomputeService;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecoveryService;
import com.github.diudiuseven.productmetricspipelinejava.compute.RetentionAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.RetentionContribution;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.EventOp;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.RetentionSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelQueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyFailureReason;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyReport;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyVerifier;
import com.github.diudiuseven.productmetricspipelinejava.verify.VerificationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the consistency guarantees: reproducible verdicts over time, stability
 * without new input, row-identical incremental vs. full recompute, dimension-cap
 * path agreement, the publish gate on mismatch, and restart recovery.
 */
@SpringBootTest
class ConsistencyVerificationTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired RecomputeService recomputeService;
    @Autowired ConsistencyVerifier verifier;
    @Autowired RecoveryService recovery;
    @Autowired AggregateStore store;
    @Autowired FunnelQueryService funnelQuery;
    @Autowired DefinitionRegistry registry;
    @Autowired PipelineProperties props;
    @Autowired JdbcTemplate jdbc;

    private final Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
    private MetricDefinition funnelDef;
    private MetricDefinition retentionDef;
    private int savedMaxDims;

    @BeforeEach
    void setUp() {
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
        jdbc.execute("DELETE FROM consistency_report");
        jdbc.execute("DELETE FROM metric_gate");

        savedMaxDims = props.getMaxDimCombinations();

        funnelDef = new MetricDefinition();
        funnelDef.setId("cons_funnel");
        funnelDef.setType(MetricType.FUNNEL);
        funnelDef.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "add_cart", Map.of()),
                        new StepDefinition("s3", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of("country"), "DAY"));
        funnelDef.setVersion(1);

        retentionDef = new MetricDefinition();
        retentionDef.setId("cons_retention");
        retentionDef.setType(MetricType.RETENTION);
        retentionDef.setRetention(new RetentionSpec("app_open", "app_open",
                List.of(Duration.ofDays(1), Duration.ofDays(7)), null, "DAY", List.of()));
        retentionDef.setVersion(1);

        registry.register(funnelDef);
        registry.register(retentionDef);
    }

    @AfterEach
    void tearDown() {
        props.setMaxDimCombinations(savedMaxDims);
    }

    private Event evt(String id, String user, String type, Instant t, String country) {
        return new Event(id, user, type, t, t,
                country == null ? Map.of() : Map.of("country", country), null);
    }

    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    /** Array-friendly projection of a funnel row (records hold long[]/double[]). */
    private static List<Object> rowKey(FunnelQueryService.FunnelRow r) {
        return List.of(r.bucket(), r.dims(), r.entered(), r.converted(), r.skipped(), r.missing(),
                r.outOfWindow(), r.pending(), java.util.Arrays.toString(r.stepCounts()),
                java.util.Arrays.toString(r.stepRates()), r.conversionRate());
    }

    private static List<List<Object>> rowKeys(QueryPage<FunnelQueryService.FunnelRow> page) {
        return page.rows().stream().map(ConsistencyVerificationTest::rowKey).toList();
    }

    // 1. verdicts are reproducible: same committed input -> same result at any query instant
    @Test
    void verdictsReproducibleAcrossQueryInstants() throws Exception {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        builder.runIncremental(funnelDef);

        QueryPage<FunnelQueryService.FunnelRow> first = funnelQuery.query("cons_funnel", 1,
                null, null, null, 100, null);
        assertEquals(1, first.rows().size());
        // window judged against the user's own committed events, not the wall clock:
        // an old event with an open window stays PENDING forever
        assertEquals(1, first.rows().get(0).pending());
        assertEquals(0, first.rows().get(0).missing());

        Thread.sleep(150); // wall clock advances; no new input
        QueryPage<FunnelQueryService.FunnelRow> second = funnelQuery.query("cons_funnel", 1,
                null, null, null, 100, null);
        assertEquals(rowKeys(first), rowKeys(second));

        // a new event for the user past the window flips PENDING -> MISSING,
        // and the result equals a full recompute
        ingest.ingest(evt("e2", "u1", "heartbeat", t0.plus(Duration.ofHours(2)), "US"));
        builder.runIncremental(funnelDef);
        QueryPage<FunnelQueryService.FunnelRow> third = funnelQuery.query("cons_funnel", 1,
                null, null, null, 100, null);
        assertEquals(1, third.rows().get(0).missing());
        assertEquals(0, third.rows().get(0).pending());

        recomputeService.recompute(funnelDef, "MANUAL");
        QueryPage<FunnelQueryService.FunnelRow> afterRecompute = funnelQuery.query("cons_funnel", 1,
                null, null, null, 100, null);
        assertEquals(rowKeys(third), rowKeys(afterRecompute));
    }

    // 2. without new events nothing drifts; verification keeps passing
    @Test
    void noNewEventsNoDrift() throws Exception {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(10)), "US"));
        ingest.ingest(evt("e3", "u2", "page_view", t0, "DE"));
        builder.runIncremental(funnelDef);

        Map<String, FunnelAggregate> aggBefore = store.loadCurrentFunnel("cons_funnel", 1);
        Map<String, FunnelContribution> usersBefore = store.loadAllFunnelUsers("cons_funnel", 1);

        // repeated ticks with no new input: no new commit, no row changes
        assertEquals(0L, builder.runIncremental(funnelDef));
        Thread.sleep(120);
        assertEquals(0L, builder.runIncremental(funnelDef));
        assertEquals(aggBefore, store.loadCurrentFunnel("cons_funnel", 1));
        assertEquals(usersBefore, store.loadAllFunnelUsers("cons_funnel", 1));

        ConsistencyReport report = verifier.verifyNow(funnelDef);
        assertEquals(VerificationStatus.VERIFIED, report.status());

        // and a later full recompute still matches the committed rows exactly
        recomputeService.recompute(funnelDef, "MANUAL");
        assertEquals(aggBefore, store.loadCurrentFunnel("cons_funnel", 1));
        assertEquals(usersBefore, store.loadAllFunnelUsers("cons_funnel", 1));
    }

    // 3. incremental and full recompute are row-identical on the same input
    @Test
    void incrementalMatchesFullRecomputeRowByRow() {
        // out-of-order, late corrections and deletes across several batches
        ingest.ingest(evt("a1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("a2", "u2", "page_view", t0, "DE"));
        builder.runIncremental(funnelDef);
        ingest.ingest(evt("a3", "u1", "purchase", t0.plus(Duration.ofMinutes(30)), "US")); // skipped step
        ingest.ingest(evt("a4", "u3", "page_view", t0, "US"));
        builder.runIncremental(funnelDef);
        // correction: u3's entry moves to FR
        ingest.ingest(new Event("a4", "u3", "page_view", t0, t0, Map.of("country", "FR"), null));
        // delete: u2's entry disappears
        ingest.ingest(new Event("a2", "u2", "page_view", t0, t0, Map.of("country", "DE"), EventOp.DELETE));
        ingest.ingest(evt("a5", "u1", "add_cart", t0.plus(Duration.ofMinutes(20)), "US")); // late, in-window
        builder.runIncremental(funnelDef);

        ingest.ingest(evt("r1", "u1", "app_open", t0, null));
        ingest.ingest(evt("r2", "u1", "app_open", t0.plus(Duration.ofDays(1)), null));
        ingest.ingest(evt("r3", "u2", "app_open", t0, null));
        builder.runIncremental(retentionDef);

        Map<String, FunnelContribution> incFunnelUsers = store.loadAllFunnelUsers("cons_funnel", 1);
        Map<String, FunnelAggregate> incFunnelAgg = store.loadCurrentFunnel("cons_funnel", 1);
        Map<String, RetentionContribution> incRetUsers = store.loadAllRetentionUsers("cons_retention", 1);
        Map<String, RetentionAggregate> incRetAgg = store.loadCurrentRetention("cons_retention", 1);

        recomputeService.recompute(funnelDef, "MANUAL");
        recomputeService.recompute(retentionDef, "MANUAL");

        assertEquals(incFunnelUsers, store.loadAllFunnelUsers("cons_funnel", 1));
        assertEquals(incFunnelAgg, store.loadCurrentFunnel("cons_funnel", 1));
        assertEquals(incRetUsers, store.loadAllRetentionUsers("cons_retention", 1));
        assertEquals(incRetAgg, store.loadCurrentRetention("cons_retention", 1));

        assertEquals(VerificationStatus.VERIFIED, verifier.verifyNow(funnelDef).status());
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyNow(retentionDef).status());
    }

    // 4. dimension cap: both paths select the same groups; recompute rewrites nothing
    @Test
    void dimensionCapConsistentAcrossIncrementalAndRecompute() {
        props.setMaxDimCombinations(3);
        try {
            // batch 1: two countries fit under the cap
            ingest.ingest(evt("e1", "u1", "page_view", t0, "C1"));
            ingest.ingest(evt("e2", "u2", "page_view", t0, "C2"));
            builder.runIncremental(funnelDef);
            // batch 2: two more countries exceed the cap -> overflow
            ingest.ingest(evt("e3", "u3", "page_view", t0, "C3"));
            ingest.ingest(evt("e4", "u4", "page_view", t0, "C4"));
            builder.runIncremental(funnelDef);

            Map<String, FunnelContribution> beforeUsers = store.loadAllFunnelUsers("cons_funnel", 1);
            Map<String, FunnelAggregate> beforeAgg = store.loadCurrentFunnel("cons_funnel", 1);
            // cap is 3: C1, C2 committed first, C3 takes the last slot, C4 overflows
            assertEquals("country=C1", beforeUsers.get("u1").dims());
            assertEquals("country=C2", beforeUsers.get("u2").dims());
            assertEquals("country=C3", beforeUsers.get("u3").dims());
            assertEquals("__overflow__", beforeUsers.get("u4").dims());

            // full recompute of the same input must keep every committed group
            recomputeService.recompute(funnelDef, "MANUAL");
            assertEquals(beforeUsers, store.loadAllFunnelUsers("cons_funnel", 1));
            assertEquals(beforeAgg, store.loadCurrentFunnel("cons_funnel", 1));
            assertEquals(VerificationStatus.VERIFIED, verifier.verifyNow(funnelDef).status());

            // a corrected user whose raw dims changed is re-capped against the
            // same committed groups on both paths
            ingest.ingest(new Event("e4", "u4", "page_view", t0, t0, Map.of("country", "C1"), null));
            builder.runIncremental(funnelDef);
            assertEquals("country=C1", store.loadAllFunnelUsers("cons_funnel", 1).get("u4").dims());
            recomputeService.recompute(funnelDef, "MANUAL");
            assertEquals(store.loadAllFunnelUsers("cons_funnel", 1),
                    store.loadAllFunnelUsers("cons_funnel", 1));
            assertEquals(VerificationStatus.VERIFIED, verifier.verifyNow(funnelDef).status());
        } finally {
            props.setMaxDimCombinations(savedMaxDims);
        }
    }

    // 5. failed verification blocks publishes, localises the diff, and heals via recompute
    @Test
    void mismatchBlocksPublishesAndHeals() {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(10)), "US"));
        builder.runIncremental(funnelDef);
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyNow(funnelDef).status());

        // corrupt the published snapshot out-of-band (simulates a bad build)
        jdbc.update("UPDATE funnel_agg SET entered = entered + 5 WHERE metric_id = 'cons_funnel'");

        ConsistencyReport report = verifier.verifyNow(funnelDef);
        assertEquals(VerificationStatus.MISMATCH, report.status());
        assertEquals(ConsistencyFailureReason.CONSISTENCY_MISMATCH, report.reason());
        assertTrue(report.diffCount() > 0);
        assertNotNull(report.diffDetail());
        // diff is localised to the concrete group
        assertTrue(report.diffDetail().contains("FUNNEL_AGG"), report.diffDetail());
        assertTrue(report.diffDetail().contains("country=US"), report.diffDetail());

        // gate blocks new incremental publishes
        assertTrue(store.loadGate("cons_funnel", 1).orElseThrow().blocked());

        // heal: recompute + re-verify re-opens the gate and restores the rows
        recomputeService.recompute(funnelDef, "AUTO_HEAL");
        ConsistencyReport healed = verifier.verifyNow(funnelDef);
        assertEquals(VerificationStatus.VERIFIED, healed.status());
        assertFalse(store.loadGate("cons_funnel", 1).orElseThrow().blocked());
    }

    // 6. no committed input -> distinguishable "basis undetermined" status
    @Test
    void noCommittedInputReportsUndeterminedBasis() {
        ConsistencyReport report = verifier.verifyNow(funnelDef);
        assertEquals(VerificationStatus.NO_COMMITTED_SNAPSHOT, report.status());
        assertEquals(ConsistencyFailureReason.DECISION_BASIS_UNDETERMINED, report.reason());
    }

    // 7. restart recovery: blocked gate persists and healing resumes
    @Test
    void restartRecoveryResumesHealing() throws Exception {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        builder.runIncremental(funnelDef);
        Map<String, FunnelAggregate> committed = store.loadCurrentFunnel("cons_funnel", 1);

        jdbc.update("UPDATE funnel_agg SET entered = entered + 7 WHERE metric_id = 'cons_funnel'");
        assertEquals(VerificationStatus.MISMATCH, verifier.verifyNow(funnelDef).status());
        assertTrue(store.loadGate("cons_funnel", 1).orElseThrow().blocked());

        // simulate process restart: recovery sees the blocked gate and heals
        recovery.recover();
        await(() -> !store.loadGate("cons_funnel", 1).orElseThrow().blocked(), "gate to re-open");

        assertEquals(VerificationStatus.VERIFIED,
                store.latestReport("cons_funnel", 1).orElseThrow().status());
        assertEquals(committed, store.loadCurrentFunnel("cons_funnel", 1));
    }
}
