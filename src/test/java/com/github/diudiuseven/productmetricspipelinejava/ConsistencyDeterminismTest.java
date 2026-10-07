package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.ConsistencyVerifier;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution;
import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecomputeService;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecoveryService;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.compute.VerificationReport;
import com.github.diudiuseven.productmetricspipelinejava.compute.VerificationStatus;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.event.EventOp;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelQueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the consistency guarantees: reproducible verdicts over time,
 * stability when no new events arrive, row-identical incremental vs full
 * recompute, deterministic dimension capping on both paths, rejection of
 * inconsistent candidates, and recovery alignment.
 */
@SpringBootTest
class ConsistencyDeterminismTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired RecomputeService recomputeService;
    @Autowired RecoveryService recovery;
    @Autowired ReplayEngine engine;
    @Autowired AggregateStore store;
    @Autowired ConsistencyVerifier verifier;
    @Autowired FunnelQueryService funnelQuery;
    @Autowired DefinitionRegistry registry;
    @Autowired PipelineProperties props;
    @Autowired JdbcTemplate jdbc;

    private final Instant t0 = Instant.parse("2026-10-01T00:00:00Z");
    private MetricDefinition def;

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
        jdbc.execute("DELETE FROM verification_report");

        def = new MetricDefinition();
        def.setId("det_funnel");
        def.setType(MetricType.FUNNEL);
        def.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP,
                List.of("country"), "DAY"));
        def.setVersion(1);
        registry.register(def);
    }

    private Event evt(String id, String user, String type, Instant t, String country) {
        return new Event(id, user, type, t, t, Map.of("country", country), null);
    }

    private QueryPage<FunnelQueryService.FunnelRow> query() {
        return funnelQuery.query("det_funnel", 1, null, null, null, 100, null);
    }

    /** Canonical string form (rows contain arrays, so record equals is unusable). */
    private static List<String> canonical(List<FunnelQueryService.FunnelRow> rows) {
        return rows.stream().map(r -> r.bucket() + "|" + r.dims() + "|" + r.entered() + "|"
                + r.converted() + "|" + r.skipped() + "|" + r.missing() + "|" + r.outOfWindow()
                + "|" + r.pending() + "|" + java.util.Arrays.toString(r.stepCounts())).toList();
    }

    @Test
    void repeatedQueriesOverTimeAreReproducible() throws Exception {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u1", "purchase", t0.plus(Duration.ofMinutes(5)), "US"));
        ingest.ingest(evt("e3", "u2", "page_view", t0, "DE"));
        builder.runIncremental(def);

        QueryPage<FunnelQueryService.FunnelRow> first = query();
        // time passes, no new events; repeated ticks and queries must not
        // change anything (PENDING stays PENDING, no new commits)
        Thread.sleep(50);
        assertEquals(0L, builder.runIncremental(def));
        QueryPage<FunnelQueryService.FunnelRow> second = query();
        assertEquals(canonical(first.rows()), canonical(second.rows()));
        assertEquals(first.commitId(), second.commitId());
        assertEquals(first.logSeqTo(), second.logSeqTo());

        // u2's window is judged against the user's own committed timeline, so
        // it stays PENDING no matter how much wall-clock time passes
        FunnelQueryService.FunnelRow deRow = second.rows().stream()
                .filter(r -> r.dims().equals("country=DE")).findFirst().orElseThrow();
        assertEquals(1, deRow.pending());
        assertEquals(0, deRow.missing());
    }

    @Test
    void verdictsChangeOnlyWhenNewInputArrives() {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        builder.runIncremental(def);
        assertEquals(1, query().rows().get(0).pending());

        // no new events -> repeated builds are no-ops, verdict stable
        assertEquals(0L, builder.runIncremental(def));
        assertEquals(0L, builder.runIncremental(def));
        assertEquals(1, query().rows().get(0).pending());

        // new input advances the user's committed timeline past the window
        ingest.ingest(evt("e2", "u1", "heartbeat", t0.plus(Duration.ofHours(2)), "US"));
        builder.runIncremental(def);
        QueryPage<FunnelQueryService.FunnelRow> page = query();
        assertEquals(1, page.rows().get(0).missing());
        assertEquals(0, page.rows().get(0).pending());

        // and the updated verdict equals a from-scratch replay
        Map<String, FunnelAggregate> replay = engine.replay(def, eventLog.findAllActiveOrdered()).funnel();
        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("det_funnel", 1);
        assertEquals(replay, stored);
    }

    @Test
    void incrementalIsRowIdenticalToFullRecompute() {
        // batch 1
        ingest.ingest(evt("b1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("b2", "u1", "purchase", t0.plus(Duration.ofMinutes(3)), "US"));
        ingest.ingest(evt("b3", "u2", "page_view", t0, "DE"));
        builder.runIncremental(def);
        assertRowIdenticalToReplay();

        // batch 2: correction + new users
        ingest.ingest(new Event("b3", "u2", "page_view", t0, t0, Map.of("country", "FR"), null));
        ingest.ingest(evt("b4", "u3", "page_view", t0.plus(Duration.ofMinutes(1)), "US"));
        ingest.ingest(evt("b5", "u3", "purchase", t0.plus(Duration.ofMinutes(90)), "US")); // oow
        builder.runIncremental(def);
        assertRowIdenticalToReplay();

        // batch 3: delete + late conversion
        ingest.ingest(new Event("b4", "u3", "page_view", t0, t0, Map.of("country", "US"), EventOp.DELETE));
        ingest.ingest(evt("b6", "u2", "purchase", t0.plus(Duration.ofMinutes(7)), "FR"));
        builder.runIncremental(def);
        assertRowIdenticalToReplay();

        // full recompute of the same committed input must not change a thing
        Map<String, FunnelContribution> usersBefore = store.loadAllFunnelUsers("det_funnel", 1);
        Map<String, FunnelAggregate> aggBefore = store.loadCurrentFunnel("det_funnel", 1);
        recomputeService.recompute(def, "MANUAL");
        assertEquals(usersBefore, store.loadAllFunnelUsers("det_funnel", 1));
        assertEquals(aggBefore, store.loadCurrentFunnel("det_funnel", 1));
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyCommitted(def));
    }

    private void assertRowIdenticalToReplay() {
        var replay = engine.replay(def, eventLog.findAllActiveOrdered());
        Map<String, FunnelContribution> expectedUsers = ReplayEngine.capFunnelDims(
                replay.funnelUsers(), store.loadCurrentFunnelDims("det_funnel", 1),
                props.getMaxDimCombinations());
        assertEquals(expectedUsers, store.loadAllFunnelUsers("det_funnel", 1), "per-user rows diverge");
        Map<String, FunnelAggregate> expectedAgg =
                ReplayEngine.aggregateFunnel(expectedUsers, def.getFunnel().steps().size());
        assertEquals(expectedAgg, store.loadCurrentFunnel("det_funnel", 1), "aggregate rows diverge");
    }

    @Test
    void dimensionCapIsStickyAndIdenticalOnBothPaths() {
        int oldMax = props.getMaxDimCombinations();
        props.setMaxDimCombinations(2);
        try {
            // C1 and C2 arrive first and become stable detail groups
            ingest.ingest(evt("d1", "u1", "page_view", t0, "C1"));
            builder.runIncremental(def);
            ingest.ingest(evt("d2", "u2", "page_view", t0, "C2"));
            builder.runIncremental(def);
            // C3 exceeds the bound -> overflow
            ingest.ingest(evt("d3", "u3", "page_view", t0, "C3"));
            builder.runIncremental(def);

            Map<String, FunnelAggregate> incremental = store.loadCurrentFunnel("det_funnel", 1);
            assertEquals(3, incremental.size());
            assertTrue(incremental.values().stream().anyMatch(a -> a.dims().equals("country=C1")));
            assertTrue(incremental.values().stream().anyMatch(a -> a.dims().equals("country=C2")));
            assertTrue(incremental.values().stream()
                    .anyMatch(a -> a.dims().equals(ReplayEngine.OVERFLOW_DIMS)));

            // recompute must select the same groups and not rewrite stable ones
            Map<String, FunnelContribution> usersBefore = store.loadAllFunnelUsers("det_funnel", 1);
            recomputeService.recompute(def, "MANUAL");
            assertEquals(incremental, store.loadCurrentFunnel("det_funnel", 1));
            assertEquals(usersBefore, store.loadAllFunnelUsers("det_funnel", 1));

            // a second recompute is a fixed point as well
            recomputeService.recompute(def, "MANUAL");
            assertEquals(incremental, store.loadCurrentFunnel("det_funnel", 1));
        } finally {
            props.setMaxDimCombinations(oldMax);
        }
    }

    @Test
    void inconsistentCandidateIsNotPublishedAndDiffsAreLocatable() {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u2", "page_view", t0, "DE"));
        builder.runIncremental(def);
        long committedSeq = store.loadCheckpoint("det_funnel", 1).orElseThrow().logSeq();
        long commitsBefore = store.listCommits("det_funnel", 1, 100).size();

        // simulate corruption of the committed per-user state
        jdbc.update("UPDATE funnel_user SET verdict = 'CONVERTED' WHERE metric_id = 'det_funnel' "
                + "AND user_id = 'u1'");

        // new input arrives for another user; the candidate built on top of
        // the corrupted state must be rejected, not published
        ingest.ingest(evt("e3", "u2", "purchase", t0.plus(Duration.ofMinutes(5)), "DE"));
        long rc = builder.runIncremental(def);
        assertEquals(-2L, rc, "corrupted candidate must be rejected");

        // nothing new was published: checkpoint and commit log unchanged
        assertEquals(committedSeq, store.loadCheckpoint("det_funnel", 1).orElseThrow().logSeq());
        assertEquals(commitsBefore, store.listCommits("det_funnel", 1, 100).size());

        // the report pinpoints the diverging user
        List<VerificationReport> reports = store.listVerificationReports("det_funnel", 1, 10);
        assertFalse(reports.isEmpty());
        VerificationReport latest = reports.get(0);
        assertEquals(VerificationStatus.MISMATCH, latest.status());
        assertNotNull(latest.diffs());
        assertTrue(latest.diffs().contains("u1"), "diff should locate user u1: " + latest.diffs());

        // the authoritative recompute converges back to a verified state
        recomputeService.recompute(def, "CONSISTENCY");
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyCommitted(def));
        assertRowIdenticalToReplay();
    }

    @Test
    void verificationSkipsBeyondConfiguredBound() {
        long oldMax = props.getConsistencyMaxEvents();
        props.setConsistencyMaxEvents(1);
        try {
            ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
            ingest.ingest(evt("e2", "u2", "page_view", t0, "DE"));
            long commitId = builder.runIncremental(def);
            assertTrue(commitId > 0, "publish still proceeds when verification is skipped");
            VerificationReport latest = store.listVerificationReports("det_funnel", 1, 1).get(0);
            assertEquals(VerificationStatus.SKIPPED_BOUNDS, latest.status());
        } finally {
            props.setConsistencyMaxEvents(oldMax);
        }
    }

    @Test
    void queriesSeeOnlyCommittedWholesDuringConcurrentRecomputeAndIngest() throws Exception {
        for (int i = 0; i < 60; i++) {
            ingest.ingest(evt("s" + i + "a", "u" + i, "page_view",
                    t0.plus(Duration.ofMinutes(i % 30)), "US"));
            ingest.ingest(evt("s" + i + "b", "u" + i, "purchase",
                    t0.plus(Duration.ofMinutes(i % 30)).plus(Duration.ofMinutes(5)), "US"));
        }
        builder.runIncremental(def);

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(3);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();

        // writer: more events + incremental builds
        futures.add(pool.submit(() -> {
            try {
                start.await();
                for (int i = 60; i < 90; i++) {
                    ingest.ingest(evt("s" + i + "a", "u" + i, "page_view",
                            t0.plus(Duration.ofMinutes(i % 30)), "DE"));
                    builder.runIncremental(def);
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                e.printStackTrace();
            }
        }));
        // recompute loop
        futures.add(pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 3; i++) {
                    recomputeService.recompute(def, "MANUAL");
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                e.printStackTrace();
            }
        }));
        // reader: every observed page must be internally self-consistent
        futures.add(pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 200; i++) {
                    QueryPage<FunnelQueryService.FunnelRow> page = query();
                    for (FunnelQueryService.FunnelRow row : page.rows()) {
                        assertTrue(row.entered() >= row.converted(), "row not self-consistent");
                        for (long sc : row.stepCounts()) {
                            assertTrue(sc <= row.entered() && sc >= 0, "step count out of range");
                        }
                        assertEquals(row.entered(),
                                row.converted() + row.skipped() + row.missing()
                                        + row.outOfWindow() + row.pending(),
                                "verdicts must partition entered users");
                    }
                    Thread.yield();
                }
            } catch (Exception e) {
                errors.incrementAndGet();
                e.printStackTrace();
            }
        }));
        start.countDown();
        for (var f : futures) {
            f.get();
        }
        pool.shutdown();
        assertEquals(0, errors.get());

        // after everything settles, the state verifies clean against a full replay
        builder.runIncremental(def);
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyCommitted(def));
        assertRowIdenticalToReplay();
    }

    @Test
    void recoveryAlignsWithFullRecompute() {
        ingest.ingest(evt("e1", "u1", "page_view", t0, "US"));
        ingest.ingest(evt("e2", "u1", "purchase", t0.plus(Duration.ofMinutes(5)), "US"));
        builder.runIncremental(def);

        // events arrive but the process "crashes" before the next build
        ingest.ingest(evt("e3", "u2", "page_view", t0, "DE"));
        recovery.recover();

        // after recovery the builder resumes and the result verifies clean
        builder.runIncremental(def);
        assertEquals(VerificationStatus.VERIFIED, verifier.verifyCommitted(def));
        assertRowIdenticalToReplay();

        // a full recompute over the same input is a fixed point
        Map<String, FunnelAggregate> before = store.loadCurrentFunnel("det_funnel", 1);
        recomputeService.recompute(def, "MANUAL");
        assertEquals(before, store.loadCurrentFunnel("det_funnel", 1));
    }
}
