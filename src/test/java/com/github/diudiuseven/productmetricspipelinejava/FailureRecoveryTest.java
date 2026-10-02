package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
import com.github.diudiuseven.productmetricspipelinejava.compute.RecoveryService;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
import com.github.diudiuseven.productmetricspipelinejava.event.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
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

@SpringBootTest
class FailureRecoveryTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired RecoveryService recovery;
    @Autowired ReplayEngine engine;
    @Autowired AggregateStore store;
    @Autowired JdbcTemplate jdbc;
    @Autowired com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry registry;

    private final Instant t0 = Instant.parse("2026-10-01T00:00:00Z");
    private MetricDefinition def;

    @BeforeEach
    void setUp() {
        jdbc.execute("DELETE FROM event_log");
        jdbc.execute("DELETE FROM metric_def");
        jdbc.execute("DELETE FROM snapshot_commit");
        jdbc.execute("DELETE FROM funnel_agg");
        jdbc.execute("DELETE FROM funnel_user");
        jdbc.execute("DELETE FROM builder_checkpoint");
        jdbc.execute("DELETE FROM recompute_task");
        jdbc.execute("DELETE FROM funnel_agg_scratch");
        jdbc.execute("DELETE FROM funnel_user_scratch");

        def = new MetricDefinition();
        def.setId("recovery_funnel");
        def.setType(MetricType.FUNNEL);
        def.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        def.setVersion(1);
        registry.register(def);
    }

    private Event evt(String id, String user, String type, Instant t) {
        return new Event(id, user, type, t, t, Map.of(), null);
    }

    @Test
    void resumesFromCheckpointWithoutDoubleCounting() {
        // batch 1: committed
        ingest.ingest(evt("a1", "u1", "page_view", t0));
        ingest.ingest(evt("a2", "u1", "purchase", t0.plus(Duration.ofMinutes(5))));
        builder.runIncremental(def);

        // batch 2: ingested but NOT yet built (crash before commit)
        ingest.ingest(evt("b1", "u2", "page_view", t0));
        ingest.ingest(evt("b2", "u2", "purchase", t0.plus(Duration.ofMinutes(5))));
        ingest.ingest(evt("b3", "u3", "page_view", t0));

        // "restart": builder resumes from last committed checkpoint
        builder.runIncremental(def);

        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("recovery_funnel", 1);
        Map<String, FunnelAggregate> replay = engine.replay(def, eventLog.findAllActiveOrdered()).funnel();
        assertEquals(replay.size(), stored.size());
        replay.forEach((k, v) -> assertEquals(v, stored.get(k), "mismatch " + k));

        // totals: 3 entered, 2 converted
        long entered = stored.values().iterator().next().entered();
        long converted = stored.values().iterator().next().converted();
        assertEquals(3, entered);
        assertEquals(2, converted);

        // checkpoint advanced past batch 2
        var cp = store.loadCheckpoint("recovery_funnel", 1).orElseThrow();
        assertEquals(eventLog.maxSeq(), cp.logSeq());
    }

    @Test
    void interruptedRecomputeIsCleanedAndResultsRemainIntact() {
        ingest.ingest(evt("a1", "u1", "page_view", t0));
        ingest.ingest(evt("a2", "u1", "purchase", t0.plus(Duration.ofMinutes(5))));
        builder.runIncremental(def);
        Map<String, FunnelAggregate> before = store.loadCurrentFunnel("recovery_funnel", 1);

        // simulate crash mid-recompute: a RUNNING task with scratch rows
        org.springframework.jdbc.support.GeneratedKeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();
        jdbc.update(con -> {
            var ps = con.prepareStatement(
                    "INSERT INTO recompute_task (metric_id, version, log_seq_from, log_seq_to, status, started_at) "
                            + "VALUES ('recovery_funnel', 1, 0, 10, 'RUNNING', CURRENT_TIMESTAMP)",
                    new String[]{"task_id"});
            return ps;
        }, kh);
        Long taskId = kh.getKey().longValue();
        assertNotNull(taskId);
        jdbc.update("INSERT INTO funnel_agg_scratch (task_id, bucket, dims, entered, converted, skipped, "
                + "missing, out_of_window, pending, step_counts) VALUES (?, '2026-10-01', '__all__', 99, 99, 0, 0, 0, 0, '[99,99]')",
                taskId);

        // recovery on restart
        recovery.recover();

        // task marked INTERRUPTED, scratch discarded
        String status = jdbc.queryForObject("SELECT status FROM recompute_task WHERE task_id = ?",
                String.class, taskId);
        assertEquals("INTERRUPTED", status);
        Integer scratchCount = jdbc.queryForObject(
                "SELECT COUNT(*) FROM funnel_agg_scratch WHERE task_id = ?", Integer.class, taskId);
        assertEquals(0, scratchCount);

        // committed snapshot untouched
        Map<String, FunnelAggregate> after = store.loadCurrentFunnel("recovery_funnel", 1);
        assertEquals(before, after);

        // builder continues correctly after recovery
        ingest.ingest(evt("b1", "u2", "page_view", t0));
        builder.runIncremental(def);
        Map<String, FunnelAggregate> replay = engine.replay(def, eventLog.findAllActiveOrdered()).funnel();
        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("recovery_funnel", 1);
        replay.forEach((k, v) -> assertEquals(v, stored.get(k)));
    }

    @Test
    void committedResultsAndUnfinishedTasksAreDistinguishable() {
        ingest.ingest(evt("a1", "u1", "page_view", t0));
        builder.runIncremental(def);

        // a commit record exists
        List<AggregateStore.CommitRecord> commits = store.listCommits("recovery_funnel", 1, 10);
        assertFalse(commits.isEmpty());
        assertEquals("COMMITTED", commits.get(0).status());

        // no running tasks after clean recovery
        recovery.recover();
        assertTrue(store.findRunningTasks().isEmpty());
    }
}
