package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelAggregate;
import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DimensionCapTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired ReplayEngine engine;
    @Autowired JdbcTemplate jdbc;
    @Autowired com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry registry;
    @Autowired com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore store;

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
        def.setId("cap_funnel");
        def.setType(MetricType.FUNNEL);
        def.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of())),
                null, FunnelSpec.WindowFrom.FIRST_STEP, List.of("country"), "DAY"));
        def.setVersion(1);
        registry.register(def);
    }

    @Test
    void dimCombinationsBeyondBoundMergeToOverflow() {
        // 12 distinct countries, bound is 50000 in test props -> no overflow expected.
        // Instead verify the cap helper directly with a small bound.
        int max = 3;
        Set<String> existing = new HashSet<>();
        var users = new java.util.LinkedHashMap<String, com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution>();
        for (int i = 0; i < 6; i++) {
            users.put("u" + i, new com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution(
                    "2026-10-01", "country=C" + i, 1,
                    com.github.diudiuseven.productmetricspipelinejava.compute.FunnelVerdict.ENTERED));
        }
        var capped = ReplayEngine.capFunnelDims(users, existing, max);
        long overflow = capped.values().stream().filter(c -> c.dims().equals(ReplayEngine.OVERFLOW_DIMS)).count();
        long real = capped.values().stream().filter(c -> !c.dims().equals(ReplayEngine.OVERFLOW_DIMS)).count();
        assertEquals(3, overflow);
        assertEquals(3, real);

        // end-to-end: 12 countries all present, no overflow (bound 50000)
        Instant t0 = Instant.parse("2026-10-01T00:00:00Z");
        for (int i = 0; i < 12; i++) {
            ingest.ingest(new Event("e" + i, "u" + i, "page_view", t0, t0,
                    Map.of("country", "C" + i), null));
        }
        builder.runIncremental(def);
        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("cap_funnel", 1);
        assertEquals(12, stored.size());
        // incremental == full replay
        Map<String, FunnelAggregate> replay = engine.replay(def, eventLog.findAllActiveOrdered()).funnel();
        assertEquals(replay.size(), stored.size());
        replay.forEach((k, v) -> assertEquals(v, stored.get(k)));
    }
}
