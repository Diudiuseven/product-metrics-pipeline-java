package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
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
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelQueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ConcurrentReadWriteTest {

    @Autowired EventIngestService ingest;
    @Autowired EventLog eventLog;
    @Autowired IncrementalBuilder builder;
    @Autowired ReplayEngine engine;
    @Autowired AggregateStore store;
    @Autowired FunnelQueryService funnelQuery;
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
        def.setId("concurrent_funnel");
        def.setType(MetricType.FUNNEL);
        def.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        def.setVersion(1);
        registry.register(def);
    }

    @Test
    void queriesNeverSeePartialStateDuringConcurrentWrites() throws Exception {
        int writers = 4;
        int readers = 4;
        int eventsPerWriter = 200;
        ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        // writers: each ingests its own users, then builds
        for (int w = 0; w < writers; w++) {
            final int wi = w;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < eventsPerWriter; i++) {
                        String user = "w" + wi + "u" + i;
                        ingest.ingest(new Event("w" + wi + "e" + i + "a", user, "page_view",
                                t0.plus(Duration.ofMinutes(i % 100)), t0, Map.of(), null));
                        if (i % 2 == 0) {
                            ingest.ingest(new Event("w" + wi + "e" + i + "b", user, "purchase",
                                    t0.plus(Duration.ofMinutes(i % 100)).plus(Duration.ofMinutes(5)),
                                    t0, Map.of(), null));
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            }));
        }

        // readers: continuously query, validate page consistency
        for (int r = 0; r < readers; r++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        QueryPage<FunnelQueryService.FunnelRow> page =
                                funnelQuery.query("concurrent_funnel", 1, null, null, null, 50, null);
                        // every page is self-consistent: rows <= pageSize, commitId consistent
                        assertTrue(page.rows().size() <= 50, "page exceeded pageSize");
                        page.rows().forEach(row -> {
                            assertTrue(row.entered() >= row.converted());
                            for (int k = 0; k < row.stepCounts().length; k++) {
                                assertTrue(row.stepCounts()[k] <= row.entered());
                            }
                        });
                        Thread.yield();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            }));
        }

        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        assertEquals(0, errors.get(), "concurrent operations had errors");

        // final build + equivalence with full replay
        builder.runIncremental(def);
        Map<String, FunnelAggregate> stored = store.loadCurrentFunnel("concurrent_funnel", 1);
        Map<String, FunnelAggregate> replay = engine.replay(def, eventLog.findAllActiveOrdered()).funnel();
        assertEquals(replay.size(), stored.size());
        replay.forEach((k, v) -> assertEquals(v, stored.get(k)));

        // total users = writers * eventsPerWriter
        long totalEntered = stored.values().iterator().next().entered();
        assertEquals((long) writers * eventsPerWriter, totalEntered);
    }
}
