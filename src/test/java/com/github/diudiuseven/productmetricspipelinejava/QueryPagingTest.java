package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.IncrementalBuilder;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class QueryPagingTest {

    @Autowired EventIngestService ingest;
    @Autowired IncrementalBuilder builder;
    @Autowired FunnelQueryService funnelQuery;
    @Autowired JdbcTemplate jdbc;
    @Autowired com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry registry;

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
        def.setId("paging_funnel");
        def.setType(MetricType.FUNNEL);
        def.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of())),
                null, FunnelSpec.WindowFrom.FIRST_STEP, List.of("country"), "DAY"));
        def.setVersion(1);
        registry.register(def);
    }

    @Test
    void keysetPagingCoversAllRowsWithoutDuplicates() {
        String[] countries = {"US", "DE", "FR", "JP"};
        for (int d = 0; d < 5; d++) {
            Instant day = Instant.parse("2026-10-0" + (d + 1) + "T00:00:00Z");
            for (String c : countries) {
                ingest.ingest(new Event("e" + d + c, "u" + d + c, "page_view", day, day,
                        Map.of("country", c), null));
            }
        }
        builder.runIncremental(def);

        List<FunnelQueryService.FunnelRow> all = new ArrayList<>();
        String token = null;
        int pages = 0;
        while (true) {
            QueryPage<FunnelQueryService.FunnelRow> page = funnelQuery.query(
                    "paging_funnel", 1, null, null, null, 7, token);
            assertTrue(page.rows().size() <= 7, "page exceeded pageSize");
            all.addAll(page.rows());
            pages++;
            if (page.nextPageToken() == null) {
                break;
            }
            token = page.nextPageToken();
            if (pages > 20) {
                fail("paging did not terminate");
            }
        }
        assertEquals(20, all.size());
        Set<String> keys = new HashSet<>();
        for (FunnelQueryService.FunnelRow r : all) {
            assertTrue(keys.add(r.bucket() + "|" + r.dims()), "duplicate row: " + r);
        }
        for (int i = 1; i < all.size(); i++) {
            FunnelQueryService.FunnelRow a = all.get(i - 1);
            FunnelQueryService.FunnelRow b = all.get(i);
            assertTrue((a.bucket() + a.dims()).compareTo(b.bucket() + b.dims()) < 0);
        }
    }

    @Test
    void timeRangeFilterRestrictsRows() {
        for (int d = 0; d < 5; d++) {
            Instant day = Instant.parse("2026-10-0" + (d + 1) + "T00:00:00Z");
            ingest.ingest(new Event("e" + d, "u" + d, "page_view", day, day, Map.of(), null));
        }
        builder.runIncremental(def);

        QueryPage<FunnelQueryService.FunnelRow> page = funnelQuery.query(
                "paging_funnel", 1, "2026-10-02", "2026-10-04", null, 100, null);
        assertEquals(3, page.rows().size());
        page.rows().forEach(r ->
                assertTrue(r.bucket().compareTo("2026-10-02") >= 0
                        && r.bucket().compareTo("2026-10-04") <= 0));
    }

    @Test
    void pageSizeIsCapped() {
        for (int d = 0; d < 3; d++) {
            Instant day = Instant.parse("2026-10-0" + (d + 1) + "T00:00:00Z");
            ingest.ingest(new Event("e" + d, "u" + d, "page_view", day, day, Map.of(), null));
        }
        builder.runIncremental(def);

        QueryPage<FunnelQueryService.FunnelRow> page = funnelQuery.query(
                "paging_funnel", 1, null, null, null, 10000, null);
        assertTrue(page.rows().size() <= 1000);
    }
}
