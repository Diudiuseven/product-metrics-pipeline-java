package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiSmokeTest {

    @Autowired TestRestTemplate rest;
    @Autowired JdbcTemplate jdbc;
    @Autowired DefinitionRegistry registry;

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
    }

    @Test
    void fullFunnelApiFlow() throws InterruptedException {
        // register definition
        String defJson = """
                {
                  "id": "api_funnel",
                  "type": "FUNNEL",
                  "funnel": {
                    "steps": [
                      {"id": "s1", "eventType": "page_view"},
                      {"id": "s2", "eventType": "purchase"}
                    ],
                    "window": "PT1H",
                    "windowFrom": "FIRST_STEP",
                    "dimensions": [],
                    "bucketGranularity": "DAY"
                  }
                }
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var defResp = rest.postForEntity("/api/metrics", new HttpEntity<>(defJson, headers), Map.class);
        assertEquals(200, defResp.getStatusCode().value());

        // ingest events
        String e1 = """
                {"eventId":"a1","userId":"u1","eventType":"page_view","eventTime":"2026-10-01T00:00:00Z","dims":{}}
                """;
        String e2 = """
                {"eventId":"a2","userId":"u1","eventType":"purchase","eventTime":"2026-10-01T00:10:00Z","dims":{}}
                """;
        var r1 = rest.postForEntity("/api/events", new HttpEntity<>(e1, headers), Map.class);
        var r2 = rest.postForEntity("/api/events", new HttpEntity<>(e2, headers), Map.class);
        assertEquals(200, r1.getStatusCode().value());
        assertEquals(200, r2.getStatusCode().value());
        assertEquals(false, r1.getBody().get("duplicate"));
        // duplicate re-post
        var r1dup = rest.postForEntity("/api/events", new HttpEntity<>(e1, headers), Map.class);
        assertEquals(true, r1dup.getBody().get("duplicate"));

        // trigger build via admin recompute (async; poll until snapshot appears)
        var recResp = rest.postForEntity("/api/admin/recompute/api_funnel", null, Map.class);
        assertEquals(200, recResp.getStatusCode().value());

        // query funnel (poll for async recompute completion)
        Map body = null;
        java.util.List<Map> rows = null;
        for (int i = 0; i < 50; i++) {
            var queryResp = rest.getForEntity("/api/metrics/api_funnel/funnel?bucketFrom=2026-10-01&bucketTo=2026-10-01", Map.class);
            assertEquals(200, queryResp.getStatusCode().value());
            body = queryResp.getBody();
            assertNotNull(body);
            rows = (java.util.List<Map>) body.get("rows");
            if (!rows.isEmpty()) break;
            Thread.sleep(100);
        }
        assertEquals(1, rows.size());
        Map row = rows.get(0);
        assertEquals(1, ((Number) row.get("entered")).longValue());
        assertEquals(1, ((Number) row.get("converted")).longValue());
    }

    @Test
    void invalidDefinitionReturns400WithFieldErrors() {
        String bad = """
                {
                  "id": "bad_funnel",
                  "type": "FUNNEL",
                  "funnel": {
                    "steps": [{"id": "s1", "eventType": ""}],
                    "window": "PT1H"
                  }
                }
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var resp = rest.postForEntity("/api/metrics", new HttpEntity<>(bad, headers), Map.class);
        assertEquals(400, resp.getStatusCode().value());
        Map body = resp.getBody();
        assertNotNull(body);
        assertEquals("INVALID_DEFINITION", body.get("error"));
        var errors = (java.util.List<Map>) body.get("errors");
        assertTrue(errors.stream().anyMatch(e -> "funnel.steps[0].eventType".equals(e.get("field"))));
    }
}
