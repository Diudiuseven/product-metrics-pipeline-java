package com.github.diudiuseven.productmetricspipelinejava.tests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end HTTP coverage: invalid definitions are rejected at load with
 * field-located reasons; duplicate events are reported, not counted; the
 * funnel report reaches a one-conversion state. Each test class gets a fresh
 * temp data directory so runs are independent.
 */
@SpringBootTest
@AutoConfigureMockMvc
class HttpApiIntegrationTest {

    @TempDir
    static Path tmp;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("pipeline.data-dir", () -> tmp.toString());
        registry.add("pipeline.drain-interval-millis", () -> "10");
        // Test fixtures use arbitrary timestamps; disable the late horizon.
        registry.add("pipeline.max-out-of-order-horizon-millis", () -> String.valueOf(Long.MAX_VALUE / 2));
    }

    @Autowired
    MockMvc mvc;

    private static long now() {
        return System.currentTimeMillis();
    }

    @Test
    void invalidDefinitionReturnsFieldLocatedReasons() throws Exception {
        String body = """
                {"id":"bad","version":1,"type":"FUNNEL",
                 "steps":[{"order":0,"eventName":"a"}],
                 "totalWindowMillis":-5,"stepWindowMillis":100}
                """;
        mvc.perform(post("/api/definitions").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fields.steps").exists())
                .andExpect(jsonPath("$.fields.totalWindowMillis").exists());
    }

    @Test
    void ingestDuplicatesAndQueryFunnelFlow() throws Exception {
        long t = now();
        String def = """
                {"id":"shop","version":1,"type":"FUNNEL",
                 "steps":[{"order":0,"eventName":"view"},{"order":1,"eventName":"buy"}],
                 "totalWindowMillis":3600000,"stepWindowMillis":3600000,
                 "dimensionKeys":["plan"]}
                """;
        mvc.perform(post("/api/definitions").contentType(MediaType.APPLICATION_JSON).content(def))
                .andExpect(status().isOk());

        String events = """
                {"events":[
                  {"eventId":"h1","userId":"u1","name":"view","eventTime":%d,"dimensions":{"plan":"free"},"revision":0},
                  {"eventId":"h2","userId":"u1","name":"buy","eventTime":%d,"dimensions":{"plan":"free"},"revision":0},
                  {"eventId":"h1","userId":"u1","name":"view","eventTime":%d,"dimensions":{"plan":"free"},"revision":0}
                ]}
                """.formatted(t, t + 1000, t);
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(events))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.appended").value(2))
                .andExpect(jsonPath("$.duplicateIds[0]").value("h1"));

        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            String content = mvc.perform(get("/api/query/funnel/report")
                            .param("metricId", "shop")
                            .param("from", String.valueOf(t - 1000))
                            .param("to", String.valueOf(t + 3_600_000)))
                    .andReturn().getResponse().getContentAsString();
            if (content.contains("\"converted\":1")) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("funnel report never showed the conversion");
    }

    @Test
    void unknownMetricVersionIsRejected() throws Exception {
        mvc.perform(get("/api/query/funnel/users")
                        .param("metricId", "nope")
                        .param("from", "1").param("to", "2"))
                .andExpect(status().is4xxClientError());
    }
}
