package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.RetentionSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DefinitionValidationTest {

    @Autowired DefinitionRegistry registry;
    @Autowired JdbcTemplate jdbc;

    private MetricDefinition validFunnel() {
        MetricDefinition d = new MetricDefinition();
        d.setId("v_funnel");
        d.setType(MetricType.FUNNEL);
        d.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        return d;
    }

    @Test
    void rejectsMissingEventTypeWithFieldPath() {
        MetricDefinition d = validFunnel();
        d.getFunnel().steps().get(1).eventType(); // no-op
        StepDefinition bad = new StepDefinition("s2", "", Map.of());
        d.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()), bad),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e ->
                e.field().equals("funnel.steps[1].eventType") && e.code().equals("REQUIRED")));
    }

    @Test
    void rejectsMissingWindow() {
        MetricDefinition d = validFunnel();
        d.setFunnel(new FunnelSpec(d.getFunnel().steps(), null, FunnelSpec.WindowFrom.FIRST_STEP,
                List.of(), "DAY"));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.field().equals("funnel.window")));
    }

    @Test
    void rejectsOutOfRangeWindow() {
        MetricDefinition d = validFunnel();
        d.setFunnel(new FunnelSpec(d.getFunnel().steps(), Duration.ofDays(40),
                FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e ->
                e.field().equals("funnel.window") && e.code().equals("OUT_OF_RANGE")));
    }

    @Test
    void rejectsDuplicateDimensions() {
        MetricDefinition d = validFunnel();
        d.setFunnel(new FunnelSpec(d.getFunnel().steps(), Duration.ofHours(1),
                FunnelSpec.WindowFrom.FIRST_STEP, List.of("country", "country"), "DAY"));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e ->
                e.field().equals("funnel.dimensions") && e.code().equals("DUPLICATE")));
    }

    @Test
    void rejectsNonAscendingRetentionPeriods() {
        MetricDefinition d = new MetricDefinition();
        d.setId("v_retention");
        d.setType(MetricType.RETENTION);
        d.setRetention(new RetentionSpec("app_open", "app_open",
                List.of(Duration.ofDays(7), Duration.ofDays(1)), null, "DAY", List.of()));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e ->
                e.field().equals("retention.periods[1]") && e.code().equals("ASCENDING")));
    }

    @Test
    void rejectsConflictingSpecs() {
        MetricDefinition d = validFunnel();
        d.setRetention(new RetentionSpec("a", "b", List.of(Duration.ofDays(1)), null, "DAY", List.of()));
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.field().equals("retention")));
    }

    @Test
    void versioningKeepsOldVersionsQueryableAndUnchanged() {
        jdbc.execute("DELETE FROM metric_def WHERE id = 'versioned'");
        MetricDefinition v1 = validFunnel();
        v1.setId("versioned");
        v1.setVersion(0);
        registry.register(v1);
        assertEquals(1, registry.maxVersion("versioned"));

        MetricDefinition v2 = validFunnel();
        v2.setId("versioned");
        v2.setVersion(0);
        registry.register(v2);
        assertEquals(2, registry.maxVersion("versioned"));

        // both versions queryable
        assertTrue(registry.get("versioned", 1).isPresent());
        assertTrue(registry.get("versioned", 2).isPresent());
        assertEquals(2, registry.getLatest("versioned").orElseThrow().getVersion());

        // re-registering v1 is a conflict, never overwrites history
        MetricDefinition v1Again = validFunnel();
        v1Again.setId("versioned");
        v1Again.setVersion(1);
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(v1Again));
        assertTrue(ex.getErrors().stream().anyMatch(e ->
                e.field().equals("version") && e.code().equals("CONFLICT")));
    }

    @Test
    void invalidIdRejected() {
        MetricDefinition d = validFunnel();
        d.setId("bad id!");
        DefinitionException ex = assertThrows(DefinitionException.class, () -> registry.register(d));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.field().equals("id") && e.code().equals("PATTERN")));
    }
}
