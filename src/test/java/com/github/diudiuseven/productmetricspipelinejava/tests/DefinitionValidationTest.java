package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DefinitionValidationTest {

    private DefinitionException assertInvalid(MetricDefinition.Builder b) {
        try {
            b.build();
            throw new AssertionError("expected DefinitionException");
        } catch (DefinitionException e) {
            return e;
        }
    }

    @Test
    void rejectsBlankIdAndBadVersion() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("  ").version(0).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "a"), new FunnelStep(1, "b")))
                .totalWindowMillis(10).stepWindowMillis(10));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("id")));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("version")));
    }

    @Test
    void rejectsNonContiguousStepsAndLocatesTheIndex() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "a"), new FunnelStep(2, "b")))
                .totalWindowMillis(10).stepWindowMillis(10));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("steps[1].order")),
                "expected field-located error, got " + e.problems());
    }

    @Test
    void rejectsSingleStepFunnelAndDuplicateStepNames() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "a")))
                .totalWindowMillis(10).stepWindowMillis(10));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("steps")));
    }

    @Test
    void rejectsStepWindowLargerThanTotal() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "a"), new FunnelStep(1, "b")))
                .totalWindowMillis(10).stepWindowMillis(20));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("stepWindowMillis")));
    }

    @Test
    void rejectsRetentionWithoutAnchorOrPeriods() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("r").version(1).type(MetricType.RETENTION));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("anchorEvent")));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("retainedEvent")));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("retentionPeriodsMillis")));
    }

    @Test
    void rejectsNonIncreasingRetentionPeriodsWithIndex() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("r").version(1).type(MetricType.RETENTION)
                .anchorEvent("a").retainedEvent("b")
                .retentionPeriodsMillis(List.of(100L, 50L)));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("retentionPeriodsMillis[1]")));
    }

    @Test
    void rejectsDuplicateDimensionKeys() {
        DefinitionException e = assertInvalid(MetricDefinition.builder()
                .id("r").version(1).type(MetricType.RETENTION)
                .anchorEvent("a").retainedEvent("b")
                .retentionPeriodsMillis(List.of(100L))
                .dimensionKeys(List.of("x", "x")));
        assertTrue(e.problems().stream().anyMatch(p -> p.field().equals("dimensionKeys")));
    }
}
