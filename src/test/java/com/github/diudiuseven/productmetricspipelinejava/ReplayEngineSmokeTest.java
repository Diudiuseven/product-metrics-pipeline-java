package com.github.diudiuseven.productmetricspipelinejava;

import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelContribution;
import com.github.diudiuseven.productmetricspipelinejava.compute.FunnelVerdict;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.compute.RetentionContribution;
import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventOp;
import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.metric.RetentionSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReplayEngineSmokeTest {

    private final ReplayEngine engine = new ReplayEngine();
    private final Instant t0 = Instant.parse("2026-10-01T00:00:00Z");

    private StoredEvent ev(String eventId, String user, String type, Instant t) {
        return new StoredEvent(1L, new Event(eventId, user, type, t, t, Map.of(), EventOp.PUT));
    }

    private MetricDefinition funnel() {
        MetricDefinition d = new MetricDefinition();
        d.setId("f");
        d.setVersion(1);
        d.setType(MetricType.FUNNEL);
        d.setFunnel(new FunnelSpec(
                List.of(new StepDefinition("s1", "page_view", Map.of()),
                        new StepDefinition("s2", "add_cart", Map.of()),
                        new StepDefinition("s3", "purchase", Map.of())),
                Duration.ofHours(1), FunnelSpec.WindowFrom.FIRST_STEP, List.of(), "DAY"));
        return d;
    }

    @Test
    void convertsInOrderWithinWindow() {
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", t0),
                ev("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(10))),
                ev("e3", "u1", "purchase", t0.plus(Duration.ofMinutes(20))));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertNotNull(c);
        assertEquals(FunnelVerdict.CONVERTED, c.verdict());
        assertEquals(0b111, c.stepMask());
        assertEquals("2026-10-01", c.bucket());
    }

    @Test
    void outOfOrderArrivalStillConverts() {
        // events arrive in reverse order; replay sorts by event time
        List<StoredEvent> events = List.of(
                ev("e3", "u1", "purchase", t0.plus(Duration.ofMinutes(20))),
                ev("e1", "u1", "page_view", t0),
                ev("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(10))));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertEquals(FunnelVerdict.CONVERTED, c.verdict());
    }

    @Test
    void skippedStepNotConverted() {
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", t0),
                ev("e3", "u1", "purchase", t0.plus(Duration.ofMinutes(10))));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertEquals(FunnelVerdict.SKIPPED_STEP, c.verdict());
        assertEquals(0b001, c.stepMask());
    }

    @Test
    void outOfWindowNotConverted() {
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", t0),
                ev("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(70))));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertEquals(FunnelVerdict.OUT_OF_WINDOW, c.verdict());
    }

    @Test
    void missingWhenWindowExpired() {
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", t0));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertEquals(FunnelVerdict.MISSING, c.verdict());
    }

    @Test
    void pendingWhenWindowOpen() {
        // event time is "now"-ish relative to window? use a time close to now
        Instant now = Instant.now();
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", now.minus(Duration.ofMinutes(5))));
        FunnelContribution c = engine.evaluateFunnel(funnel(), events);
        assertEquals(FunnelVerdict.PENDING, c.verdict());
    }

    @Test
    void windowFromPreviousStep() {
        MetricDefinition d = funnel();
        d.setFunnel(new FunnelSpec(d.getFunnel().steps(), Duration.ofMinutes(10),
                FunnelSpec.WindowFrom.PREVIOUS_STEP, List.of(), "DAY"));
        List<StoredEvent> events = List.of(
                ev("e1", "u1", "page_view", t0),
                ev("e2", "u1", "add_cart", t0.plus(Duration.ofMinutes(9))),
                ev("e3", "u1", "purchase", t0.plus(Duration.ofMinutes(25))));
        FunnelContribution c = engine.evaluateFunnel(d, events);
        // s2 within 10min of s1, but s3 > 10min after s2 -> out of window
        assertEquals(FunnelVerdict.OUT_OF_WINDOW, c.verdict());
    }

    @Test
    void retentionAnchoredOnFirstOccurrence() {
        MetricDefinition d = new MetricDefinition();
        d.setId("r");
        d.setVersion(1);
        d.setType(MetricType.RETENTION);
        d.setRetention(new RetentionSpec("app_open", "app_open",
                List.of(Duration.ofDays(1), Duration.ofDays(7)), null, "DAY", List.of()));
        // user opens app on day 1 and day 7 -> retained for period 1 (day1) but not period 7?
        // anchor = first app_open = t0; period1 = [t0+1d, t0+2d); period7 = [t0+7d, t0+8d)
        List<StoredEvent> events = List.of(
                ev("late", "u1", "app_open", t0.plus(Duration.ofDays(7))),
                ev("first", "u1", "app_open", t0),
                ev("d1", "u1", "app_open", t0.plus(Duration.ofDays(1)).plus(Duration.ofHours(2))));
        RetentionContribution c = engine.evaluateRetention(d, events);
        assertNotNull(c);
        assertEquals("2026-10-01", c.cohort());
        // period1 retained (d1 event in [+1d,+2d)); period7: late event at +7d in [+7d,+8d) -> retained
        assertEquals(0b11, c.mask());
    }
}
