package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelClassifier;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.Journey;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FunnelClassifierTest {

    private MetricDefinition funnel(long total, long stepWindow) {
        return MetricDefinition.builder()
                .id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"),
                        new FunnelStep(1, "cart"),
                        new FunnelStep(2, "buy")))
                .totalWindowMillis(total).stepWindowMillis(stepWindow)
                .build();
    }

    private UserEvent e(long seq, String name, long t) {
        return new UserEvent(seq, "id" + seq, name, t, 0, Map.of());
    }

    @Test
    void convertsWhenStepsOrderedInsideBothWindows() {
        MetricDefinition f = funnel(100, 60);
        List<UserEvent> events = List.of(e(0, "view", 0), e(1, "cart", 50), e(2, "buy", 90));
        Journey j = FunnelClassifier.classify(f, events, 0, 100);
        assertEquals(FunnelStatus.CONVERTED, j.status());
        assertEquals(2, j.reachedStep());
        assertEquals(List.of(0L, 50L, 90L), j.stepTimes());
    }

    @Test
    void outOfOrderArrivalIsOrderedByEventTimeAndDoesNotCreateFalseStep() {
        MetricDefinition f = funnel(100, 60);
        // Physical arrival order (seq) is view, buy, cart; but event times must
        // govern step order. buy@20 arrives before cart@30 and must not count as
        // step 2 out of order.
        List<UserEvent> events = List.of(e(0, "view", 0), e(1, "buy", 20), e(2, "cart", 30));
        Journey j = FunnelClassifier.classify(f, events, 0, 100);
        assertEquals(FunnelStatus.DROPPED, j.status());
        assertEquals(1, j.reachedStep(), "reached cart but no in-order buy within bounds");
    }

    @Test
    void missingStepIsDroppedAtExactStep() {
        MetricDefinition f = funnel(100, 60);
        Journey j = FunnelClassifier.classify(f, List.of(e(0, "view", 0), e(1, "buy", 10)), 0, 100);
        assertEquals(FunnelStatus.DROPPED, j.status());
        assertEquals(0, j.reachedStep(), "cart missing => dropped at step 1 (reached 0)");
    }

    @Test
    void stepOutsideStepWindowDropsEvenIfInsideTotalWindow() {
        MetricDefinition f = funnel(1000, 60);
        Journey j = FunnelClassifier.classify(f,
                List.of(e(0, "view", 0), e(1, "cart", 70), e(2, "buy", 80)), 0, 1000);
        assertEquals(FunnelStatus.DROPPED, j.status());
        assertEquals(0, j.reachedStep());
    }

    @Test
    void stepOutsideTotalWindowDropsEvenIfEachStepGapIsSmall() {
        MetricDefinition f = funnel(150, 150);
        Journey j = FunnelClassifier.classify(f,
                List.of(e(0, "view", 0), e(1, "cart", 80), e(2, "buy", 160)), 0, 1000);
        assertEquals(FunnelStatus.DROPPED, j.status());
        assertEquals(1, j.reachedStep());
    }

    @Test
    void noAnchorInRangeIsNotStarted() {
        MetricDefinition f = funnel(100, 60);
        Journey j = FunnelClassifier.classify(f,
                List.of(e(0, "view", 200), e(1, "cart", 210), e(2, "buy", 220)), 0, 100);
        assertEquals(FunnelStatus.NOT_STARTED, j.status());
    }

    @Test
    void choosesBestJourneyAcrossMultipleAnchorsDeterministically() {
        MetricDefinition f = funnel(100, 100);
        // First anchor drops (cart outside its 100-window); a later anchor completes.
        List<UserEvent> events = List.of(
                e(0, "view", 0), e(1, "cart", 150),
                e(2, "view", 200), e(3, "cart", 210), e(4, "buy", 220));
        Journey j = FunnelClassifier.classify(f, events, 0, 400);
        assertEquals(FunnelStatus.CONVERTED, j.status());
        assertEquals(200L, j.anchor().eventTime());
    }

    @Test
    void equalEventTimesAreOrderedByIngestSeqAndCannotStepBackwards() {
        MetricDefinition f = funnel(100, 60);
        // Same-time events are ordered by seq. Here buy@50 (seq 1) precedes
        // cart@50 (seq 2); after cart there is no strictly-later buy.
        List<UserEvent> events = List.of(
                new UserEvent(0, "a", "view", 0, 0, Map.of()),
                new UserEvent(1, "b", "buy", 50, 0, Map.of()),
                new UserEvent(2, "c", "cart", 50, 0, Map.of()));
        Journey j = FunnelClassifier.classify(f, events, 0, 100);
        assertEquals(FunnelStatus.DROPPED, j.status());
        assertEquals(1, j.reachedStep());

        // ...but the mirror ordering (cart seq1, buy seq2) converts, proving
        // equal-time steps follow the deterministic (eventTime, seq) order.
        List<UserEvent> events2 = List.of(
                new UserEvent(0, "a", "view", 0, 0, Map.of()),
                new UserEvent(1, "c", "cart", 50, 0, Map.of()),
                new UserEvent(2, "b", "buy", 50, 0, Map.of()));
        Journey j2 = FunnelClassifier.classify(f, events2, 0, 100);
        assertEquals(FunnelStatus.CONVERTED, j2.status());
    }
}
