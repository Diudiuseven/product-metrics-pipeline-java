package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionClassifier;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.UserRetention;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RetentionClassifierTest {

    static final long DAY = 24L * 3600 * 1000;

    private MetricDefinition retention() {
        return MetricDefinition.builder()
                .id("r").version(1).type(MetricType.RETENTION)
                .anchorEvent("signup").retainedEvent("active")
                .retentionPeriodsMillis(List.of(DAY, 2 * DAY, 3 * DAY))
                .cohortBucketMillis(DAY)
                .build();
    }

    private UserEvent e(long seq, String name, long t) {
        return new UserEvent(seq, "id" + seq, name, t, 0, Map.of());
    }

    @Test
    void anchorIsFirstAnchorEventByEventTimeRegardlessOfArrivalOrder() {
        // signup@day3 arrives (seq) AFTER signup@day1; anchor must be day1.
        List<UserEvent> events = List.of(
                e(1, "signup", 3 * DAY),
                e(0, "signup", DAY),
                e(2, "active", DAY + 1000));
        UserRetention ur = RetentionClassifier.classify(retention(), events);
        assertEquals(DAY, ur.anchor().eventTime());
        assertEquals(DAY, ur.cohortStart());
    }

    @Test
    void periodBucketsAreHalfOpenAndDistinguishable() {
        List<UserEvent> events = List.of(
                e(0, "signup", 0),
                e(1, "active", DAY + 1),      // period 1 (day1..day2)
                e(2, "active", 3 * DAY - 1)); // period 2 (day2..day3)
        UserRetention ur = RetentionClassifier.classify(retention(), events);
        assertEquals(List.of(false, true, true), ur.retained());
    }

    @Test
    void eventExactlyAtPeriodBoundaryBelongsToLaterBucket() {
        List<UserEvent> events = List.of(e(0, "signup", 0), e(1, "active", DAY));
        UserRetention ur = RetentionClassifier.classify(retention(), events);
        assertEquals(List.of(false, true, false), ur.retained());
    }

    @Test
    void noAnchorYieldsNullAndIsExcludedFromCohorts() {
        assertNull(RetentionClassifier.classify(retention(), List.of(e(0, "active", DAY))));
    }

    @Test
    void duplicateRetainedEventsCountOnlyAsPresence() {
        List<UserEvent> events = List.of(
                e(0, "signup", 0),
                e(1, "active", 100), e(2, "active", 200), e(3, "active", 300));
        UserRetention ur = RetentionClassifier.classify(retention(), events);
        assertEquals(List.of(true, false, false), ur.retained());
    }
}
