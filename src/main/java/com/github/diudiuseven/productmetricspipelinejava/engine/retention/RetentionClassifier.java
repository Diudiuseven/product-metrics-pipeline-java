package com.github.diudiuseven.productmetricspipelinejava.engine.retention;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure, deterministic retention classification.
 *
 * <p>The anchor is each user's first {@code anchorEvent} in total order
 * (earliest eventTime; ties broken by ingestion seq). Cohort bucket is
 * {@code floor(anchorTime / cohortBucketMillis) * cohortBucketMillis}. A user
 * belongs to the queried cohort range when its anchor eventTime is within
 * {@code [fromEventTime, toEventTime)}.
 *
 * <p>Period p (0-based over {@code retentionPeriodsMillis}) counts retention
 * when some {@code retainedEvent} has eventTime in
 * {@code [anchor + periods[p-1] (or anchor for p=0), anchor + periods[p])}.
 * Periods are therefore distinguishable buckets, not cumulative.
 */
public final class RetentionClassifier {

    private RetentionClassifier() {
    }

    public static UserRetention classify(MetricDefinition def, List<UserEvent> events) {
        List<UserEvent> sorted = new ArrayList<>(events);
        sorted.sort(null);
        UserEvent anchor = null;
        for (UserEvent e : sorted) {
            if (e.name().equals(def.anchorEvent())) {
                anchor = e;
                break;
            }
        }
        if (anchor == null) {
            return null;
        }
        List<Long> periods = def.retentionPeriodsMillis();
        List<Boolean> retained = new ArrayList<>(periods.size());
        for (int i = 0; i < periods.size(); i++) {
            long start = anchor.eventTime() + (i == 0 ? 0L : periods.get(i - 1));
            long end = anchor.eventTime() + periods.get(i);
            boolean hit = false;
            for (UserEvent e : sorted) {
                if (!e.name().equals(def.retainedEvent())) {
                    continue;
                }
                long t = e.eventTime();
                if (t >= start && t < end) {
                    hit = true;
                    break;
                }
            }
            retained.add(hit);
        }
        long bucket = def.cohortBucketMillis();
        long cohortStart = Math.floorDiv(anchor.eventTime(), bucket) * bucket;
        return new UserRetention(cohortStart, anchor, retained);
    }
}
