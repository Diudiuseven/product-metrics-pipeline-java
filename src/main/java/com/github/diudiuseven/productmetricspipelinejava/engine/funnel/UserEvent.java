package com.github.diudiuseven.productmetricspipelinejava.engine.funnel;

import java.util.List;
import java.util.Map;

/**
 * One stored per-user event (only events whose name belongs to the metric are
 * retained). {@code seq} is the global ingestion sequence; together with
 * {@code eventTime} it defines total order for out-of-order arrivals.
 */
public record UserEvent(long seq, String eventId, String name, long eventTime, long revision,
                        Map<String, String> dimensions) implements Comparable<UserEvent> {

    @Override
    public int compareTo(UserEvent o) {
        int c = Long.compare(eventTime, o.eventTime);
        if (c != 0) {
            return c;
        }
        return Long.compare(seq, o.seq);
    }

    /** Strictly later in total order (eventTime, then ingestion seq). */
    public boolean after(UserEvent o) {
        int c = Long.compare(eventTime, o.eventTime);
        if (c != 0) {
            return c > 0;
        }
        return seq > o.seq;
    }

    public List<String> dims(List<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        String[] out = new String[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            out[i] = dimensions.getOrDefault(keys.get(i), "");
        }
        return List.of(out);
    }
}
