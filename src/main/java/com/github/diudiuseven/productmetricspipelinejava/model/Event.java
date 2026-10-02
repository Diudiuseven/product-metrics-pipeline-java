package com.github.diudiuseven.productmetricspipelinejava.model;

import java.util.Collections;
import java.util.Map;

/**
 * Incoming behavioural event. Time semantics:
 * {@code eventTime} is when the behaviour actually happened (may arrive out of
 * order / late), {@code ingestTime} is assigned by the pipeline at ingestion.
 */
public record Event(String eventId,
                    String userId,
                    String name,
                    long eventTime,
                    Map<String, String> dimensions,
                    long revision) {

    public Event {
        dimensions = dimensions == null ? Map.of() : Collections.unmodifiableMap(new java.util.HashMap<>(dimensions));
    }

    /** Convenience constructor for a fresh (non-correction) event. */
    public Event(String eventId, String userId, String name, long eventTime, Map<String, String> dimensions) {
        this(eventId, userId, name, eventTime, dimensions, 0L);
    }
}
