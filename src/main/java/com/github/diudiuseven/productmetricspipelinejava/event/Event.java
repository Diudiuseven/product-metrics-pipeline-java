package com.github.diudiuseven.productmetricspipelinejava.event;

import java.time.Instant;
import java.util.Map;

/**
 * A behavior event. {@code eventId} is the idempotency key; {@code eventTime}
 * is when the behavior happened (used for ordering/funnels/cohorts);
 * {@code receiveTime} is when the pipeline durably received it.
 */
public record Event(
        String eventId,
        String userId,
        String eventType,
        Instant eventTime,
        Instant receiveTime,
        Map<String, String> dims,
        EventOp op
) {
    public Event {
        dims = dims == null ? Map.of() : Map.copyOf(dims);
    }
}
