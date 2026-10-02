package com.github.diudiuseven.productmetricspipelinejava.store;

import com.github.diudiuseven.productmetricspipelinejava.model.Event;

import java.util.Map;

/**
 * An event after it has been durably appended to the {@link EventLog}.
 * {@code seq} is the globally monotonic, gap-free ingestion sequence assigned
 * within an ingest batch. All deterministic processing is ordered by seq.
 */
public record EventRecord(long seq,
                          String eventId,
                          String userId,
                          String name,
                          long eventTime,
                          long ingestTime,
                          long revision,
                          Map<String, String> dimensions) {

    public Event event() {
        return new Event(eventId, userId, name, eventTime, dimensions, revision);
    }
}
