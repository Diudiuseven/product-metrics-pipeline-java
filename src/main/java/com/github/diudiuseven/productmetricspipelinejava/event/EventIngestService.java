package com.github.diudiuseven.productmetricspipelinejava.event;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Validates and durably ingests events. Ingestion is idempotent and never
 * blocks on aggregation: events are logged before any builder runs.
 */
@Service
public class EventIngestService {

    public record IngestResult(long logSeq, boolean duplicate, boolean corrected, boolean deleted) {
        public static IngestResult duplicate(long seq) { return new IngestResult(seq, true, false, false); }
        public static IngestResult corrected(long seq) { return new IngestResult(seq, false, true, false); }
        public static IngestResult deleted(long seq) { return new IngestResult(seq, false, false, true); }
        public static IngestResult created(long seq) { return new IngestResult(seq, false, false, false); }
    }

    private final EventLog eventLog;

    public EventIngestService(EventLog eventLog) {
        this.eventLog = eventLog;
    }

    public IngestResult ingest(Event event) {
        Event validated = validate(event);
        StoredEvent before = eventLog.findByEventId(validated.eventId()).orElse(null);
        long seq = eventLog.append(validated);
        if (validated.op() == EventOp.DELETE) {
            return IngestResult.deleted(seq);
        }
        if (validated.op() == EventOp.CORRECT) {
            return IngestResult.corrected(seq);
        }
        if (before != null && before.logSeq() == seq) {
            // identical re-PUT: append was a no-op
            return IngestResult.duplicate(seq);
        }
        return IngestResult.created(seq);
    }

    /**
     * Ingests a batch atomically: either all events are logged or none are.
     * Each item is still individually idempotent.
     */
    @Transactional
    public List<IngestResult> ingestBatch(List<Event> events) {
        List<IngestResult> results = new ArrayList<>(events.size());
        for (Event e : events) {
            results.add(ingest(e));
        }
        return results;
    }

    private Event validate(Event event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (isBlank(event.eventId())) {
            throw new IllegalArgumentException("event.eventId is required");
        }
        if (isBlank(event.userId())) {
            throw new IllegalArgumentException("event.userId is required");
        }
        if (isBlank(event.eventType())) {
            throw new IllegalArgumentException("event.eventType is required");
        }
        Instant eventTime = event.eventTime() == null ? Instant.now() : event.eventTime();
        EventOp op = event.op() == null ? EventOp.PUT : event.op();
        return new Event(event.eventId(), event.userId(), event.eventType(), eventTime,
                Instant.now(), event.dims() == null ? Map.of() : event.dims(), op);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
