package com.github.diudiuseven.productmetricspipelinejava.event;

import org.springframework.stereotype.Service;

import java.util.List;

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
        // TODO implement
        return IngestResult.created(eventLog.append(event));
    }

    public List<IngestResult> ingestBatch(List<Event> events) {
        // TODO implement
        return events.stream().map(this::ingest).toList();
    }
}
