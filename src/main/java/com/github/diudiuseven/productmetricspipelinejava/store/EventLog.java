package com.github.diudiuseven.productmetricspipelinejava.store;

import com.github.diudiuseven.productmetricspipelinejava.model.Event;

import java.util.List;

/**
 * Append-only, durable, totally-ordered event log.
 *
 * <p>Contract:
 * <ul>
 *   <li>seqs are gap-free and monotonic across process restarts;</li>
 *   <li>a batch is visible to readers only once fully durable (fsync +
 *       directory entry), so recovery never sees a torn batch;</li>
 *   <li>duplicate {@code eventId}s are rejected atomically with the batch.</li>
 * </ul>
 */
public interface EventLog {

    /** Durably append a batch; assigns contiguous seqs. Thread-safe. */
    IngestResult ingest(List<Event> batch);

    /** Highest durable seq, or -1 when empty. */
    long latestSeq();

    /**
     * Read up to {@code max} records with seq strictly greater than
     * {@code afterSeq}, in seq order. Never returns a partial trailing batch.
     */
    List<EventRecord> readNext(long afterSeq, int max);

    /** Visit every record in seq order from the beginning (recovery path). */
    void replay(java.util.function.Consumer<EventRecord> visitor);
}
