package com.github.diudiuseven.productmetricspipelinejava.store;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Stores immutable per-metric snapshot files and the atomic pointer
 * ("checkpoint") to the current one.
 *
 * <p>Snapshots are NDJSON: the first line is a header object, every following
 * line is one self-contained per-user object. This lets the query side stream
 * a committed snapshot with a small memory footprint instead of loading a whole
 * metric into the heap.
 *
 * <p>A pointer is published only after the snapshot file is durable; a temp
 * snapshot without a pointer is garbage on recovery — that is what makes
 * committed results and unfinished work distinguishable after restart.
 */
public interface SnapshotStore {

    /**
     * Atomically publish snapshot + checkpoint. {@code writer} receives a
     * fresh output stream and must produce the whole NDJSON snapshot; the
     * pointer is switched only after a durable fsync.
     */
    void commit(String metricId, int version, long commitSeq, Consumer<OutputStream> writer) throws IOException;

    /** Current committed pointer, if any. */
    Optional<Checkpoint> current(String metricId, int version);

    /** Absolute path of a committed checkpoint's snapshot (immutable file). */
    Path resolve(Checkpoint checkpoint);
}
