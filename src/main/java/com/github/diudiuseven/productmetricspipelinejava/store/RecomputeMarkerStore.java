package com.github.diudiuseven.productmetricspipelinejava.store;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for <em>unfinished</em> full-recomputations only.
 *
 * <p>A marker's mere presence means "recompute in progress, result not yet
 * committed"; it is created before building and deleted after the new snapshot
 * pointer is published. On restart, markers and committed checkpoints are
 * therefore strictly distinguishable: a marker never references a published
 * result, and committed work never depends on a marker.
 */
public interface RecomputeMarkerStore {

    /** Durably record the start of a recomputation (idempotent). */
    void begin(RecomputeMarker marker);

    /** Update the phase / target while running. */
    void update(RecomputeMarker marker);

    /** Remove the marker once the recomputed snapshot is committed. */
    void complete(String metricId, int version);

    Optional<RecomputeMarker> find(String metricId, int version);

    /** Every still-unfinished recomputation (recovery scan). */
    List<RecomputeMarker> allOpen();
}
