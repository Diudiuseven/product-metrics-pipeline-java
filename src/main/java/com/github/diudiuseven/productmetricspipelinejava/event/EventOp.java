package com.github.diudiuseven.productmetricspipelinejava.event;

/**
 * Log record operation. The log is append-only: corrections and deletes are
 * new records; the latest record per event id wins.
 */
public enum EventOp {
    /** New event (idempotent: identical re-PUT is a no-op). */
    PUT,
    /** Correction: same event id, changed payload. */
    CORRECT,
    /** Deletion tombstone: the event no longer exists. */
    DELETE
}
