package com.github.diudiuseven.productmetricspipelinejava.compute;

/**
 * Per-user funnel verdict. Every entered user ends up in exactly one verdict
 * bucket (mutually exclusive, precedence: CONVERTED > SKIPPED_STEP >
 * OUT_OF_WINDOW > MISSING > PENDING > ENTERED).
 */
public enum FunnelVerdict {
    /** All steps satisfied in order within the window. */
    CONVERTED,
    /** Jumped ahead: a later step appeared before the next required step. */
    SKIPPED_STEP,
    /** Steps in order but the gap exceeded the configured window. */
    OUT_OF_WINDOW,
    /** Entered, window expired, never finished. */
    MISSING,
    /** Inside the window, still open (not yet decided). */
    PENDING,
    /** Reached step 1 only (fallback). */
    ENTERED
}
