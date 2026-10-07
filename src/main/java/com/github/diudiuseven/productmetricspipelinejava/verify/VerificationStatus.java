package com.github.diudiuseven.productmetricspipelinejava.verify;

/**
 * Outcome of one consistency verification run (published snapshot vs. a from-
 * scratch replay of the same committed input prefix).
 */
public enum VerificationStatus {
    /** Published snapshot is row-identical to the full replay. */
    VERIFIED,
    /** Differences found; gate blocks further incremental publishes. */
    MISMATCH,
    /** Input exceeds configured bounds; verification did not run. */
    SKIPPED_BOUNDS,
    /** No committed input exists yet, so no decision basis can be determined. */
    NO_COMMITTED_SNAPSHOT
}
