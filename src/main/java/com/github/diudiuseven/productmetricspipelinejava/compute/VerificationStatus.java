package com.github.diudiuseven.productmetricspipelinejava.compute;

/**
 * Outcome of a consistency check between the incrementally maintained
 * snapshot and a full replay over the same committed input prefix.
 */
public enum VerificationStatus {
    /** Incremental state is row-identical to a full replay. */
    VERIFIED,
    /** Row-level differences found; the new result was NOT published.
     *  The report carries per-user / per-group diffs. */
    MISMATCH,
    /** Input exceeds pipeline.consistency-max-events; check skipped to keep
     *  verification cost bounded. */
    SKIPPED_BOUNDS,
    /** The decision horizon (time basis) could not be established for at
     *  least one user, so no trustworthy verdict can be derived. */
    HORIZON_UNDETERMINED
}
