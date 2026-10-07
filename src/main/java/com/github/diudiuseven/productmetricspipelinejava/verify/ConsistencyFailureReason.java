package com.github.diudiuseven.productmetricspipelinejava.verify;

/**
 * Machine-readable, mutually distinguishable failure reasons surfaced when a
 * consistency check fails or a decision basis cannot be determined.
 */
public enum ConsistencyFailureReason {
    /** Incremental/recompute result diverges from a full replay of the same input. */
    CONSISTENCY_MISMATCH,
    /** No committed input exists, so the decision basis is undetermined. */
    DECISION_BASIS_UNDETERMINED,
    /** Verification or evaluation exceeded a configured resource bound. */
    BOUNDS_EXCEEDED
}
