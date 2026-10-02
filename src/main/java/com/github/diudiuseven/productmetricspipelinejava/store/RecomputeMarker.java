package com.github.diudiuseven.productmetricspipelinejava.store;

/**
 * Persistent state of a background full recomputation. A marker exists only
 * while a recompute is unfinished; on restart its presence lets the pipeline
 * resume/abort deterministically. {@code phase} is BUILDING or COMPARING.
 */
public record RecomputeMarker(String metricId, int version, long targetSeq, String phase) {
    public static final String BUILDING = "BUILDING";
    public static final String COMPARING = "COMPARING";
}
