package com.github.diudiuseven.productmetricspipelinejava.store;

/**
 * Durable progress marker of one aggregator.
 * {@code commitSeq} means every event with seq &lt;= commitSeq is reflected;
 * {@code snapshotPath} names the immutable snapshot file holding that state.
 */
public record Checkpoint(String metricId, int version, long commitSeq, String snapshotPath) {
}
