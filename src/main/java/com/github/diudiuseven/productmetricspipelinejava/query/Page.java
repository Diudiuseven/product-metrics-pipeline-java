package com.github.diudiuseven.productmetricspipelinejava.query;

import java.util.List;

/**
 * One immutable page of a time-ranged, batch query. {@code nextFrom} is the
 * opaque cursor (null when exhausted). A query that times out or is cancelled
 * returns an error rather than this half-built object.
 */
public record Page<T>(List<T> rows, String nextFrom, boolean complete) {
}
