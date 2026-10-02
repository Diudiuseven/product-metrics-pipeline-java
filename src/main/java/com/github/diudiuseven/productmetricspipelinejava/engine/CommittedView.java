package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One immutable, fully committed result: snapshot JSON plus the log seq it
 * reflects. Query code sees only these, so a query result is always a single
 * self-consistent version of one metric — never an old/new mix.
 */
public record CommittedView(String metricId, int version, long commitSeq, JsonNode snapshot) {
}
