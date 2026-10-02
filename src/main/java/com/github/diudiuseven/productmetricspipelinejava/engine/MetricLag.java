package com.github.diudiuseven.productmetricspipelinejava.engine;

/** Observability row: how far one aggregator is behind the durable log tip. */
public record MetricLag(String metricId, int version, long committedSeq, long latestSeq, long lag) {
}
