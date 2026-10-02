package com.github.diudiuseven.productmetricspipelinejava.query;

/** Time-ranged paged query against one committed metric version. */
public record QueryRequest(String metricId,
                           Integer version,
                           long fromEventTime,
                           long toEventTime,
                           String cursor,
                           int pageSize,
                           long deadlineNanoTime) {
}
