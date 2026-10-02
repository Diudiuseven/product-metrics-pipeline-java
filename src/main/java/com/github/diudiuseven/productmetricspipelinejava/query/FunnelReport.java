package com.github.diudiuseven.productmetricspipelinejava.query;

import java.util.List;

/** Aggregate funnel row for one dimension tuple. */
public record FunnelReport(List<String> dimensionValues,
                           long entered,
                           long converted,
                           List<Long> reachedCounts,
                           long droppedGroupsCollapsed) {
}
