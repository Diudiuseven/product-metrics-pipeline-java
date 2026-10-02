package com.github.diudiuseven.productmetricspipelinejava.query;

import java.util.List;

/** One retention row: cohort (anchor bucket + dimensions) and per-period counts. */
public record RetentionReport(long cohortStart,
                              List<String> dimensionValues,
                              long cohortSize,
                              List<Long> retainedCounts,
                              long droppedGroupsCollapsed) {
}
