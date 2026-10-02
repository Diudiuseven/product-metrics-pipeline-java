package com.github.diudiuseven.productmetricspipelinejava.query;

import java.util.List;

/** One user's funnel classification within a query window. */
public record FunnelUserResult(String userId,
                               FunnelStatus status,
                               int reachedStep,
                               List<String> dimensionValues,
                               List<Long> stepTimes) {
}
