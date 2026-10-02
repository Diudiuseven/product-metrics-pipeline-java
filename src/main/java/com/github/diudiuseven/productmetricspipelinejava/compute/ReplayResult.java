package com.github.diudiuseven.productmetricspipelinejava.compute;

import java.util.Map;

/**
 * Output of a full replay: per-(bucket/cohort, dims) aggregates plus the
 * per-user contributions they were derived from.
 */
public record ReplayResult(
        Map<String, FunnelAggregate> funnel,
        Map<String, RetentionAggregate> retention,
        Map<String, FunnelContribution> funnelUsers,
        Map<String, RetentionContribution> retentionUsers
) {
    public ReplayResult {
        funnel = funnel == null ? Map.of() : Map.copyOf(funnel);
        retention = retention == null ? Map.of() : Map.copyOf(retention);
        funnelUsers = funnelUsers == null ? Map.of() : Map.copyOf(funnelUsers);
        retentionUsers = retentionUsers == null ? Map.of() : Map.copyOf(retentionUsers);
    }
}
