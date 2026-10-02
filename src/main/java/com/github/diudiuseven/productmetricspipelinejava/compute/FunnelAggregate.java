package com.github.diudiuseven.productmetricspipelinejava.compute;

import java.util.Arrays;

/**
 * Funnel counts for one (bucket, dimension tuple).
 */
public record FunnelAggregate(
        String bucket,
        String dims,
        long entered,
        long converted,
        long skipped,
        long missing,
        long outOfWindow,
        long pending,
        long[] stepCounts
) {
    public FunnelAggregate {
        stepCounts = stepCounts == null ? new long[0] : stepCounts.clone();
    }

    public long verdictCount(FunnelVerdict v) {
        return switch (v) {
            case CONVERTED -> converted;
            case SKIPPED_STEP -> skipped;
            case OUT_OF_WINDOW -> outOfWindow;
            case MISSING -> missing;
            case PENDING -> pending;
            case ENTERED -> entered;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FunnelAggregate other)) return false;
        return entered == other.entered && converted == other.converted
                && skipped == other.skipped && missing == other.missing
                && outOfWindow == other.outOfWindow && pending == other.pending
                && bucket.equals(other.bucket) && dims.equals(other.dims)
                && Arrays.equals(stepCounts, other.stepCounts);
    }

    @Override
    public int hashCode() {
        int h = bucket.hashCode() * 31 + dims.hashCode();
        h = (int) (h * 31 + entered);
        return h * 31 + Arrays.hashCode(stepCounts);
    }
}
