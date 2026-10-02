package com.github.diudiuseven.productmetricspipelinejava.compute;

import java.util.Arrays;

/**
 * Retention counts for one (cohort, dimension tuple).
 */
public record RetentionAggregate(
        String cohort,
        String dims,
        long cohortSize,
        long[] retained
) {
    public RetentionAggregate {
        retained = retained == null ? new long[0] : retained.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetentionAggregate other)) return false;
        return cohortSize == other.cohortSize && cohort.equals(other.cohort)
                && dims.equals(other.dims) && Arrays.equals(retained, other.retained);
    }

    @Override
    public int hashCode() {
        int h = cohort.hashCode() * 31 + dims.hashCode();
        h = (int) (h * 31 + cohortSize);
        return h * 31 + Arrays.hashCode(retained);
    }
}
