package com.github.diudiuseven.productmetricspipelinejava.compute;

/**
 * A single user's current contribution to a retention snapshot.
 *
 * @param cohort time bucket of the user's FIRST anchor event
 * @param dims   dimension tuple of the anchor event
 * @param mask   bit i set if retained for period i
 */
public record RetentionContribution(
        String cohort,
        String dims,
        int mask
) {
}
