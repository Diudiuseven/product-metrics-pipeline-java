package com.github.diudiuseven.productmetricspipelinejava.compute;

/**
 * A single user's current contribution to a funnel snapshot.
 *
 * @param bucket   time bucket of the entry (step-1) event, e.g. 2026-10-01
 * @param dims      dimension tuple of the entry event (sorted key=value, or __all__)
 * @param stepMask  bit k set if the user reached step k+1 in order
 * @param verdict   final verdict for the user
 */
public record FunnelContribution(
        String bucket,
        String dims,
        int stepMask,
        FunnelVerdict verdict
) {
}
