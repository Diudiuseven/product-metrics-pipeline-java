package com.github.diudiuseven.productmetricspipelinejava.query;

/**
 * Distinguishable funnel outcomes per user journey:
 * CONVERTED — every step occurred in order within the configured windows;
 * DROPPED — the journey entered the funnel but a later step never followed in
 *           time (the last reached step is carried separately);
 * NOT_STARTED — no qualifying step-0 anchor in the query range.
 */
public enum FunnelStatus {
    CONVERTED,
    DROPPED,
    NOT_STARTED
}
