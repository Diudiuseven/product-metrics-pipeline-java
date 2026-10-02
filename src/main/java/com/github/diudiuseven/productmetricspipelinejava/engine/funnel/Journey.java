package com.github.diudiuseven.productmetricspipelinejava.engine.funnel;

import com.github.diudiuseven.productmetricspipelinejava.query.FunnelStatus;

import java.util.List;

/**
 * Deterministic classification of one user against a funnel over a fixed event
 * list and query range.
 *
 * @param status       CONVERTED / DROPPED / NOT_STARTED
 * @param reachedStep  highest step index reached (0 for a bare anchor)
 * @param anchor       the step-0 event of the chosen journey
 * @param stepTimes    eventTime chosen for each reached step (size reachedStep+1)
 */
public record Journey(FunnelStatus status, int reachedStep, UserEvent anchor, List<Long> stepTimes) {

    public boolean converted() {
        return status == FunnelStatus.CONVERTED;
    }
}
