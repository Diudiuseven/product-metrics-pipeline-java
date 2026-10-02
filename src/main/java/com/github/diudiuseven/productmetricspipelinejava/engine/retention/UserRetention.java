package com.github.diudiuseven.productmetricspipelinejava.engine.retention;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;

import java.util.List;

/**
 * One user's retention classification.
 *
 * @param cohortStart bucket floor of the anchor eventTime
 * @param anchor      the first (earliest eventTime, tie -> seq) anchor event
 * @param retained    per configured period: true if a retained event lies in
 *                    {@code [anchor + periodStart, anchor + nextPeriodStart)}
 *                    (for the last period the upper bound is open-ended)
 */
public record UserRetention(long cohortStart, UserEvent anchor, List<Boolean> retained) {
}
