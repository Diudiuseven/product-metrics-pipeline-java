package com.github.diudiuseven.productmetricspipelinejava.engine.funnel;

import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure, deterministic funnel classification.
 *
 * <p>Rules (see docs/SEMANTICS.md for the prose version):
 * <ol>
 *   <li>events are considered strictly in total order {@code (eventTime, seq)};
 *       an event simultaneous with the previous one but inserted earlier never
 *       counts as a later step;</li>
 *   <li>an anchor is every step-0 event whose eventTime lies in the query
 *       range {@code [fromEventTime, toEventTime]};</li>
 *   <li>from an anchor, each next step is the earliest following event with the
 *       right name, satisfying BOTH
 *       {@code t <= anchor.t + totalWindowMillis} and
 *       {@code t <= previousStep.t + stepWindowMillis}, and {@code t <= to};</li>
 *   <li>out-of-order, skipped or missing steps therefore produce DROPPED with
 *       the exact reached step, never a false conversion;</li>
 *   <li>among all anchor attempts the journey reaching the furthest step wins;
 *       ties are broken by the earliest anchor {@code (eventTime, seq)} — so
 *       the result is a reproducible function of the event set.</li>
 * </ol>
 */
public final class FunnelClassifier {

    private FunnelClassifier() {
    }

    public static Journey classify(MetricDefinition def, List<UserEvent> events,
                                   long fromEventTime, long toEventTime) {
        List<FunnelStep> steps = def.steps();
        // Events arrive stored sorted; keep the guarantee local too.
        List<UserEvent> sorted = new ArrayList<>(events);
        sorted.sort(null);

        Journey best = null;
        for (UserEvent anchor : sorted) {
            if (!anchor.name().equals(steps.get(0).eventName())) {
                continue;
            }
            if (anchor.eventTime() < fromEventTime || anchor.eventTime() > toEventTime) {
                continue;
            }
            Journey j = walk(def, sorted, anchor, toEventTime);
            if (best == null || better(j, best)) {
                best = j;
            }
        }
        if (best == null) {
            return new Journey(FunnelStatus.NOT_STARTED, 0, null, List.of());
        }
        return best;
    }

    private static Journey walk(MetricDefinition def, List<UserEvent> sorted, UserEvent anchor, long to) {
        List<FunnelStep> steps = def.steps();
        List<Long> times = new ArrayList<>();
        times.add(anchor.eventTime());
        UserEvent current = anchor;
        int reached = 0;
        for (int i = 1; i < steps.size(); i++) {
            UserEvent next = findNext(sorted, current, steps.get(i).eventName(),
                    anchor.eventTime() + def.totalWindowMillis(),
                    current.eventTime() + def.stepWindowMillis(),
                    to);
            if (next == null) {
                break;
            }
            times.add(next.eventTime());
            current = next;
            reached = i;
        }
        FunnelStatus status = reached == steps.size() - 1 ? FunnelStatus.CONVERTED : FunnelStatus.DROPPED;
        return new Journey(status, reached, anchor, times);
    }

    private static UserEvent findNext(List<UserEvent> sorted, UserEvent after, String name,
                                      long totalDeadline, long stepDeadline, long queryEnd) {
        UserEvent candidate = null;
        for (UserEvent e : sorted) {
            if (!e.name().equals(name)) {
                continue;
            }
            if (!e.after(after)) {
                continue;
            }
            if (e.eventTime() > totalDeadline || e.eventTime() > stepDeadline || e.eventTime() > queryEnd) {
                continue;
            }
            candidate = e; // sorted ascending => first match is earliest
            break;
        }
        return candidate;
    }

    private static boolean better(Journey a, Journey b) {
        if (a.reachedStep() != b.reachedStep()) {
            return a.reachedStep() > b.reachedStep();
        }
        if (a.anchor() == null || b.anchor() == null) {
            return false;
        }
        int c = Long.compare(a.anchor().eventTime(), b.anchor().eventTime());
        if (c != 0) {
            return c < 0;
        }
        return Long.compare(a.anchor().seq(), b.anchor().seq()) < 0;
    }
}
