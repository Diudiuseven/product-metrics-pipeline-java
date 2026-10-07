package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.event.StoredEvent;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.metric.RetentionSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.StepDefinition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deterministic per-user state machine. Every result is derived from the event
 * log alone, so incremental results are always row-identical to a full replay.
 *
 * <p>Funnel semantics: a user enters on the first step-1 event; later steps must
 * follow in order within the window. Verdicts are mutually exclusive:
 * CONVERTED (all steps in order, in window) > SKIPPED_STEP (jumped ahead) >
 * OUT_OF_WINDOW (in order but late) > MISSING (window expired, unfinished) >
 * PENDING (window still open) > ENTERED.
 *
 * <p>Time basis: whether a window is "expired" is judged against the
 * <em>decision horizon</em> — the max event_time of the user's committed
 * events — never the wall clock. Verdicts are therefore reproducible for the
 * same committed input and cannot drift while no new events arrive.
 *
 * <p>Retention semantics: anchor = the user's FIRST anchor event ever; cohort =
 * bucket of that anchor; retained for period i if a return event lands in
 * [anchor + period, anchor + period + returnWindow).
 */
@Component
public class ReplayEngine {

    public static final String DIMS_ALL = "__all__";
    private static final DateTimeFormatter DAY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH").withZone(ZoneOffset.UTC);

    public ReplayResult replay(MetricDefinition def, List<StoredEvent> events) {
        Map<String, List<StoredEvent>> byUser = new LinkedHashMap<>();
        for (StoredEvent se : events) {
            byUser.computeIfAbsent(se.event().userId(), k -> new ArrayList<>()).add(se);
        }
        Map<String, FunnelAggregate> funnelAgg = new LinkedHashMap<>();
        Map<String, RetentionAggregate> retentionAgg = new LinkedHashMap<>();
        Map<String, FunnelContribution> funnelUsers = new LinkedHashMap<>();
        Map<String, RetentionContribution> retentionUsers = new LinkedHashMap<>();
        for (Map.Entry<String, List<StoredEvent>> e : byUser.entrySet()) {
            String userId = e.getKey();
            List<StoredEvent> userEvents = e.getValue();
            if (def.getType() == com.github.diudiuseven.productmetricspipelinejava.metric.MetricType.FUNNEL) {
                FunnelContribution c = evaluateFunnel(def, userEvents);
                if (c != null) {
                    funnelUsers.put(userId, c);
                    accumulateFunnel(funnelAgg, c, def.getFunnel().steps().size());
                }
            } else {
                RetentionContribution c = evaluateRetention(def, userEvents);
                if (c != null) {
                    retentionUsers.put(userId, c);
                    accumulateRetention(retentionAgg, c, def.getRetention().periods().size());
                }
            }
        }
        return new ReplayResult(funnelAgg, retentionAgg, funnelUsers, retentionUsers);
    }

    /** Re-aggregates funnel contributions into aggregates (used after dimension capping). */
    public static Map<String, FunnelAggregate> aggregateFunnel(Map<String, FunnelContribution> users, int steps) {
        Map<String, FunnelAggregate> agg = new LinkedHashMap<>();
        for (FunnelContribution c : users.values()) {
            accumulateFunnel(agg, c, steps);
        }
        return agg;
    }

    /** Re-aggregates retention contributions into aggregates (used after dimension capping). */
    public static Map<String, RetentionAggregate> aggregateRetention(Map<String, RetentionContribution> users, int periods) {
        Map<String, RetentionAggregate> agg = new LinkedHashMap<>();
        for (RetentionContribution c : users.values()) {
            accumulateRetention(agg, c, periods);
        }
        return agg;
    }

    public FunnelContribution evaluateFunnel(MetricDefinition def, List<StoredEvent> userEvents) {
        List<StoredEvent> sorted = sortedByTime(userEvents);
        FunnelSpec spec = def.getFunnel();
        List<StepDefinition> steps = spec.steps();
        int n = steps.size();
        Duration window = spec.window();
        boolean fromFirst = spec.windowFrom() == FunnelSpec.WindowFrom.FIRST_STEP;
        String gran = spec.bucketGranularity() == null ? "DAY" : spec.bucketGranularity();

        int bestMask = 0;
        FunnelVerdict bestVerdict = FunnelVerdict.ENTERED;
        String bucket = null;
        String dims = null;

        int curStep = 0;          // next required step index; 0 = no open attempt
        Instant start = null;
        Instant prevTime = null;
        int attemptMask = 0;
        FunnelVerdict attemptVerdict = null;
        StoredEvent entryEvent = null;

        for (StoredEvent se : sorted) {
            if (curStep == 0) {
                if (matchesStep(se, steps.get(0))) {
                    entryEvent = se;
                    curStep = 1;
                    start = prevTime = se.event().eventTime();
                    attemptMask = 1;
                    attemptVerdict = null;
                    if (n == 1) {
                        attemptVerdict = FunnelVerdict.CONVERTED;
                        bestMask = attemptMask;
                        bestVerdict = FunnelVerdict.CONVERTED;
                        curStep = 0;
                    }
                }
                continue;
            }
            // open attempt: a new step-1 event restarts the funnel
            if (matchesStep(se, steps.get(0))) {
                if (attemptVerdict == null) {
                    attemptVerdict = se.event().eventTime().isAfter(start.plus(window))
                            ? FunnelVerdict.MISSING : null; // null = superseded
                }
                curStep = 1;
                start = prevTime = se.event().eventTime();
                attemptMask = 1;
                attemptVerdict = null;
                continue;
            }
            int matchIdx = -1;
            for (int k = curStep; k < n; k++) {
                if (matchesStep(se, steps.get(k))) {
                    matchIdx = k;
                    break;
                }
            }
            if (matchIdx == -1) {
                continue; // event matches no funnel step: ignore
            }
            if (matchIdx == curStep) {
                Instant base = fromFirst ? start : prevTime;
                if (!se.event().eventTime().isAfter(base.plus(window))) {
                    attemptMask |= (1 << matchIdx);
                    prevTime = se.event().eventTime();
                    curStep = matchIdx + 1;
                    if (curStep == n) {
                        attemptVerdict = FunnelVerdict.CONVERTED;
                    }
                } else {
                    attemptVerdict = FunnelVerdict.OUT_OF_WINDOW;
                }
            } else {
                attemptVerdict = FunnelVerdict.SKIPPED_STEP;
            }
            if (attemptVerdict != null) {
                bestMask |= attemptMask;
                if (attemptVerdict == FunnelVerdict.CONVERTED) {
                    bestVerdict = FunnelVerdict.CONVERTED;
                } else if (verdictRank(attemptVerdict) > verdictRank(bestVerdict)) {
                    bestVerdict = attemptVerdict;
                }
                attemptMask = 0;
                attemptVerdict = null;
                curStep = 0;
            }
        }
        if (curStep > 0 && attemptVerdict == null) {
            // Decision horizon: the max event_time of this user's committed
            // events — a pure function of the committed input, never of the
            // wall clock. An open attempt flips to MISSING only once the
            // user's own committed timeline provably passed the window.
            Instant horizon = decisionHorizon(sorted);
            attemptVerdict = horizon.isAfter(start.plus(window))
                    ? FunnelVerdict.MISSING : FunnelVerdict.PENDING;
            bestMask |= attemptMask;
            if (verdictRank(attemptVerdict) > verdictRank(bestVerdict)) {
                bestVerdict = attemptVerdict;
            }
        }
        if (entryEvent == null) {
            return null;
        }
        bucket = bucketOf(entryEvent.event().eventTime(), gran);
        dims = dimsTuple(entryEvent.event().dims(), spec.dimensions());
        return new FunnelContribution(bucket, dims, bestMask, bestVerdict);
    }

    public RetentionContribution evaluateRetention(MetricDefinition def, List<StoredEvent> userEvents) {
        List<StoredEvent> sorted = sortedByTime(userEvents);
        RetentionSpec spec = def.getRetention();
        StoredEvent anchorEvent = null;
        for (StoredEvent se : sorted) {
            if (matchesEventType(se, spec.anchorEventType())) {
                anchorEvent = se;
                break;
            }
        }
        if (anchorEvent == null) {
            return null;
        }
        Instant anchor = anchorEvent.event().eventTime();
        String gran = spec.cohortGranularity() == null ? "DAY" : spec.cohortGranularity();
        Duration window = spec.returnWindow() != null ? spec.returnWindow()
                : (gran.equals("HOUR") ? Duration.ofHours(1) : Duration.ofDays(1));
        int mask = 0;
        for (int i = 0; i < spec.periods().size(); i++) {
            Instant from = anchor.plus(spec.periods().get(i));
            Instant to = from.plus(window);
            for (StoredEvent se : sorted) {
                if (matchesEventType(se, spec.returnEventType())
                        && !se.event().eventTime().isBefore(from)
                        && se.event().eventTime().isBefore(to)) {
                    mask |= (1 << i);
                    break;
                }
            }
        }
        return new RetentionContribution(
                bucketOf(anchor, gran),
                dimsTuple(anchorEvent.event().dims(), spec.dimensions()),
                mask);
    }

    // --------------------------- helpers ---------------------------

    private static List<StoredEvent> sortedByTime(List<StoredEvent> events) {
        List<StoredEvent> sorted = new ArrayList<>(events);
        sorted.sort((a, b) -> {
            int c = a.event().eventTime().compareTo(b.event().eventTime());
            return c != 0 ? c : Long.compare(a.logSeq(), b.logSeq());
        });
        return sorted;
    }

    /**
     * The deterministic time basis for open-attempt verdicts: the maximum
     * event_time among the user's committed events. It advances only when new
     * input for this user is committed, so committed verdicts never drift
     * while no new events arrive.
     */
    public static Instant decisionHorizon(List<StoredEvent> userEvents) {
        Instant horizon = null;
        for (StoredEvent se : userEvents) {
            Instant t = se.event().eventTime();
            if (horizon == null || t.isAfter(horizon)) {
                horizon = t;
            }
        }
        return horizon;
    }

    private static void accumulateFunnel(Map<String, FunnelAggregate> agg, FunnelContribution c, int steps) {
        String key = c.bucket() + "\u0000" + c.dims();
        FunnelAggregate prev = agg.get(key);
        long[] stepCounts = prev == null ? new long[steps] : prev.stepCounts().clone();
        for (int k = 0; k < steps; k++) {
            if ((c.stepMask() & (1 << k)) != 0) {
                stepCounts[k]++;
            }
        }
        long entered = (prev == null ? 0 : prev.entered()) + 1;
        long converted = (prev == null ? 0 : prev.converted()) + (c.verdict() == FunnelVerdict.CONVERTED ? 1 : 0);
        long skipped = (prev == null ? 0 : prev.skipped()) + (c.verdict() == FunnelVerdict.SKIPPED_STEP ? 1 : 0);
        long missing = (prev == null ? 0 : prev.missing()) + (c.verdict() == FunnelVerdict.MISSING ? 1 : 0);
        long oow = (prev == null ? 0 : prev.outOfWindow()) + (c.verdict() == FunnelVerdict.OUT_OF_WINDOW ? 1 : 0);
        long pending = (prev == null ? 0 : prev.pending()) + (c.verdict() == FunnelVerdict.PENDING ? 1 : 0);
        agg.put(key, new FunnelAggregate(c.bucket(), c.dims(), entered, converted, skipped, missing, oow, pending, stepCounts));
    }

    private static void accumulateRetention(Map<String, RetentionAggregate> agg, RetentionContribution c, int periods) {
        String key = c.cohort() + "\u0000" + c.dims();
        RetentionAggregate prev = agg.get(key);
        long[] retained = prev == null ? new long[periods] : prev.retained().clone();
        for (int i = 0; i < periods; i++) {
            if ((c.mask() & (1 << i)) != 0) {
                retained[i]++;
            }
        }
        long size = (prev == null ? 0 : prev.cohortSize()) + 1;
        agg.put(key, new RetentionAggregate(c.cohort(), c.dims(), size, retained));
    }

    public static String bucketOf(Instant time, String granularity) {
        return "HOUR".equals(granularity) ? HOUR_FMT.format(time) : DAY_FMT.format(time);
    }

    public static String dimsTuple(Map<String, String> eventDims, List<String> dimNames) {
        if (dimNames == null || dimNames.isEmpty()) {
            return DIMS_ALL;
        }
        StringBuilder sb = new StringBuilder();
        for (String name : dimNames) {
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(name).append('=').append(eventDims == null ? null : eventDims.getOrDefault(name, "__none__"));
        }
        return sb.toString();
    }

    public static boolean matchesStep(StoredEvent se, StepDefinition step) {
        if (!matchesEventType(se, step.eventType())) {
            return false;
        }
        Map<String, String> filters = step.dimFilters();
        if (filters == null || filters.isEmpty()) {
            return true;
        }
        Map<String, String> dims = se.event().dims();
        for (Map.Entry<String, String> f : filters.entrySet()) {
            if (dims == null || !f.getValue().equals(dims.get(f.getKey()))) {
                return false;
            }
        }
        return true;
    }

    public static boolean matchesEventType(StoredEvent se, String eventType) {
        return eventType != null && eventType.equals(se.event().eventType());
    }

    private static int verdictRank(FunnelVerdict v) {
        return switch (v) {
            case CONVERTED -> 6;
            case SKIPPED_STEP -> 5;
            case OUT_OF_WINDOW -> 4;
            case MISSING -> 3;
            case PENDING -> 2;
            case ENTERED -> 1;
        };
    }

    public static final String OVERFLOW_DIMS = "__overflow__";

    /**
     * Caps the number of distinct dimension tuples. The selection is sticky
     * and deterministic: tuples already materialised as detail groups
     * ({@code existingDims}) are always kept — a recompute never rewrites a
     * stable group — and remaining slots are filled with new tuples in
     * lexicographic order, so the incremental path and the full-recompute
     * path always select the same groups for the same committed input.
     * The overflow pseudo-tuple never consumes a detail slot.
     */
    public static Map<String, FunnelContribution> capFunnelDims(Map<String, FunnelContribution> users,
                                                                Set<String> existingDims, int max) {
        if (max <= 0 || users.isEmpty()) {
            return users;
        }
        Set<String> keep = new java.util.TreeSet<>();
        for (String d : existingDims) {
            if (!OVERFLOW_DIMS.equals(d) && keep.size() < max) {
                keep.add(d);
            }
        }
        List<String> newDims = users.values().stream().map(FunnelContribution::dims).distinct()
                .filter(d -> !keep.contains(d) && !OVERFLOW_DIMS.equals(d)).sorted().toList();
        for (String d : newDims) {
            if (keep.size() >= max) {
                break;
            }
            keep.add(d);
        }
        Map<String, FunnelContribution> result = new LinkedHashMap<>();
        users.forEach((k, v) -> result.put(k, keep.contains(v.dims()) ? v
                : new FunnelContribution(v.bucket(), OVERFLOW_DIMS, v.stepMask(), v.verdict())));
        return result;
    }

    public static Map<String, RetentionContribution> capRetentionDims(Map<String, RetentionContribution> users,
                                                                     Set<String> existingDims, int max) {
        if (max <= 0 || users.isEmpty()) {
            return users;
        }
        Set<String> keep = new java.util.TreeSet<>();
        for (String d : existingDims) {
            if (!OVERFLOW_DIMS.equals(d) && keep.size() < max) {
                keep.add(d);
            }
        }
        List<String> newDims = users.values().stream().map(RetentionContribution::dims).distinct()
                .filter(d -> !keep.contains(d) && !OVERFLOW_DIMS.equals(d)).sorted().toList();
        for (String d : newDims) {
            if (keep.size() >= max) {
                break;
            }
            keep.add(d);
        }
        Map<String, RetentionContribution> result = new LinkedHashMap<>();
        users.forEach((k, v) -> result.put(k, keep.contains(v.dims()) ? v
                : new RetentionContribution(v.cohort(), OVERFLOW_DIMS, v.mask())));
        return result;
    }

}
