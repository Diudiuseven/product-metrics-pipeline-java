package com.github.diudiuseven.productmetricspipelinejava.query;

import com.github.diudiuseven.productmetricspipelinejava.engine.CommittedView;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelClassifier;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.Journey;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionClassifier;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.UserRetention;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.Checkpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Read side. Every query runs against one immutable committed snapshot
 * ({@link CommittedView}), scans NDJSON rows sequentially with a bounded
 * buffer, and enforces its deadline <em>before</em> returning a page — so
 * timeouts/cancellations surface as errors, never half pages, and never touch
 * aggregation state.
 *
 * <h2>Cost</h2>
 * User-level pages cost one sequential scan resumed by ordinal cursor
 * ({@code O(rowsExamined)}). Grouped report pages cost at most two sequential
 * scans of the same snapshot ({@code O(N log K)} heap work, K = page/group
 * bound). Peak heap is {@code O(pageSize)} plus {@code O(maxDimensionGroups)}
 * for group-key selection — independent of total users.
 */
public class QueryService {

    /** Sentinel tuple for groups collapsed by the cardinality bound. */
    public static final List<String> OTHER_GROUP = List.of("__OTHER__");

    private final PipelineOrchestrator orchestrator;

    public QueryService(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    // ---- Funnel: per-user detail ----------------------------------------

    public Page<FunnelUserResult> funnelUsers(QueryRequest req) {
        Context ctx = open(req, true);
        int limit = req.pageSize();
        List<FunnelUserResult> rows = new ArrayList<>(limit + 1);
        long[] nextOrdinal = {ctx.startOrdinal};
        boolean[] hasMore = {false};

        ctx.reader.scanFrom(ctx.startOrdinal,
                u -> hasAnchorInRange(ctx.def, u, req.fromEventTime(), req.toEventTime()),
                (u, ordinal) -> {
                    checkDeadline(req);
                    Journey j = FunnelClassifier.classify(ctx.def, u.events(),
                            req.fromEventTime(), req.toEventTime());
                    List<String> dims = j.anchor() == null
                            ? List.of()
                            : j.anchor().dims(ctx.def.dimensionKeys());
                    rows.add(new FunnelUserResult(u.userId(), j.status(), j.reachedStep(), dims, j.stepTimes()));
                    if (rows.size() <= limit) {
                        // Cursor points just past the last *returned* row; the
                        // single probe row (rows[limit]) only signals hasMore.
                        nextOrdinal[0] = ordinal + 1;
                    } else {
                        hasMore[0] = true;
                    }
                    return rows.size() <= limit; // fetch exactly one probe extra
                },
                () -> checkDeadline(req));
        if (rows.size() > limit) {
            rows.remove(rows.size() - 1);
        }
        String cursor = hasMore[0] ? String.valueOf(nextOrdinal[0]) : null;
        return new Page<>(rows, cursor, cursor == null);
    }

    // ---- Funnel: grouped report -----------------------------------------

    public Page<FunnelReport> funnelReport(QueryRequest req) {
        Context ctx = open(req, true);
        int page = pageIndex(req.cursor());
        int size = req.pageSize();

        // Pass 1: bounded selection of which dimension keys exist (K smallest
        // tuples); everything else collapses into __OTHER__.
        int cap = ctx.def.maxDimensionGroups();
        PriorityQueue<List<String>> heap = new PriorityQueue<>(QueryService::compareTupleReversed);
        java.util.concurrent.atomic.AtomicBoolean over = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicLong totalEligible = new java.util.concurrent.atomic.AtomicLong();
        ctx.reader.scanFrom(0,
                u -> hasAnchorInRange(ctx.def, u, req.fromEventTime(), req.toEventTime()),
                (u, ordinal) -> {
                    checkDeadline(req);
                    totalEligible.incrementAndGet();
                    Journey j = FunnelClassifier.classify(ctx.def, u.events(),
                            req.fromEventTime(), req.toEventTime());
                    List<String> key = j.anchor() == null
                            ? List.of()
                            : j.anchor().dims(ctx.def.dimensionKeys());
                    if (upsertSmallest(heap, key, cap)) {
                        over.set(true);
                    }
                    return true;
                },
                () -> checkDeadline(req));

        List<List<String>> keys = new ArrayList<>(heap);
        keys.sort(QueryService::compareTuple);
        boolean overflow = over.get(); // only true if a distinct key was actually excluded
        int from = page * size;
        if (from >= keys.size() + (overflow ? 1 : 0)) {
            return new Page<>(List.of(), null, true);
        }
        boolean otherOnThisPage = overflow && from <= keys.size() && from + size > keys.size();
        int sliceTo = Math.min(keys.size(), from + size - (otherOnThisPage ? 1 : 0));
        List<List<String>> slice = keys.subList(Math.min(from, keys.size()), sliceTo);

        // Pass 2: aggregate only groups on this page (+OTHER if in slice).
        Map<List<String>, FunnelAcc> acc = new LinkedHashMap<>();
        for (List<String> k : slice) {
            acc.put(k, new FunnelAcc(ctx.def.steps().size()));
        }
        FunnelAcc other = otherOnThisPage ? new FunnelAcc(ctx.def.steps().size()) : null;
        java.util.Set<List<String>> sliceSet = Set.copyOf(acc.keySet());
        ctx.reader.scanFrom(0,
                u -> hasAnchorInRange(ctx.def, u, req.fromEventTime(), req.toEventTime()),
                (u, ordinal) -> {
                    checkDeadline(req);
                    Journey j = FunnelClassifier.classify(ctx.def, u.events(),
                            req.fromEventTime(), req.toEventTime());
                    List<String> key = j.anchor() == null
                            ? List.of()
                            : j.anchor().dims(ctx.def.dimensionKeys());
                    FunnelAcc a = sliceSet.contains(key) ? acc.get(key) : other;
                    if (a != null) {
                        a.add(j, ctx.def.steps().size());
                    }
                    return true;
                },
                () -> checkDeadline(req));

        List<FunnelReport> rows = new ArrayList<>();
        for (List<String> k : slice) {
            rows.add(acc.get(k).toReport(k, 0));
        }
        if (other != null) {
            rows.add(other.toReport(OTHER_GROUP, totalEligible.get()));
        }
        int totalPages = (keys.size() + (overflow ? 1 : 0) + size - 1) / size;
        boolean complete = page + 1 >= totalPages;
        return new Page<>(rows, complete ? null : String.valueOf(page + 1), complete);
    }

    // ---- Retention -------------------------------------------------------

    public Page<RetentionReport> retention(QueryRequest req) {
        Context ctx = open(req, false);
        int page = pageIndex(req.cursor());
        int size = req.pageSize();

        // Cohort row key = (cohortStart, dimension tuple).
        List<Key> keys = new ArrayList<>();
        int cap = ctx.def.maxDimensionGroups();
        PriorityQueue<Key> heap = new PriorityQueue<>((a, b) -> -compareKey(a, b));
        java.util.concurrent.atomic.AtomicBoolean over = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicLong eligible = new java.util.concurrent.atomic.AtomicLong();
        ctx.reader.scanFrom(0,
                u -> true,
                (u, ordinal) -> {
                    checkDeadline(req);
                    UserRetention ur = RetentionClassifier.classify(ctx.def, u.events());
                    if (ur == null || ur.anchor().eventTime() < req.fromEventTime()
                            || ur.anchor().eventTime() >= req.toEventTime()) {
                        return true;
                    }
                    eligible.incrementAndGet();
                    Key k = new Key(ur.cohortStart(), ur.anchor().dims(ctx.def.dimensionKeys()));
                    if (upsertSmallestKey(heap, k, cap)) {
                        over.set(true);
                    }
                    return true;
                },
                () -> checkDeadline(req));
        keys.addAll(heap);
        keys.sort(QueryService::compareKey);
        boolean overflow = over.get();
        int totalGroups = keys.size() + (overflow ? 1 : 0);
        int from = page * size;
        if (from >= totalGroups) {
            return new Page<>(List.of(), null, true);
        }
        boolean otherOnThisPage = overflow && from <= keys.size() && from + size > keys.size();
        int sliceTo = Math.min(keys.size(), from + size - (otherOnThisPage ? 1 : 0));
        List<Key> slice = new ArrayList<>(keys.subList(Math.min(from, keys.size()), sliceTo));

        Map<Key, RetentionAcc> acc = new LinkedHashMap<>();
        for (Key k : slice) {
            acc.put(k, new RetentionAcc(ctx.def.retentionPeriodsMillis().size()));
        }
        RetentionAcc other = otherOnThisPage ? new RetentionAcc(ctx.def.retentionPeriodsMillis().size()) : null;
        java.util.Set<Key> sliceSet = Set.copyOf(acc.keySet());
        ctx.reader.scanFrom(0, u -> true, (u, ordinal) -> {
            checkDeadline(req);
            UserRetention ur = RetentionClassifier.classify(ctx.def, u.events());
            if (ur == null || ur.anchor().eventTime() < req.fromEventTime()
                    || ur.anchor().eventTime() >= req.toEventTime()) {
                return true;
            }
            Key k = new Key(ur.cohortStart(), ur.anchor().dims(ctx.def.dimensionKeys()));
            RetentionAcc a = sliceSet.contains(k) ? acc.get(k) : other;
            if (a != null) {
                a.add(ur);
            }
            return true;
        }, () -> checkDeadline(req));

        List<RetentionReport> rows = new ArrayList<>();
        for (Key k : slice) {
            rows.add(acc.get(k).toReport(k, 0));
        }
        if (other != null) {
            rows.add(other.toReport(new Key(0, OTHER_GROUP), eligible.get()));
        }
        int totalPages = (totalGroups + size - 1) / size;
        boolean complete = page + 1 >= totalPages;
        return new Page<>(rows, complete ? null : String.valueOf(page + 1), complete);
    }

    // ---- internals -------------------------------------------------------

    private record Context(MetricDefinition def, SnapshotReader reader, long startOrdinal) {
    }

    private Context open(QueryRequest req, boolean expectFunnel) {
        if (req.fromEventTime() >= req.toEventTime()) {
            throw new IllegalArgumentException("query 'from' must be < 'to'");
        }
        int v = req.version() != null ? req.version()
                : orchestrator.definitions().active(req.metricId()).version();
        MetricDefinition def = orchestrator.definitions().get(req.metricId(), v);
        if (expectFunnel && !"FUNNEL".equals(def.type().name())
                || !expectFunnel && !"RETENTION".equals(def.type().name())) {
            throw new IllegalArgumentException("metric " + req.metricId() + "@v" + v + " is the wrong type");
        }
        Optional<Checkpoint> cp = orchestrator.snapshots().current(req.metricId(), v);
        if (cp.isEmpty()) {
            throw new IllegalArgumentException("metric " + req.metricId() + "@v" + v + " has no committed results yet");
        }
        SnapshotReader reader = new SnapshotReader(orchestrator.snapshots(), cp.get());
        long start = req.cursor() == null ? 0L : parseOrdinal(req.cursor());
        return new Context(def, reader, start);
    }

    private static void checkDeadline(QueryRequest req) {
        if (System.nanoTime() > req.deadlineNanoTime()) {
            throw new QueryDeadlineException("query exceeded its deadline; no page returned");
        }
    }

    private static int pageIndex(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        try {
            int p = Integer.parseInt(cursor);
            if (p < 0) {
                throw new IllegalArgumentException("cursor must be >= 0");
            }
            return p;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid pagination cursor");
        }
    }

    private static long parseOrdinal(String cursor) {
        try {
            long v = Long.parseLong(cursor);
            if (v < 0) {
                throw new IllegalArgumentException("cursor must be >= 0");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid pagination cursor");
        }
    }

    private boolean hasAnchorInRange(MetricDefinition def, ParsedUser u, long from, long to) {
        String anchor = def.steps().get(0).eventName();
        for (var e : u.events()) {
            if (e.name().equals(anchor) && e.eventTime() >= from && e.eventTime() <= to) {
                return true;
            }
        }
        return false;
    }

    private static int compareTuple(List<String> a, List<String> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            int c = a.get(i).compareTo(b.get(i));
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    private static int compareTupleReversed(List<String> a, List<String> b) {
        return -compareTuple(a, b);
    }

    /**
     * Keep the K smallest keys seen (max-heap capped at K).
     * @return true exactly when {@code key} is a distinct key that is excluded
     *         from the retained K (either evicted now or, being larger than the
     *         retained maximum, simply outside). Used to detect real overflow.
     */
    private static boolean upsertSmallest(PriorityQueue<List<String>> heap, List<String> key, int cap) {
        if (heap.size() < cap) {
            if (!heap.contains(key)) {
                heap.add(key);
            }
            return false;
        }
        if (heap.contains(key)) {
            return false;
        }
        List<String> top = heap.peek();
        if (top != null && compareTuple(key, top) < 0) {
            heap.poll();
            heap.add(key);
        }
        return true; // a distinct key beyond the retained K exists
    }

    private record Key(long cohort, List<String> dims) {
    }

    private static int compareKey(Key a, Key b) {
        int c = Long.compare(a.cohort, b.cohort);
        if (c != 0) {
            return c;
        }
        return compareTuple(a.dims, b.dims);
    }

    /** @return true when a distinct key beyond the retained K exists. */
    private boolean upsertSmallestKey(PriorityQueue<Key> heap, Key key, int cap) {
        for (Key k : heap) {
            if (compareKey(k, key) == 0) {
                return false;
            }
        }
        if (heap.size() < cap) {
            heap.add(key);
            return false;
        }
        Key top = heap.peek();
        if (top != null && compareKey(key, top) < 0) {
            heap.poll();
            heap.add(key);
        }
        return true;
    }

    // Per-group funnel accumulator.
    private static final class FunnelAcc {
        long entered;
        long converted;
        final long[] reached;

        FunnelAcc(int steps) {
            this.reached = new long[steps];
        }

        void add(Journey j, int steps) {
            entered++;
            if (j.converted()) {
                converted++;
            }
            for (int i = 0; i <= j.reachedStep() && i < steps; i++) {
                reached[i]++;
            }
        }

        FunnelReport toReport(List<String> key, long collapsed) {
            List<Long> counts = new ArrayList<>(reached.length);
            for (long v : reached) {
                counts.add(v);
            }
            return new FunnelReport(key, entered, converted, Collections.unmodifiableList(counts), collapsed);
        }
    }

    // Per-(cohort,group) retention accumulator.
    private static final class RetentionAcc {
        long size;
        final long[] retained;

        RetentionAcc(int periods) {
            this.retained = new long[periods];
        }

        void add(UserRetention ur) {
            size++;
            for (int i = 0; i < retained.length; i++) {
                if (ur.retained().get(i)) {
                    retained[i]++;
                }
            }
        }

        RetentionReport toReport(Key k, long collapsed) {
            List<Long> counts = new ArrayList<>(retained.length);
            for (long v : retained) {
                counts.add(v);
            }
            return new RetentionReport(k.cohort, k.dims, size,
                    Collections.unmodifiableList(counts), collapsed);
        }
    }
}
