package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelAggregator;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionAggregator;
import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.DefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.EventRecord;
import com.github.diudiuseven.productmetricspipelinejava.store.IngestResult;
import com.github.diudiuseven.productmetricspipelinejava.store.RecomputeMarker;
import com.github.diudiuseven.productmetricspipelinejava.store.FileRecomputeMarkerStore;
import com.github.diudiuseven.productmetricspipelinejava.store.RecomputeMarkerStore;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top-level coordinator.
 *
 * <h3>Parallelism and isolation</h3>
 * Every (metric id, version) gets its own aggregator and its own state; a
 * fixed, bounded pool ({@code pipeline.worker-threads}) drains them round
 * robin. One slow/blocked metric cannot stop another beyond consuming one pool
 * thread; memory per metric is bounded by {@code pipeline.max-events-per-metric}
 * (when exceeded the metric skips catching up until it drains, and the lag is
 * exposed — it never silently overwrites results).
 *
 * <h3>Consistency under drain and recompute</h3>
 * Incremental drain commits whole batches atomically. A full recomputation
 * replays the log into separate state, then:
 * <ol>
 *   <li>pauses that metric's worker (pause does not block ingestion or other
 *       metrics — already durable new events are safe and merely wait);</li>
 *   <li>catches the rebuilt state up to the current log tip and asserts it
 *       equals a fresh replay (parity check);</li>
 *   <li>publishes the rebuilt snapshot and atomically points the checkpoint at
 *       it, then resumes.</li>
 * </ol>
 * Failure at any point leaves the previous committed pointer valid; an on-disk
 * marker records the unfinished recompute so a restart can distinguish it from
 * committed work.
 */
public class PipelineOrchestrator {

    private final EventLog eventLog;
    private final SnapshotStore snapshotStore;
    private final DefinitionsRepository definitions;
    private final PipelineProperties props;
    private final RecomputeMarkerStore recomputeMarkers;
    private final List<RecomputeMarker> abortedOnStartup = new ArrayList<>();

    private final Map<String, MetricAggregator> aggregators = new ConcurrentHashMap<>();
    private final Map<String, AtomicBoolean> paused = new ConcurrentHashMap<>();
    private ExecutorService pool;
    private volatile boolean running;
    private Thread supervisor;
    private final Object lifecycle = new Object();

    public PipelineOrchestrator(EventLog eventLog,
                                SnapshotStore snapshotStore,
                                DefinitionsRepository definitions,
                                PipelineProperties props) {
        this.eventLog = eventLog;
        this.snapshotStore = snapshotStore;
        this.definitions = definitions;
        this.props = props;
        this.recomputeMarkers = new FileRecomputeMarkerStore(props.getDataDir());
    }

    public void start() {
        synchronized (lifecycle) {
            if (running) {
                return;
            }
            running = true;
            // Definitions registered in a previous process must be recovered
            // before workers start, otherwise their aggregators would not
            // exist and committed results would look "missing" after restart.
            if (definitions instanceof
                    com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository fileDefs) {
                fileDefs.loadAll();
            }
            for (MetricDefinition def : definitions.allActive()) {
                spawnAggregator(def);
            }
            for (RecomputeMarker m : recomputeMarkers.allOpen()) {
                recomputeMarkers.complete(m.metricId(), m.version());
                abortedOnStartup.add(m);
            }
            pool = Executors.newFixedThreadPool(Math.max(1, props.getWorkerThreads()), r -> {
                Thread t = new Thread(r, "metric-worker");
                t.setDaemon(true);
                return t;
            });
            supervisor = new Thread(this::supervise, "metric-supervisor");
            supervisor.setDaemon(true);
            supervisor.start();
        }
    }

    private void spawnAggregator(MetricDefinition def) {
        MetricAggregator agg = AggregatorFactory.create(def, eventLog, snapshotStore);
        agg.open();
        aggregators.put(key(def.id(), def.version()), agg);
        paused.computeIfAbsent(key(def.id(), def.version()), k -> new AtomicBoolean(false));
    }

    @PreDestroy
    public void stop() {
        synchronized (lifecycle) {
            if (!running) {
                return;
            }
            running = false;
            if (supervisor != null) {
                supervisor.interrupt();
            }
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void supervise() {
        while (running) {
            List<Future<?>> futures = new ArrayList<>();
            List<MetricAggregator> ready = new ArrayList<>(aggregators.values());
            ready.sort(Comparator.comparing(MetricAggregator::metricId)
                    .thenComparingInt(MetricAggregator::version));
            for (MetricAggregator agg : ready) {
                AtomicBoolean gate = paused.get(key(agg.metricId(), agg.version()));
                if (gate != null && gate.get()) {
                    continue;
                }
                if (agg.committedSeq() >= eventLog.latestSeq()) {
                    continue;
                }
                futures.add(pool.submit(() -> drainGuarded(agg)));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    // One failing metric must not kill workers for the others;
                    // its lag remains visible and it is retried next round.
                    Thread.currentThread().interrupt(); // defensive; cleared below
                    Thread.interrupted();
                }
            }
            try {
                Thread.sleep(props.getDrainIntervalMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void drainGuarded(MetricAggregator agg) {
        try {
            long held = agg instanceof FunnelAggregator fa
                    ? fa.funnelState().eventCount()
                    : ((RetentionAggregator) agg).retentionState().eventCount();
            if (held >= props.getMaxEventsPerMetric()) {
                // Resource upper bound: skip catching up; lag surfaces in status.
                return;
            }
            synchronized (agg.commitLock()) {
                agg.drainOnce(props.getDrainBatchSize());
            }
        } catch (IOException | RuntimeException e) {
            // State is unchanged on a failed batch (commit is atomic); next
            // round replays the same seq range — no double count, no skip.
        }
    }

    public IngestResult ingest(List<Event> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("events must be a non-empty list");
        }
        long horizon = System.currentTimeMillis() - props.getMaxOutOfOrderHorizonMillis();
        List<Event> accepted = new ArrayList<>();
        List<String> rejected = new ArrayList<>();
        for (Event e : events) {
            if (e.eventTime() < horizon) {
                rejected.add(e.eventId());
            } else {
                accepted.add(e);
            }
        }
        IngestResult result = eventLog.ingest(accepted);
        List<String> dups = new ArrayList<>(result.duplicateIds());
        dups.addAll(rejected);
        return new IngestResult(result.appended(), dups);
    }

    public synchronized MetricDefinition registerDefinition(MetricDefinition def) {
        definitions.register(def);
        if (!aggregators.containsKey(key(def.id(), def.version()))) {
            spawnAggregator(def);
        }
        return def;
    }

    public DefinitionsRepository definitions() {
        return definitions;
    }

    public EventLog eventLog() {
        return eventLog;
    }

    public SnapshotStore snapshots() {
        return snapshotStore;
    }

    public Optional<CommittedView> view(String metricId, Integer version) {
        int v = version != null ? version : definitions.active(metricId).version();
        MetricAggregator agg = aggregators.get(key(metricId, v));
        return agg == null ? Optional.empty() : Optional.ofNullable(agg.view());
    }

    MetricAggregator aggregator(String metricId, int version) {
        return aggregators.get(key(metricId, version));
    }

    /** Test hook: pause/resume one metric's background worker. */
    public void pauseForTest(String metricId, int version, boolean gate) {
        AtomicBoolean b = paused.get(key(metricId, version));
        if (b != null) {
            b.set(gate);
        }
    }

    /**
     * Synchronously drain one metric until it reaches the current log tip.
     * Intended for tests and operational tooling; performs the same locked,
     * atomic batch commits as background workers.
     */
    public void drainAwait(String metricId, int version) {
        MetricAggregator agg = aggregators.get(key(metricId, version));
        if (agg == null) {
            throw new IllegalArgumentException("unknown metric " + metricId + "@v" + version);
        }
        synchronized (agg.commitLock()) {
            try {
                int n;
                do {
                    n = agg.drainOnce(props.getDrainBatchSize());
                } while (n > 0);
            } catch (IOException e) {
                throw new IllegalStateException("drain failed", e);
            }
        }
    }

    /**
     * Full recomputation: rebuild from the entire durable log, verify parity
     * against what a from-scratch replay would produce at the same tip, then
     * atomically adopt. Events ingested during the build are not lost: the
     * builder state is extended to the tip before the switch.
     */
    public RecomputeMarker recompute(String metricId, Integer version) {
        int v = version != null ? version : definitions.active(metricId).version();
        MetricAggregator agg = aggregators.get(key(metricId, v));
        if (agg == null) {
            throw new IllegalArgumentException("unknown metric " + metricId + "@v" + v);
        }
        AtomicBoolean gate = paused.get(key(metricId, v));
        synchronized (agg.commitLock()) {
            gate.set(true);
            try {
                long tip0 = eventLog.latestSeq();
                recomputeMarkers.begin(new RecomputeMarker(metricId, v, tip0, RecomputeMarker.BUILDING));
                long tip;
                AggregatorState rebuilt;
                // Build independently; while building, ingestion continues and
                // the live state keeps serving the last committed snapshot.
                do {
                    tip = eventLog.latestSeq();
                    rebuilt = agg.rebuildUpTo(tip, props.getDrainBatchSize());
                } while (eventLog.latestSeq() != tip); // events arrived: extend
                // Parity: rebuilt commit seq must reach the tip and its row
                // content must match the current committed view's log basis.
                if (rebuilt.commitSeq() != tip) {
                    throw new IllegalStateException("recompute parity failed: seq mismatch");
                }
                if (!parity(agg, rebuilt)) {
                    throw new IllegalStateException("recompute parity failed: content mismatch");
                }
                recomputeMarkers.update(new RecomputeMarker(metricId, v, tip, RecomputeMarker.COMPARING));
                agg.adopt(rebuilt);
                recomputeMarkers.complete(metricId, v);
                return new RecomputeMarker(metricId, v, tip, "COMPLETED");
            } catch (IOException | RuntimeException e) {
                // Marker stays until success; on a manual retry it is
                // overwritten. Previous committed checkpoint remains live.
                throw new IllegalStateException("recompute failed; previous results remain live", e);
            } finally {
                gate.set(false);
            }
        }
    }

    /**
     * Structural parity between live state and rebuilt state at the same seq.
     * Because both states are deterministic merges of the same log records,
     * their serialised NDJSON bodies must be byte-identical after the header.
     */
    private boolean parity(MetricAggregator live, AggregatorState rebuilt) {
        if (live.committedSeq() != rebuilt.commitSeq()) {
            return false;
        }
        return SnapshotParity.equalsSerialized(live, rebuilt);
    }

    public List<MetricLag> lag() {
        long latest = eventLog.latestSeq();
        List<MetricLag> out = new ArrayList<>();
        aggregators.values().forEach(a -> {
            long c = a.committedSeq();
            out.add(new MetricLag(a.metricId(), a.version(), c, latest, latest - c));
        });
        out.sort(Comparator.comparing(MetricLag::metricId).thenComparingInt(MetricLag::version));
        return out;
    }

    /** Recomputations found unfinished at startup and safely aborted (old
     *  committed checkpoint kept). Exposed for operational explainability. */
    public List<RecomputeMarker> abortedRecomputesOnStartup() {
        return List.copyOf(abortedOnStartup);
    }

    private static String key(String id, int version) {
        return id + "#" + version;
    }
}
