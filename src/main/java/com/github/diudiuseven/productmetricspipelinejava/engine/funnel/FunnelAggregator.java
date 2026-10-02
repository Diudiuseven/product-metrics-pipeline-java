package com.github.diudiuseven.productmetricspipelinejava.engine.funnel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.engine.AggregatorState;
import com.github.diudiuseven.productmetricspipelinejava.engine.CommittedView;
import com.github.diudiuseven.productmetricspipelinejava.engine.MetricAggregator;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.Checkpoint;
import com.github.diudiuseven.productmetricspipelinejava.store.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.EventRecord;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Funnel aggregator. State transitions are:
 * <pre>
 *   open()            -> restore last committed NDJSON snapshot or empty
 *   drainOnce() loop  -> pull next log batch, merge into state, publish a new
 *                        immutable snapshot + move the checkpoint pointer
 *   rebuildUpTo()     -> replay the whole log into a separate state, leaving
 *                        live state untouched (background recompute path)
 *   adopt()           -> under external coordination, atomically publish and
 *                        point at the rebuilt state
 * </pre>
 * Queries read {@link #view()} — the last published snapshot file — never the
 * live in-flight state, so they always see a single self-consistent whole.
 */
public class FunnelAggregator extends MetricAggregator {

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<CommittedView> published = new AtomicReference<>();

    public FunnelAggregator(MetricDefinition definition, EventLog eventLog, SnapshotStore snapshots) {
        super(definition, eventLog, snapshots);
        this.state = new FunnelState(definition);
    }

    public FunnelState funnelState() {
        return (FunnelState) state;
    }

    @Override
    public void open() {
        Optional<Checkpoint> cp = snapshots.current(metricId(), version());
        if (cp.isPresent()) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    Files.newInputStream(snapshots.resolve(cp.get())), StandardCharsets.UTF_8))) {
                String headerLine = r.readLine();
                if (headerLine != null) {
                    FunnelState restored = new FunnelState(definition);
                    JsonNode header = mapper.readTree(headerLine);
                    restored.restoreHeader(header);
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (!line.isEmpty()) {
                            restored.restoreRow(mapper.readTree(line));
                        }
                    }
                    state = restored;
                    published.set(new CommittedView(metricId(), version(), cp.get().commitSeq(), header));
                }
            } catch (IOException e) {
                throw new IllegalStateException("cannot restore funnel " + metricId(), e);
            }
        }
    }

    @Override
    public int drainOnce(int maxEvents) throws IOException {
        List<EventRecord> batch = eventLog.readNext(state.commitSeq(), maxEvents);
        if (batch.isEmpty()) {
            return 0;
        }
        for (EventRecord r : batch) {
            state.apply(r);
        }
        publish(state);
        return batch.size();
    }

    private void publish(AggregatorState st) throws IOException {
        FunnelState fs = (FunnelState) st;
        long seq = fs.commitSeq();
        snapshots.commit(metricId(), version(), seq, out -> {
            try {
                fs.writeSnapshot(out, mapper);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        Checkpoint cp = snapshots.current(metricId(), version()).orElseThrow();
        JsonNode header = mapper.createObjectNode();
        // Header content mirrors what writeSnapshot emits; re-read it so the
        // view is literally the committed bytes.
        try (BufferedReader r = Files.newBufferedReader(snapshots.resolve(cp), StandardCharsets.UTF_8)) {
            header = mapper.readTree(r.readLine());
        }
        published.set(new CommittedView(metricId(), version(), seq, header));
    }

    @Override
    public AggregatorState rebuildUpTo(long targetSeq, int batchSize) {
        FunnelState fresh = new FunnelState(definition);
        long after = -1L;
        while (after < targetSeq) {
            List<EventRecord> batch = eventLog.readNext(after, batchSize);
            if (batch.isEmpty()) {
                break;
            }
            for (EventRecord r : batch) {
                if (r.seq() > targetSeq) {
                    return fresh;
                }
                fresh.apply(r);
                after = r.seq();
            }
        }
        return fresh;
    }

    @Override
    public void adopt(AggregatorState rebuilt) throws IOException {
        publish(rebuilt);
        state = rebuilt;
    }

    @Override
    public long committedSeq() {
        CommittedView v = published.get();
        return v == null ? -1L : v.commitSeq();
    }

    @Override
    public CommittedView view() {
        return published.get();
    }
}
