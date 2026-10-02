package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.engine.AggregatorState;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelAggregator;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelClassifier;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.Journey;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionAggregator;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.query.FunnelStatus;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The central correctness contract: no matter how events arrive (late,
 * out-of-order, duplicated, corrected), the committed incremental state is
 * byte-for-byte what a from-scratch replay of the same log prefix produces.
 */
class AggregatorConsistencyTest {

    @TempDir
    Path tmp;

    static final long HOUR = 3600_000L;

    private MetricDefinition funnelDef() {
        return MetricDefinition.builder()
                .id("f").version(1).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(6 * HOUR).stepWindowMillis(6 * HOUR)
                .dimensionKeys(List.of("plan"))
                .build();
    }

    private String hash(FunnelAggregator agg) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        agg.funnelState().writeSnapshot(out, new ObjectMapper());
        String text = out.toString(StandardCharsets.UTF_8);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(text.substring(text.indexOf('\n') + 1)
                .getBytes(StandardCharsets.UTF_8)));
    }

    private String hashState(AggregatorState st) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ((com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelState) st)
                .writeSnapshot(out, new ObjectMapper());
        String text = out.toString(StandardCharsets.UTF_8);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(md.digest(text.substring(text.indexOf('\n') + 1)
                .getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void lateOutOfOrderDuplicateAndCorrectedEventsMatchFullReplay() throws Exception {
        FileEventLog log = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        MetricDefinition def = funnelDef();
        FunnelAggregator agg = new FunnelAggregator(def, log, snaps);
        agg.open();

        // Batch 1: u1 starts.
        log.ingest(List.of(new Event("e1", "u1", "view", HOUR, Map.of("plan", "free"))));
        agg.drainOnce(100);
        Journey j1 = FunnelClassifier.classify(def, agg.funnelState().eventsOf("u1"), 0, 10 * HOUR);
        assertEquals(FunnelStatus.DROPPED, j1.status());

        // Late arrival (older eventTime, newer ingestion): u2's view then buy.
        log.ingest(List.of(
                new Event("e2", "u2", "buy", 5 * HOUR, Map.of()),
                new Event("e3", "u2", "view", 2 * HOUR, Map.of())));
        agg.drainOnce(100);
        Journey j2 = FunnelClassifier.classify(def, agg.funnelState().eventsOf("u2"), 0, 10 * HOUR);
        assertEquals(FunnelStatus.CONVERTED, j2.status());

        // Duplicate of e3 must not change anything.
        String beforeDup = hash(agg);
        log.ingest(List.of(new Event("e3", "u2", "view", 2 * HOUR, Map.of())));
        agg.drainOnce(100);
        assertEquals(beforeDup, hash(agg), "duplicate replay must leave state identical");

        // Post-hoc correction: u1's view moves out of window of its later buy...
        // first add a buy for u1, then correct e1 to an earlier time so it
        // remains a conversion; also correct plan dimension.
        log.ingest(List.of(new Event("e4", "u1", "buy", 3 * HOUR, Map.of())));
        agg.drainOnce(100);
        log.ingest(List.of(new Event("e1", "u1", "view", 2 * HOUR, Map.of("plan", "pro"), 1L)));
        agg.drainOnce(100);
        Journey j1b = FunnelClassifier.classify(def, agg.funnelState().eventsOf("u1"), 0, 10 * HOUR);
        assertEquals(FunnelStatus.CONVERTED, j1b.status());
        assertEquals(List.of("pro"), j1b.anchor().dims(List.of("plan")));

        // Correction that moves the fact away from a relevant name removes it.
        String beforeRename = hash(agg);
        log.ingest(List.of(new Event("e4", "u1", "refund", 3 * HOUR, Map.of(), 2L)));
        // Constructor with revision + name change:
        agg.drainOnce(100);
        Journey j1c = FunnelClassifier.classify(def, agg.funnelState().eventsOf("u1"), 0, 10 * HOUR);
        assertEquals(FunnelStatus.DROPPED, j1c.status());
        assertNotEquals(beforeRename, hash(agg));

        // Full replay of the exact same log prefix must equal incremental state.
        AggregatorState rebuilt = agg.rebuildUpTo(log.latestSeq(), 2);
        assertEquals(hash(agg), hashState(rebuilt),
                "incremental result must equal from-scratch recomputation row-for-row");
    }

    @Test
    void restartReloadsCommittedStateAndContinuesWithoutDoubleCount() throws Exception {
        FileEventLog log = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        MetricDefinition def = funnelDef();
        FunnelAggregator first = new FunnelAggregator(def, log, snaps);
        first.open();
        log.ingest(List.of(new Event("e1", "u1", "view", HOUR, Map.of())));
        first.drainOnce(100);
        long committedAfterFirst = first.committedSeq();

        // Simulate process restart: fresh objects over the same directory.
        FileEventLog log2 = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps2 = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        FunnelAggregator second = new FunnelAggregator(def, log2, snaps2);
        second.open();
        assertEquals(committedAfterFirst, second.committedSeq());
        assertEquals(1, second.funnelState().userCount());

        log2.ingest(List.of(new Event("e2", "u1", "buy", 2 * HOUR, Map.of())));
        second.drainOnce(100);
        Journey j = FunnelClassifier.classify(def, second.funnelState().eventsOf("u1"), 0, 10 * HOUR);
        assertEquals(FunnelStatus.CONVERTED, j.status());
    }

    @Test
    void retentionIncrementalEqualsFullReplay() throws Exception {
        FileEventLog log = new FileEventLog(tmp.toString());
        FileSnapshotStore snaps = new FileSnapshotStore(tmp.toString(), new ObjectMapper());
        MetricDefinition def = MetricDefinition.builder()
                .id("r").version(1).type(MetricType.RETENTION)
                .anchorEvent("signup").retainedEvent("active")
                .retentionPeriodsMillis(List.of(DAY, 2 * DAY)).cohortBucketMillis(DAY)
                .build();
        RetentionAggregator agg = new RetentionAggregator(def, log, snaps);
        agg.open();
        log.ingest(List.of(
                new Event("a1", "u1", "active", DAY + 100, Map.of()),
                new Event("s1", "u1", "signup", 0, Map.of()),
                new Event("s2", "u2", "signup", DAY, Map.of())));
        agg.drainOnce(100);
        var rebuilt = agg.rebuildUpTo(log.latestSeq(), 50);
        assertEquals(agg.retentionState().userCount(), rebuilt.userCount());
        assertEquals(agg.committedSeq(), rebuilt.commitSeq());
    }

    static final long DAY = 24L * HOUR;
}
