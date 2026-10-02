package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.github.diudiuseven.productmetricspipelinejava.model.Event;
import com.github.diudiuseven.productmetricspipelinejava.store.EventRecord;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.IngestResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileEventLogTest {

    @TempDir
    Path tmp;

    private FileEventLog log() {
        return new FileEventLog(tmp.toString());
    }

    private Event event(String id, String user, String name, long t) {
        return new Event(id, user, name, t, Map.of());
    }

    @Test
    void assignsGapFreeContiguousSeqsAndPersistsAcrossRestart() {
        FileEventLog log = log();
        IngestResult r1 = log.ingest(List.of(event("e1", "u1", "view", 10), event("e2", "u1", "buy", 20)));
        assertEquals(0L, r1.appended().get(0).seq());
        assertEquals(1L, r1.appended().get(1).seq());
        assertEquals(1L, log.latestSeq());

        IngestResult r2 = log.ingest(List.of(event("e3", "u2", "view", 30)));
        assertEquals(2L, r2.appended().get(0).seq());

        // Reopen: dedup set and seq counter come from disk.
        FileEventLog reopened = log();
        assertEquals(2L, reopened.latestSeq());
        List<EventRecord> page = reopened.readNext(-1, 100);
        assertEquals(3, page.size());
        assertIterableEquals(List.of(0L, 1L, 2L), page.stream().map(EventRecord::seq).toList());
    }

    @Test
    void duplicateEventIdsAreRejectedNeverAppended() {
        FileEventLog log = log();
        log.ingest(List.of(event("dup", "u1", "view", 10)));
        IngestResult again = log.ingest(List.of(
                event("dup", "u1", "view", 10),
                event("fresh", "u1", "view", 11)));
        assertIterableEquals(List.of("dup"), again.duplicateIds());
        assertEquals(1, again.appended().size());
        assertEquals("fresh", again.appended().get(0).eventId());

        // After reopening, the earlier id is still known as a duplicate.
        assertEquals(List.of("dup"), log().ingest(List.of(event("dup", "u", "v", 1))).duplicateIds());
    }

    @Test
    void readNextIsPagedAndOrdered() {
        FileEventLog log = log();
        log.ingest(List.of(event("a", "u", "v", 1), event("b", "u", "v", 2), event("c", "u", "v", 3)));
        assertEquals(List.of("a"), log.readNext(-1, 1).stream().map(EventRecord::eventId).toList());
        assertEquals(List.of("b", "c"), log.readNext(0, 10).stream().map(EventRecord::eventId).toList());
        assertTrue(log.readNext(2, 10).isEmpty());
    }

    @Test
    void emptyTrailingSegmentFromCrashReopensCleanly() throws Exception {
        FileEventLog log = log();
        log.ingest(List.of(event("a", "u", "v", 1)));
        // Simulate a rolled, never-written-to trailing segment after a crash.
        java.nio.file.Path stray = tmp.resolve("log").resolve("000001.seg");
        java.nio.file.Files.createFile(stray);
        FileEventLog reopened = log();
        assertEquals(0L, reopened.latestSeq());
        reopened.ingest(List.of(event("b", "u", "v", 2)));
        assertEquals(1L, reopened.latestSeq());
    }

    @Test
    void rejectsMalformedEvents() {
        FileEventLog log = log();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> log.ingest(List.of(new Event("", "u", "v", 1, Map.of()))));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> log.ingest(List.of(new Event("x", " ", "v", 1, Map.of()))));
    }
}
