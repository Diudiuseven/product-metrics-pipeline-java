package com.github.diudiuseven.productmetricspipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.github.diudiuseven.productmetricspipelinejava.model.Event;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * File-backed {@link EventLog}.
 *
 * <p>Layout under {@code <dataDir>/log/}: segment files {@code 000000.seg},
 * each a sequence of length-prefixed UTF-8 JSON records:
 * <pre>
 *   [4-byte big-endian length n][n bytes JSON of EventRecord]
 * </pre>
 *
 * <p>All records in one {@link #ingest} call are written with a single
 * {@code force(true)} on the file and an {@code fsync} of the directory when a
 * new segment is created, so either the whole batch is durable or none of it
 * is; recovery truncates any length/frame that does not end on a record
 * boundary. Global seq order equals record order across segments.
 */
public class FileEventLog implements EventLog {

    private static final int FRAME_LIMIT = 16 * 1024 * 1024;

    private final Path dir;
    private final ObjectMapper mapper;
    private final Object writeLock = new Object();

    private final List<Path> segments = new ArrayList<>();
    private Path activeSegment;
    private long nextSeq;
    private long activeBytes;
    private boolean segmentJustCreated;
    /** Durably-known max revision per event id (rebuilt on open). */
    private final java.util.Map<String, Long> maxRevision = new HashMap<>();

    public FileEventLog(String dataDir) {
        this.dir = Path.of(dataDir, "log");
        this.mapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);
        try {
            Files.createDirectories(dir);
            open();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open event log at " + dir, e);
        }
    }

    private void open() throws IOException {
        long maxRolledSeq = -1L;
        List<Path> found;
        try (Stream<Path> files = Files.list(dir)) {
            found = files.filter(p -> p.getFileName().toString().endsWith(".seg"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        }
        for (int i = 0; i < found.size(); i++) {
            Path seg = found.get(i);
            if (Files.size(seg) == 0) {
                // Zero-size segments carry no batch (created eagerly ahead of
                // the first write, or left by a crash); they are never part of
                // the seq chain. Drop them unless they are the trailing active
                // segment, which is reused for the next append.
                if (i < found.size() - 1) {
                    Files.deleteIfExists(seg);
                }
                continue;
            }
            long[] meta = scanSegment(seg, r -> maxRevision.merge(r.eventId(), r.revision(), Math::max));
            long firstSeq = meta[0];
            long lastSeq = meta[1];
            if (firstSeq != maxRolledSeq + 1) {
                throw new IOException("non-contiguous event log segment " + seg + " expected first seq "
                        + (maxRolledSeq + 1) + " but was " + firstSeq);
            }
            maxRolledSeq = lastSeq;
            segments.add(seg);
        }
        if (segments.isEmpty()) {
            activeSegment = dir.resolve("000000.seg");
            if (!Files.exists(activeSegment)) {
                Files.createFile(activeSegment);
                segmentJustCreated = true;
            }
            segments.add(activeSegment);
            nextSeq = 0;
            activeBytes = Files.size(activeSegment);
        } else {
            activeSegment = segments.get(segments.size() - 1);
            nextSeq = maxRolledSeq + 1;
            activeBytes = Files.size(activeSegment);
        }
    }

    /**
     * Scan one segment, returning {firstSeq, lastSeq}; a truncated trailing
     * frame (process died mid-write) is physically removed on open.
     */
    private long[] scanSegment(Path seg, Consumer<EventRecord> visitor) throws IOException {
        long firstSeq = -1L;
        long lastSeq = -1L;
        long goodBytes = 0L;
        byte[] all = Files.readAllBytes(seg);
        int pos = 0;
        while (pos < all.length) {
            if (all.length - pos < 4) {
                break;
            }
            int len = ((all[pos] & 0xff) << 24) | ((all[pos + 1] & 0xff) << 16)
                    | ((all[pos + 2] & 0xff) << 8) | (all[pos + 3] & 0xff);
            if (len <= 0 || len > FRAME_LIMIT || pos + 4 + len > all.length) {
                break; // torn frame from a crashed batch
            }
            EventRecord r = decode(all, pos + 4, len);
            visitor.accept(r);
            if (firstSeq < 0) {
                firstSeq = r.seq();
            }
            if (lastSeq >= 0 && r.seq() != lastSeq + 1) {
                throw new IOException("non-contiguous seq in " + seg + ": " + r.seq() + " after " + lastSeq);
            }
            lastSeq = r.seq();
            pos += 4 + len;
            goodBytes = pos;
        }
        if (goodBytes < all.length) {
            try (java.nio.channels.FileChannel ch = (java.nio.channels.FileChannel)
                    Files.newByteChannel(seg, StandardOpenOption.WRITE)) {
                ch.truncate(goodBytes);
                ch.force(true);
            }
        }
        return new long[] {firstSeq, lastSeq};
    }

    @Override
    public IngestResult ingest(List<Event> batch) {
        if (batch == null || batch.isEmpty()) {
            return new IngestResult(List.of(), List.of());
        }
        synchronized (writeLock) {
            List<String> dups = new ArrayList<>();
            List<Event> unique = new ArrayList<>();
            // Highest revision seen within this batch per id (later entry wins
            // for equal revisions -> that is a duplicate).
            java.util.Map<String, Event> inBatch = new HashMap<>();
            Set<String> inBatchDuplicates = new HashSet<>();
            for (Event e : batch) {
                validate(e);
                Long known = maxRevision.get(e.eventId());
                Event earlier = inBatch.get(e.eventId());
                if (known != null && e.revision() <= known) {
                    dups.add(e.eventId()); // replay of an already-durable fact
                } else if (earlier != null) {
                    if (e.revision() > earlier.revision()) {
                        inBatch.put(e.eventId(), e); // correction within batch
                    } else {
                        inBatchDuplicates.add(e.eventId());
                    }
                } else {
                    inBatch.put(e.eventId(), e);
                }
            }
            dups.addAll(inBatchDuplicates);
            unique.addAll(inBatch.values());
            if (unique.isEmpty()) {
                return new IngestResult(List.of(), dups);
            }
            long now = System.currentTimeMillis();
            List<EventRecord> records = new ArrayList<>(unique.size());
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            long seq = nextSeq;
            try {
                for (Event e : unique) {
                    EventRecord r = new EventRecord(seq++, e.eventId(), e.userId(), e.name(),
                            e.eventTime(), now, e.revision(), e.dimensions());
                    records.add(r);
                    byte[] json = mapper.writeValueAsBytes(r);
                    if (json.length > FRAME_LIMIT) {
                        throw new IOException("event frame exceeds " + FRAME_LIMIT + " bytes");
                    }
                    buf.write((json.length >>> 24) & 0xff);
                    buf.write((json.length >>> 16) & 0xff);
                    buf.write((json.length >>> 8) & 0xff);
                    buf.write(json.length & 0xff);
                    buf.write(json);
                }
                byte[] frame = buf.toByteArray();
                // One append + one fsync == one atomic durability boundary.
                try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(activeSegment,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(frame);
                    while (bb.hasRemaining()) {
                        ch.write(bb);
                    }
                    ch.force(true);
                }
                if (segmentJustCreated) {
                    fsyncDirectory(activeSegment.getParent());
                    segmentJustCreated = false;
                }
            } catch (IOException ex) {
                throw new UncheckedIOException("failed to durably ingest batch", ex);
            }
            records.forEach(r -> maxRevision.merge(r.eventId(), r.revision(), Math::max));
            nextSeq = seq;
            activeBytes += buf.size();
            rollIfNeeded();
            return new IngestResult(records, dups);
        }
    }

    private void rollIfNeeded() {
        long target = 64L * 1024 * 1024;
        if (activeBytes < target) {
            return;
        }
        activeSegment = dir.resolve(String.format("%06d.seg", segments.size()));
        try {
            Files.createFile(activeSegment);
            fsyncDirectory(activeSegment.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot roll event log segment", e);
        }
        segments.add(activeSegment);
        activeBytes = 0;
        segmentJustCreated = true;
    }

    private static void fsyncDirectory(Path dir) {
        try (java.nio.channels.FileChannel ch = (java.nio.channels.FileChannel)
                java.nio.channels.FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignore) {
            // Some platforms cannot fsync directories; best effort.
        }
    }

    private void validate(Event e) {
        if (e == null || e.eventId() == null || e.eventId().isBlank()) {
            throw new IllegalArgumentException("event.eventId must be a non-blank string");
        }
        if (e.userId() == null || e.userId().isBlank()) {
            throw new IllegalArgumentException("event.userId must be a non-blank string");
        }
        if (e.name() == null || e.name().isBlank()) {
            throw new IllegalArgumentException("event.name must be a non-blank string");
        }
    }

    @Override
    public long latestSeq() {
        synchronized (writeLock) {
            return nextSeq - 1;
        }
    }

    @Override
    public List<EventRecord> readNext(long afterSeq, int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be > 0");
        }
        List<EventRecord> out = new ArrayList<>(Math.min(max, 1024));
        long want = afterSeq + 1;
        for (Path seg : segments) {
            if (out.size() >= max) {
                break;
            }
            readSegment(seg, want, max, out);
            if (!out.isEmpty()) {
                want = out.get(out.size() - 1).seq() + 1;
            }
        }
        return out;
    }

    private void readSegment(Path seg, long fromSeq, int max, List<EventRecord> out) {
        try {
            byte[] all = Files.readAllBytes(seg);
            int pos = 0;
            while (pos < all.length && out.size() < max) {
                int len = ((all[pos] & 0xff) << 24) | ((all[pos + 1] & 0xff) << 16)
                        | ((all[pos + 2] & 0xff) << 8) | (all[pos + 3] & 0xff);
                EventRecord r = decode(all, pos + 4, len);
                if (r.seq() >= fromSeq) {
                    out.add(r);
                }
                pos += 4 + len;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read segment " + seg, e);
        }
    }

    @Override
    public void replay(Consumer<EventRecord> visitor) {
        long want = 0;
        for (Path seg : List.copyOf(segments)) {
            List<EventRecord> page = new ArrayList<>();
            readSegment(seg, want, Integer.MAX_VALUE, page);
            for (EventRecord r : page) {
                visitor.accept(r);
                want = r.seq() + 1;
            }
        }
    }

    private EventRecord decode(byte[] all, int off, int len) throws IOException {
        try (var parser = mapper.getFactory().createParser(all, off, len)) {
            EventRecord r = parser.readValueAs(EventRecord.class);
            if (r == null || r.seq() < 0) {
                throw new IOException("invalid event frame");
            }
            return r;
        } catch (com.fasterxml.jackson.core.JacksonException e) {
            throw new IOException("corrupt event frame", e);
        }
    }

}
