package com.github.diudiuseven.productmetricspipelinejava.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Directory-backed {@link SnapshotStore}.
 *
 * <p>Per (metric,version), under {@code <dataDir>/snapshots/<metric>/v<n>/}:
 * <ul>
 *   <li>{@code snap-<seq>.ndjson} — immutable NDJSON snapshots (header + rows);</li>
 *   <li>{@code current.ptr}       — one JSON object naming the current snapshot
 *       and commit seq; replaced via temp file + atomic rename + dir fsync.</li>
 * </ul>
 *
 * <p>Two-phase publication: the snapshot file is fully written and fsynced
 * <em>before</em> the pointer is renamed into place, so a snapshot without a
 * pointer is provably uncommitted and treated as garbage on recovery.
 */
public class FileSnapshotStore implements SnapshotStore {

    private final Path root;
    private final ObjectMapper mapper;

    public FileSnapshotStore(String dataDir, ObjectMapper mapper) {
        this.root = Path.of(dataDir, "snapshots");
        this.mapper = mapper;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create snapshot root " + root, e);
        }
    }

    private Path dir(String metricId, int version) {
        return root.resolve(safe(metricId)).resolve("v" + version);
    }

    private static String safe(String metricId) {
        if (!metricId.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("illegal metric id: " + metricId);
        }
        return metricId;
    }

    @Override
    public void commit(String metricId, int version, long commitSeq, Consumer<OutputStream> writer)
            throws IOException {
        Path d = dir(metricId, version);
        Files.createDirectories(d);
        String snapName = "snap-" + commitSeq + ".ndjson";
        Path snap = d.resolve(snapName);

        // Phase 1: whole NDJSON snapshot durably via temp -> atomic rename.
        Path tmp = d.resolve(snapName + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
             BufferedOutputStream out = new BufferedOutputStream(
                     new java.io.OutputStream() {
                         @Override
                         public void write(int b) throws IOException {
                             ch.write(java.nio.ByteBuffer.wrap(new byte[] {(byte) b}));
                         }

                         @Override
                         public void write(byte[] b, int off, int len) throws IOException {
                             java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b, off, len);
                             while (bb.hasRemaining()) {
                                 ch.write(bb);
                             }
                         }
                     })) {
            writer.accept(out);
            out.flush();
            ch.force(true);
        }
        Files.move(tmp, snap, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(d);

        // Phase 2: publish pointer atomically.
        ObjectNode ptr = mapper.createObjectNode();
        ptr.put("metricId", metricId);
        ptr.put("version", version);
        ptr.put("commitSeq", commitSeq);
        ptr.put("snapshot", snapName);
        Path ptrFile = d.resolve("current.ptr");
        Path ptrTmp = d.resolve("current.ptr.tmp");
        try (FileChannel ch = FileChannel.open(ptrTmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(mapper.writeValueAsBytes(ptr)));
            ch.force(true);
        }
        Files.move(ptrTmp, ptrFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        fsyncDir(d);

        pruneOldSnapshots(d, snapName);
    }

    private void pruneOldSnapshots(Path d, String keep) {
        try (var list = Files.list(d)) {
            var old = list.filter(p -> p.getFileName().toString().startsWith("snap-")
                            && p.getFileName().toString().endsWith(".ndjson")
                            && !p.getFileName().toString().equals(keep))
                    .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
                    .skip(1)
                    .toList();
            for (Path p : old) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignore) {
                    // a reader may hold it; harmless
                }
            }
        } catch (IOException ignore) {
            // maintenance only
        }
    }

    private static void fsyncDir(Path dir) {
        try (FileChannel ch = (FileChannel) FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignore) {
            // best effort on platforms without directory fsync
        }
    }

    @Override
    public Optional<Checkpoint> current(String metricId, int version) {
        Path ptr = dir(metricId, version).resolve("current.ptr");
        if (!Files.exists(ptr)) {
            return Optional.empty();
        }
        try {
            JsonNode node = mapper.readTree(Files.readAllBytes(ptr));
            return Optional.of(new Checkpoint(
                    node.get("metricId").asText(),
                    node.get("version").asInt(),
                    node.get("commitSeq").asLong(),
                    node.get("snapshot").asText()));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read checkpoint pointer for " + metricId, e);
        }
    }

    @Override
    public Path resolve(Checkpoint checkpoint) {
        return dir(checkpoint.metricId(), checkpoint.version()).resolve(checkpoint.snapshotPath());
    }
}
