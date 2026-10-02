package com.github.diudiuseven.productmetricspipelinejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * File-backed {@link RecomputeMarkerStore}. Markers live under
 * {@code <dataDir>/recompute/<metric>/v<n>.marker} as one small JSON object,
 * published via temp + atomic rename. Any marker left behind by a crashed
 * process is treated as "abort and discard the uncommitted rebuild" on
 * recovery — the previous checkpoint stays authoritative.
 */
public class FileRecomputeMarkerStore implements RecomputeMarkerStore {

    private final Path root;
    private final ObjectMapper mapper = new ObjectMapper();

    public FileRecomputeMarkerStore(String dataDir) {
        this.root = Path.of(dataDir, "recompute");
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create recompute marker root", e);
        }
    }

    private Path file(String metricId, int version) {
        if (!metricId.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("illegal metric id: " + metricId);
        }
        Path d = root.resolve(metricId);
        try {
            Files.createDirectories(d);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return d.resolve("v" + version + ".marker");
    }

    private void writeAtomic(Path target, ObjectNode node) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(java.nio.ByteBuffer.wrap(mapper.writeValueAsBytes(node)));
            ch.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel ch = (FileChannel) FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignore) {
            // directory fsync best effort
        }
    }

    @Override
    public void begin(RecomputeMarker marker) {
        write(marker);
    }

    @Override
    public void update(RecomputeMarker marker) {
        write(marker);
    }

    private void write(RecomputeMarker m) {
        try {
            ObjectNode n = mapper.createObjectNode();
            n.put("metricId", m.metricId());
            n.put("version", m.version());
            n.put("targetSeq", m.targetSeq());
            n.put("phase", m.phase());
            writeAtomicFile(file(m.metricId(), m.version()), n);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist recompute marker", e);
        }
    }

    private void writeAtomicFile(Path target, ObjectNode node) throws IOException {
        writeAtomic(target, node);
    }

    @Override
    public void complete(String metricId, int version) {
        try {
            Files.deleteIfExists(file(metricId, version));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to clear recompute marker", e);
        }
    }

    @Override
    public Optional<RecomputeMarker> find(String metricId, int version) {
        Path f = file(metricId, version);
        if (!Files.exists(f)) {
            return Optional.empty();
        }
        return Optional.of(read(f));
    }

    private RecomputeMarker read(Path f) {
        try {
            var n = mapper.readTree(Files.readAllBytes(f));
            return new RecomputeMarker(n.get("metricId").asText(), n.get("version").asInt(),
                    n.get("targetSeq").asLong(), n.get("phase").asText());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read recompute marker " + f, e);
        }
    }

    @Override
    public List<RecomputeMarker> allOpen() {
        List<RecomputeMarker> out = new ArrayList<>();
        if (!Files.exists(root)) {
            return out;
        }
        try (Stream<Path> dirs = Files.list(root).filter(Files::isDirectory)) {
            for (Path d : dirs.sorted().toList()) {
                try (Stream<Path> files = Files.list(d)) {
                    files.filter(p -> p.getFileName().toString().endsWith(".marker"))
                            .sorted().forEach(p -> out.add(read(p)));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot scan recompute markers", e);
        }
        return out;
    }
}
