package com.github.diudiuseven.productmetricspipelinejava.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * File-backed {@link DefinitionsRepository}.
 *
 * <p>Layout under {@code <dataDir>/definitions/<metric-id>/}:
 * <ul>
 *   <li>{@code v<n>.json} — immutable version payloads, all kept;</li>
 *   <li>{@code active.ptr} — JSON naming the active version.</li>
 * </ul>
 *
 * <p>Semantic versioning rules enforced here:
 * <ul>
 *   <li>a version is immutable: re-registering the same (id, version) with a
 *       different payload is a conflict (field-scoped error);</li>
 *   <li>versions ascend; the active pointer only moves forward;</li>
 *   <li>old versions remain on disk and queryable after upgrades — no
 *       migration step.</li>
 * </ul>
 */
public class FileDefinitionsRepository implements DefinitionsRepository {

    private final Path root;
    private final ObjectMapper mapper;
    private final Map<String, List<MetricDefinition>> byId = new ConcurrentHashMap<>();
    private final Map<String, Integer> activeVersion = new ConcurrentHashMap<>();

    public FileDefinitionsRepository(String dataDir, ObjectMapper mapper) {
        this.root = Path.of(dataDir, "definitions");
        this.mapper = mapper;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create definitions root", e);
        }
    }

    private static String safe(String id) {
        if (id == null || !id.matches("[A-Za-z0-9_.-]+")) {
            throw new IllegalArgumentException("illegal metric id: " + id);
        }
        return id;
    }

    @Override
    public synchronized MetricDefinition register(MetricDefinition d) {
        d.validate();
        String id = safe(d.id());
        Path dir = root.resolve(id);
        try {
            Files.createDirectories(dir);
            List<MetricDefinition> existing = byId.computeIfAbsent(id, k -> new ArrayList<>());
            Optional<MetricDefinition> same = existing.stream()
                    .filter(x -> x.version() == d.version()).findFirst();
            if (same.isPresent()) {
                if (!same.get().samePayload(d)) {
                    throw new DefinitionException(
                            "conflicting definition for " + id + "@v" + d.version(),
                            List.of(new DefinitionException.Problem("version",
                                    "version " + d.version() + " already exists with a different payload; "
                                            + "published definitions are immutable, register a higher version")));
                }
                return same.get(); // idempotent re-registration
            }
            int max = existing.stream().mapToInt(MetricDefinition::version).max().orElse(0);
            if (d.version() <= max) {
                throw new DefinitionException("out-of-order version for " + id,
                        List.of(new DefinitionException.Problem("version",
                                "must be greater than the highest known version " + max)));
            }
            // Persist version payload immutably + atomically.
            writeAtomically(dir.resolve("v" + d.version() + ".json"), toJson(d));
            // Publish active pointer (monotonic).
            int newActive = Math.max(d.version(), activeVersion.getOrDefault(id, 0));
            ObjectNode ptr = mapper.createObjectNode();
            ptr.put("active", newActive);
            writeAtomically(dir.resolve("active.ptr"), ptr);

            List<MetricDefinition> copy = new ArrayList<>(existing);
            copy.add(d);
            copy.sort(Comparator.comparingInt(MetricDefinition::version));
            byId.put(id, List.copyOf(copy));
            activeVersion.put(id, newActive);
            return d;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist definition " + id, e);
        }
    }

    private void writeAtomically(Path target, JsonNode node) throws IOException {
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

    private ObjectNode toJson(MetricDefinition d) {
        ObjectNode n = mapper.createObjectNode();
        n.put("id", d.id());
        n.put("version", d.version());
        n.put("type", d.type().name());
        n.put("totalWindowMillis", d.totalWindowMillis());
        n.put("stepWindowMillis", d.stepWindowMillis());
        if (d.anchorEvent() != null) {
            n.put("anchorEvent", d.anchorEvent());
        }
        if (d.retainedEvent() != null) {
            n.put("retainedEvent", d.retainedEvent());
        }
        if (!d.retentionPeriodsMillis().isEmpty()) {
            n.set("retentionPeriodsMillis", n.arrayNode().addAll(
                    d.retentionPeriodsMillis().stream().map(mapper.getNodeFactory()::numberNode).toList()));
        }
        if (!d.dimensionKeys().isEmpty()) {
            n.set("dimensionKeys", n.arrayNode().addAll(
                    d.dimensionKeys().stream().map(mapper.getNodeFactory()::textNode).toList()));
        }
        n.put("maxDimensionGroups", d.maxDimensionGroups());
        n.put("cohortBucketMillis", d.cohortBucketMillis());
        var steps = n.arrayNode();
        d.steps().forEach(s -> {
            var o = n.objectNode();
            o.put("order", s.order());
            o.put("eventName", s.eventName());
            steps.add(o);
        });
        n.set("steps", steps);
        return n;
    }

    /** Parse a persisted/request JSON node into a validated definition. */
    public MetricDefinition fromJson(JsonNode n) {
        var b = MetricDefinition.builder();
        List<DefinitionException.Problem> problems = new ArrayList<>();
        b.id(textOrProblems(n, "id", problems));
        Integer version = intOrProblems(n, "version", problems);
        b.version(version == null ? 1 : version);
        String type = textOrProblems(n, "type", problems);
        if (type != null) {
            try {
                b.type(MetricType.valueOf(type));
            } catch (IllegalArgumentException e) {
                problems.add(new DefinitionException.Problem("type", "must be FUNNEL or RETENTION"));
            }
        }
        if (n.hasNonNull("totalWindowMillis")) {
            b.totalWindowMillis(n.get("totalWindowMillis").asLong());
        }
        if (n.hasNonNull("stepWindowMillis")) {
            b.stepWindowMillis(n.get("stepWindowMillis").asLong());
        }
        if (n.hasNonNull("anchorEvent")) {
            b.anchorEvent(n.get("anchorEvent").asText());
        }
        if (n.hasNonNull("retainedEvent")) {
            b.retainedEvent(n.get("retainedEvent").asText());
        }
        if (n.hasNonNull("retentionPeriodsMillis")) {
            List<Long> periods = new ArrayList<>();
            n.get("retentionPeriodsMillis").forEach(x -> periods.add(x.asLong()));
            b.retentionPeriodsMillis(periods);
        }
        if (n.hasNonNull("dimensionKeys")) {
            List<String> keys = new ArrayList<>();
            n.get("dimensionKeys").forEach(x -> keys.add(x.asText()));
            b.dimensionKeys(keys);
        }
        if (n.hasNonNull("maxDimensionGroups")) {
            b.maxDimensionGroups(n.get("maxDimensionGroups").asInt());
        }
        if (n.hasNonNull("cohortBucketMillis")) {
            b.cohortBucketMillis(n.get("cohortBucketMillis").asLong());
        }
        if (n.hasNonNull("steps")) {
            List<FunnelStep> steps = new ArrayList<>();
            var arr = n.get("steps");
            for (int i = 0; i < arr.size(); i++) {
                JsonNode s = arr.get(i);
                Integer order = s != null && s.hasNonNull("order") ? s.get("order").asInt() : i;
                String name = s != null && s.hasNonNull("eventName") ? s.get("eventName").asText() : null;
                steps.add(new FunnelStep(order, name));
            }
            b.steps(steps);
        }
        MetricDefinition def;
        try {
            def = buildLenient(b);
        } catch (DefinitionException e) {
            throw new DefinitionException("invalid definition", e.problems());
        }
        def.validate();
        return def;
    }

    private MetricDefinition buildLenient(MetricDefinition.Builder b) {
        // Builder.build() validates; callers also call validate(). Wrap via
        // reflection-free construction: builder throws on invalid, which we let
        // propagate with field locations.
        return b.build();
    }

    private static String textOrProblems(JsonNode n, String field, List<DefinitionException.Problem> out) {
        if (!n.hasNonNull(field)) {
            return null;
        }
        return n.get(field).asText();
    }

    private static Integer intOrProblems(JsonNode n, String field, List<DefinitionException.Problem> out) {
        if (!n.hasNonNull(field)) {
            return null;
        }
        return n.get(field).asInt();
    }

    /** Load everything persisted on disk; invalid files are returned, not thrown. */
    public List<LoadIssue> loadAll() {
        List<LoadIssue> issues = new ArrayList<>();
        if (!Files.exists(root)) {
            return issues;
        }
        try (Stream<Path> dirs = Files.list(root).filter(Files::isDirectory)) {
            for (Path dir : dirs.sorted(Comparator.comparing(p -> p.getFileName().toString())).toList()) {
                String id = dir.getFileName().toString();
                List<Path> versionFiles;
                try (Stream<Path> s = Files.list(dir)) {
                    versionFiles = s.filter(p -> p.getFileName().toString().matches("v\\d+\\.json"))
                            .sorted().toList();
                }
                List<MetricDefinition> loaded = new ArrayList<>();
                for (Path vf : versionFiles) {
                    try {
                        MetricDefinition d = fromJson(mapper.readTree(Files.readAllBytes(vf)));
                        loaded.add(d);
                    } catch (DefinitionException e) {
                        issues.add(new LoadIssue(vf.toString(), id, e.problems()));
                    } catch (IOException e) {
                        issues.add(new LoadIssue(vf.toString(), id,
                                List.of(new DefinitionException.Problem("", "unreadable: " + e.getMessage()))));
                    }
                }
                loaded.sort(Comparator.comparingInt(MetricDefinition::version));
                if (!loaded.isEmpty()) {
                    byId.put(id, List.copyOf(loaded));
                    Path ptr = dir.resolve("active.ptr");
                    int active = loaded.get(loaded.size() - 1).version();
                    if (Files.exists(ptr)) {
                        active = mapper.readTree(Files.readAllBytes(ptr)).get("active").asInt();
                    }
                    int a = active;
                    if (loaded.stream().noneMatch(d -> d.version() == a)) {
                        active = loaded.get(loaded.size() - 1).version();
                    }
                    activeVersion.put(id, active);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot scan definitions directory", e);
        }
        return issues;
    }

    @Override
    public synchronized List<MetricDefinition> versions(String metricId) {
        return List.copyOf(byId.getOrDefault(safe(metricId), List.of()));
    }

    @Override
    public synchronized MetricDefinition get(String metricId, int version) {
        return byId.getOrDefault(safe(metricId), List.of()).stream()
                .filter(d -> d.version() == version).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("no definition " + metricId + "@v" + version));
    }

    @Override
    public synchronized MetricDefinition active(String metricId) {
        String id = safe(metricId);
        Integer v = activeVersion.get(id);
        if (v == null) {
            throw new IllegalArgumentException("unknown metric: " + metricId);
        }
        return get(id, v);
    }

    /** One rejected file discovered during startup loading. */
    public record LoadIssue(String file, String metricId,
                            List<DefinitionException.Problem> problems) {
    }

    @Override
    public synchronized List<MetricDefinition> allActive() {
        List<MetricDefinition> out = new ArrayList<>();
        activeVersion.forEach((id, v) -> out.add(get(id, v)));
        out.sort(Comparator.comparing(MetricDefinition::id));
        return out;
    }
}
