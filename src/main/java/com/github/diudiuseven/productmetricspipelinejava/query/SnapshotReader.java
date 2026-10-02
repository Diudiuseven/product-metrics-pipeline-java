package com.github.diudiuseven.productmetricspipelinejava.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;
import com.github.diudiuseven.productmetricspipelinejava.store.Checkpoint;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Forward-only, bounded-memory reader over a committed NDJSON snapshot.
 *
 * <p>A cursor is the 0-based row ordinal to continue from (encoded as a string
 * in {@link Page#nextFrom()}). Resuming a page therefore costs a single
 * sequential scan that skips rows by ordinal — never a full deserialisation of
 * earlier pages and never materialising the whole snapshot into memory.
 */
public final class SnapshotReader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path file;

    public SnapshotReader(SnapshotStore store, Checkpoint cp) {
        this.file = store.resolve(cp);
    }

    public JsonNode header() {
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return MAPPER.readTree(r.readLine());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read snapshot header", e);
        }
    }

    /**
     * Stream user rows starting at ordinal {@code startRow}. Only rows passing
     * {@code filter} are visited. {@code deadlineCheck} is invoked per row so a
     * timeout/cancellation aborts before any partial page is produced.
     */
    public void scanFrom(long startRow, Predicate<? super ParsedUser> filter, RowVisitor visitor,
                         Runnable deadlineCheck) {
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            r.readLine(); // header
            long ordinal = 0;
            String line;
            while ((line = r.readLine()) != null) {
                long here = ordinal++;
                if (here < startRow || line.isEmpty()) {
                    continue;
                }
                deadlineCheck.run();
                ParsedUser u = parse(MAPPER.readTree(line));
                if (filter.test(u) && !visitor.accept(u, here)) {
                    return;
                }
            }
        } catch (QueryDeadlineException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot scan failed", e);
        }
    }

    private static ParsedUser parse(JsonNode row) {
        String user = row.get("u").asText();
        List<UserEvent> events = new ArrayList<>();
        for (JsonNode e : row.get("e")) {
            Map<String, String> dims = new LinkedHashMap<>();
            if (e.hasNonNull("d")) {
                e.get("d").fields().forEachRemaining(f -> dims.put(f.getKey(), f.getValue().asText()));
            }
            events.add(new UserEvent(e.get("s").asLong(), e.get("id").asText(), e.get("n").asText(),
                    e.get("t").asLong(), e.path("r").asLong(0), Map.copyOf(dims)));
        }
        return new ParsedUser(user, events);
    }

    @FunctionalInterface
    public interface RowVisitor {
        /** @return false to stop the scan. */
        boolean accept(ParsedUser user, long ordinal);
    }
}
