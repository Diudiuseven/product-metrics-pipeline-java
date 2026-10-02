package com.github.diudiuseven.productmetricspipelinejava.engine.funnel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.diudiuseven.productmetricspipelinejava.engine.AggregatorState;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.store.EventRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Per-(metric,version) funnel state.
 *
 * <p>It stores, per user, exactly the metric-relevant events sorted by total
 * order. Every update ({@link #apply}) is an idempotent merge keyed on the
 * global log seq:
 * <ul>
 *   <li>unseen seq/event id → insert;</li>
 *   <li>same event id with a strictly higher {@code revision} → the old
 *       occurrence (possibly under another user) is removed and the corrected
 *       one inserted, which is the "post-hoc correction" path;</li>
 *   <li>same event id with equal/lower revision → ignored (duplicate replay).</li>
 * </ul>
 * Because stored rows are the raw facts, aggregate results are always a pure
 * deterministic function of them — identical to replaying the whole log.
 */
public final class FunnelState extends AggregatorState {

    /** userId -> events kept sorted in total order. */
    private final TreeMap<String, List<UserEvent>> rows = new TreeMap<>();
    /** eventId -> (userId, seq) so corrections find the old occurrence. */
    private final Map<String, EventLocator> eventIndex = new LinkedHashMap<>();
    /** seq -> eventId, supporting log-order identity. */
    private final Map<Long, String> seqIndex = new LinkedHashMap<>();

    private long eventCount;

    public FunnelState(MetricDefinition definition) {
        super(definition);
    }

    private record EventLocator(String userId, long seq) {
    }

    private Set<String> relevantNames() {
        Set<String> names = new HashSet<>();
        definition.steps().forEach(s -> names.add(s.eventName()));
        return names;
    }

    @Override
    public void apply(EventRecord r) {
        if (r.seq() <= commitSeq) {
            return; // already reflected; replay is idempotent
        }
        EventLocator existing = eventIndex.get(r.eventId());
        if (existing != null) {
            Stored prior = findStored(existing);
            if (prior == null) {
                eventIndex.remove(r.eventId()); // index pointed at deleted row
            } else if (r.revision() < prior.event.revision()) {
                advance(r.seq());
                return; // older revision of a corrected event: ignore
            } else if (r.revision() == prior.event.revision()) {
                advance(r.seq());
                return; // duplicate of the same factual event: never recounted
            } else {
                removeAt(existing, prior); // newer revision: erase old fact
            }
        }
        if (!relevantNames().contains(r.name())) {
            // Either a fresh irrelevant event or a correction that moved the
            // fact away from a funnel-relevant name: nothing to store.
            eventIndex.remove(r.eventId());
            advance(r.seq());
            return;
        }
        UserEvent ue = new UserEvent(r.seq(), r.eventId(), r.name(), r.eventTime(), r.revision(),
                Map.copyOf(r.dimensions()));
        List<UserEvent> list = rows.computeIfAbsent(r.userId(), k -> new ArrayList<>());
        insertSorted(list, ue);
        eventIndex.put(r.eventId(), new EventLocator(r.userId(), r.seq()));
        seqIndex.put(r.seq(), r.eventId());
        eventCount++;
        advance(r.seq());
    }

    private record Stored(EventLocator locator, UserEvent event, int indexInList) {
    }

    private Stored findStored(EventLocator locator) {
        List<UserEvent> list = rows.get(locator.userId());
        if (list == null) {
            return null;
        }
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).seq() == locator.seq()) {
                return new Stored(locator, list.get(i), i);
            }
        }
        return null;
    }

    private void removeAt(EventLocator locator, Stored stored) {
        List<UserEvent> list = rows.get(locator.userId());
        list.remove(stored.indexInList());
        if (list.isEmpty()) {
            rows.remove(locator.userId());
        }
        eventCount--;
        seqIndex.remove(locator.seq());
    }

    private static void insertSorted(List<UserEvent> list, UserEvent e) {
        int lo = 0, hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).compareTo(e) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        list.add(lo, e);
    }

    private void advance(long seq) {
        if (seq > commitSeq) {
            commitSeq = seq;
        }
    }

    @Override
    public int userCount() {
        return rows.size();
    }

    @Override
    public long eventCount() {
        return eventCount;
    }

    public List<UserEvent> eventsOf(String userId) {
        return List.copyOf(rows.getOrDefault(userId, List.of()));
    }

    /** Immutable, lexicographically ordered user ids (for snapshot iteration). */
    public Iterable<String> userIds() {
        return List.copyOf(rows.keySet());
    }

    public List<String> dimensionKeys() {
        return definition.dimensionKeys();
    }

    // ---- NDJSON snapshot ------------------------------------------------

    public void writeSnapshot(OutputStream raw, ObjectMapper mapper) throws IOException {
        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(raw, StandardCharsets.UTF_8));
        ObjectNode header = mapper.createObjectNode();
        header.put("kind", "FUNNEL");
        header.put("metricId", definition.id());
        header.put("version", definition.version());
        header.put("commitSeq", commitSeq);
        header.put("userCount", rows.size());
        header.put("eventCount", eventCount);
        w.write(mapper.writeValueAsString(header));
        w.write('\n');
        for (Map.Entry<String, List<UserEvent>> en : rows.entrySet()) {
            ObjectNode row = mapper.createObjectNode();
            row.put("u", en.getKey());
            var arr = row.putArray("e");
            for (UserEvent e : en.getValue()) {
                var o = arr.addObject();
                o.put("s", e.seq());
                o.put("id", e.eventId());
                o.put("n", e.name());
                o.put("t", e.eventTime());
                o.put("r", e.revision());
                var d = o.putObject("d");
                e.dimensions().forEach(d::put);
            }
            w.write(mapper.writeValueAsString(row));
            w.write('\n');
        }
        w.flush();
    }

    /** Read the header line, seeding commitSeq. Returns nothing. */
    public void restoreHeader(JsonNode header) {
        this.commitSeq = header.get("commitSeq").asLong();
        long ec = header.has("eventCount") ? header.get("eventCount").asLong() : 0L;
        this.eventCount = ec;
    }

    /** Replay one NDJSON per-user line back into state (used by open()). */
    public void restoreRow(JsonNode row) {
        String user = row.get("u").asText();
        List<UserEvent> list = new ArrayList<>();
        for (JsonNode e : row.get("e")) {
            Map<String, String> dims = new LinkedHashMap<>();
            if (e.hasNonNull("d")) {
                e.get("d").fields().forEachRemaining(f -> dims.put(f.getKey(), f.getValue().asText()));
            }
            String eid = e.get("id").asText();
            UserEvent ue = new UserEvent(e.get("s").asLong(), eid, e.get("n").asText(),
                    e.get("t").asLong(), e.path("r").asLong(0), Map.copyOf(dims));
            list.add(ue);
            seqIndex.put(ue.seq(), eid);
        }
        list.sort(null);
        rows.put(user, list);
        for (UserEvent ue : list) {
            eventIndex.put(ue.eventId(), new EventLocator(user, ue.seq()));
        }
    }
}
