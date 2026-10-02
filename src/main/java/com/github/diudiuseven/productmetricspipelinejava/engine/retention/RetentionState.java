package com.github.diudiuseven.productmetricspipelinejava.engine.retention;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.diudiuseven.productmetricspipelinejava.engine.AggregatorState;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;
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
 * Per-(metric,version) retention state: for each user it keeps the events
 * named by {@code anchorEvent} / {@code retainedEvent}, sorted in total order.
 * Merge semantics are identical to the funnel state (idempotent seq merge,
 * revision-based corrections), keeping results exactly replay-equivalent.
 */
public final class RetentionState extends AggregatorState {

    private final TreeMap<String, List<UserEvent>> rows = new TreeMap<>();
    private final Map<String, EventLocator> eventIndex = new LinkedHashMap<>();
    private long eventCount;

    public RetentionState(MetricDefinition definition) {
        super(definition);
    }

    private record EventLocator(String userId, long seq) {
    }

    private Set<String> relevantNames() {
        Set<String> names = new HashSet<>();
        names.add(definition.anchorEvent());
        names.add(definition.retainedEvent());
        return names;
    }

    @Override
    public void apply(EventRecord r) {
        if (r.seq() <= commitSeq) {
            return;
        }
        EventLocator existing = eventIndex.get(r.eventId());
        if (existing != null) {
            Stored prior = findStored(existing);
            if (prior == null) {
                eventIndex.remove(r.eventId());
            } else if (r.revision() <= prior.event.revision()) {
                if (r.seq() > commitSeq) {
                    commitSeq = r.seq();
                }
                return;
            } else {
                removeAt(existing, prior);
            }
        }
        if (!relevantNames().contains(r.name())) {
            eventIndex.remove(r.eventId());
            if (r.seq() > commitSeq) {
                commitSeq = r.seq();
            }
            return;
        }
        UserEvent ue = new UserEvent(r.seq(), r.eventId(), r.name(), r.eventTime(), r.revision(),
                Map.copyOf(r.dimensions()));
        rows.computeIfAbsent(r.userId(), k -> new ArrayList<>());
        insertSorted(rows.get(r.userId()), ue);
        eventIndex.put(r.eventId(), new EventLocator(r.userId(), r.seq()));
        eventCount++;
        if (r.seq() > commitSeq) {
            commitSeq = r.seq();
        }
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

    @Override
    public int userCount() {
        return rows.size();
    }

    @Override
    public long eventCount() {
        return eventCount;
    }

    /** First anchor event in total order, or null. */
    public UserEvent anchorOf(List<UserEvent> events) {
        for (UserEvent e : events) {
            if (e.name().equals(definition.anchorEvent())) {
                return e;
            }
        }
        return null;
    }

    public List<UserEvent> eventsOf(String userId) {
        return List.copyOf(rows.getOrDefault(userId, List.of()));
    }

    public Iterable<String> userIds() {
        return List.copyOf(rows.keySet());
    }

    public void writeSnapshot(OutputStream raw, com.fasterxml.jackson.databind.ObjectMapper mapper)
            throws IOException {
        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(raw, StandardCharsets.UTF_8));
        ObjectNode header = mapper.createObjectNode();
        header.put("kind", "RETENTION");
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

    public void restoreHeader(JsonNode header) {
        this.commitSeq = header.get("commitSeq").asLong();
        this.eventCount = header.has("eventCount") ? header.get("eventCount").asLong() : 0L;
    }

    public void restoreRow(JsonNode row) {
        String user = row.get("u").asText();
        List<UserEvent> list = new ArrayList<>();
        for (JsonNode e : row.get("e")) {
            Map<String, String> dims = new LinkedHashMap<>();
            if (e.hasNonNull("d")) {
                e.get("d").fields().forEachRemaining(f -> dims.put(f.getKey(), f.getValue().asText()));
            }
            UserEvent ue = new UserEvent(e.get("s").asLong(), e.get("id").asText(), e.get("n").asText(),
                    e.get("t").asLong(), e.path("r").asLong(0), Map.copyOf(dims));
            list.add(ue);
            eventIndex.put(ue.eventId(), new EventLocator(user, ue.seq()));
        }
        list.sort(null);
        rows.put(user, list);
    }
}
