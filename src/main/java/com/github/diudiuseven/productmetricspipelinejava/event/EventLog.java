package com.github.diudiuseven.productmetricspipelinejava.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Append-only durable event log. The log is the source of truth: builders
 * never mutate it, they only consume it.
 *
 * <p>Idempotency: identical re-PUTs (same eventId + same payload) return the
 * existing sequence; changed payloads append a CORRECT record; DELETE appends
 * a tombstone. Replay always takes the latest record per event id.
 */
@Repository
public class EventLog {

    private static final TypeReference<Map<String, String>> DIMS_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public EventLog(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    private record Existing(long logSeq, String payloadHash, EventOp op) {
    }

    /**
     * Appends an event idempotently. Returns the log sequence of the resulting
     * record (existing one for duplicate PUTs).
     */
    @Transactional
    public long append(Event event) {
        String hash = hash(event);
        Existing existing = findExisting(event.eventId()).orElse(null);
        if (existing != null && existing.op() == EventOp.PUT
                && existing.payloadHash().equals(hash) && event.op() == EventOp.PUT) {
            return existing.logSeq();
        }
        EventOp op = event.op() == null ? EventOp.PUT : event.op();
        if (existing != null && op == EventOp.PUT) {
            // changed payload -> correction
            op = EventOp.CORRECT;
        }
        // DELETE records get a unique sentinel hash so the (event_id, payload_hash)
        // uniqueness never rejects a tombstone.
        String storedHash = op == EventOp.DELETE
                ? hash + ":DELETE:" + java.util.UUID.randomUUID()
                : hash;
        try {
            jdbcTemplate.update(
                    "INSERT INTO event_log (event_id, user_id, event_type, event_time, receive_time, dims, op, payload_hash, prev_seq) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    event.eventId(), event.userId(), event.eventType(),
                    Timestamp.from(event.eventTime()), Timestamp.from(event.receiveTime()),
                    writeDims(event.dims()), op.name(), storedHash,
                    existing == null ? null : existing.logSeq());
        } catch (DuplicateKeyException dup) {
            // concurrent append of the same event id: retry as idempotent no-op
            Existing retry = findExisting(event.eventId()).orElse(null);
            if (retry != null && retry.op() == EventOp.PUT && retry.payloadHash().equals(hash)) {
                return retry.logSeq();
            }
            throw dup;
        }
        Long seq = jdbcTemplate.queryForObject("SELECT MAX(log_seq) FROM event_log WHERE event_id = ?",
                Long.class, event.eventId());
        return seq == null ? 0L : seq;
    }

    public List<StoredEvent> findAfterSeq(long seq, int limit) {
        return jdbcTemplate.query(
                "SELECT * FROM event_log WHERE log_seq > ? ORDER BY log_seq LIMIT ?",
                rowMapper, seq, limit);
    }

    /**
     * All active (latest-op-wins, non-deleted) events for a user, by event time.
     *
     * @deprecated build paths must use {@link #findActiveByUserUpTo} so a
     *             published snapshot stays an exact function of the committed
     *             prefix; this unbounded read is kept for diagnostics only.
     */
    @Deprecated
    public List<StoredEvent> findActiveByUser(String userId) {
        return jdbcTemplate.query(
                "SELECT e.* FROM event_log e "
                        + "WHERE e.user_id = ? AND e.op <> 'DELETE' "
                        + "AND e.log_seq = (SELECT MAX(e2.log_seq) FROM event_log e2 WHERE e2.event_id = e.event_id) "
                        + "ORDER BY e.event_time, e.log_seq",
                rowMapper, userId);
    }

    /**
     * Active events for a user considering only records with {@code log_seq <= seq}.
     * Latest-op-wins is also bounded: a correction beyond {@code seq} does not
     * hide the original record. Makes a snapshot an exact function of the
     * committed prefix [1, seq].
     */
    public List<StoredEvent> findActiveByUserUpTo(String userId, long seq) {
        return jdbcTemplate.query(
                "SELECT e.* FROM event_log e "
                        + "WHERE e.user_id = ? AND e.op <> 'DELETE' AND e.log_seq <= ? "
                        + "AND e.log_seq = (SELECT MAX(e2.log_seq) FROM event_log e2 "
                        + "                 WHERE e2.event_id = e.event_id AND e2.log_seq <= ?) "
                        + "ORDER BY e.event_time, e.log_seq",
                rowMapper, userId, seq, seq);
    }

    /** All active events ordered by (event_time, log_seq) — full replay input. */
    public List<StoredEvent> findAllActiveOrdered() {
        return jdbcTemplate.query(
                "SELECT e.* FROM event_log e "
                        + "WHERE e.op <> 'DELETE' "
                        + "AND e.log_seq = (SELECT MAX(e2.log_seq) FROM event_log e2 WHERE e2.event_id = e.event_id) "
                        + "ORDER BY e.event_time, e.log_seq",
                rowMapper);
    }

    /**
     * All active events with {@code log_seq <= seq} — replay input bounded to a
     * committed prefix. Corrections/deletes beyond {@code seq} are invisible.
     */
    public List<StoredEvent> findAllActiveOrderedUpTo(long seq) {
        return jdbcTemplate.query(
                "SELECT e.* FROM event_log e "
                        + "WHERE e.op <> 'DELETE' AND e.log_seq <= ? "
                        + "AND e.log_seq = (SELECT MAX(e2.log_seq) FROM event_log e2 "
                        + "                 WHERE e2.event_id = e.event_id AND e2.log_seq <= ?) "
                        + "ORDER BY e.event_time, e.log_seq",
                rowMapper, seq, seq);
    }

    public long maxSeq() {
        Long v = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(log_seq), 0) FROM event_log", Long.class);
        return v == null ? 0L : v;
    }

    public long countActive() {
        Long v = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM event_log e WHERE e.op <> 'DELETE' "
                        + "AND e.log_seq = (SELECT MAX(e2.log_seq) FROM event_log e2 WHERE e2.event_id = e.event_id)",
                Long.class);
        return v == null ? 0L : v;
    }

    public Optional<StoredEvent> findByEventId(String eventId) {
        List<StoredEvent> list = jdbcTemplate.query(
                "SELECT * FROM event_log WHERE event_id = ? ORDER BY log_seq DESC LIMIT 1",
                rowMapper, eventId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    private Optional<Existing> findExisting(String eventId) {
        List<Existing> list = jdbcTemplate.query(
                "SELECT log_seq, payload_hash, op FROM event_log WHERE event_id = ? ORDER BY log_seq DESC LIMIT 1",
                (rs, n) -> new Existing(rs.getLong("log_seq"), rs.getString("payload_hash"),
                        EventOp.valueOf(rs.getString("op"))),
                eventId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    private final RowMapper<StoredEvent> rowMapper = (rs, n) -> {
        Map<String, String> dims = readDims(rs.getString("dims"));
        Event event = new Event(
                rs.getString("event_id"),
                rs.getString("user_id"),
                rs.getString("event_type"),
                rs.getTimestamp("event_time").toInstant(),
                rs.getTimestamp("receive_time").toInstant(),
                dims,
                EventOp.valueOf(rs.getString("op")));
        return new StoredEvent(rs.getLong("log_seq"), event);
    };

    private String writeDims(Map<String, String> dims) {
        try {
            return objectMapper.writeValueAsString(dims == null ? Map.of() : dims);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot serialize dims", e);
        }
    }

    private Map<String, String> readDims(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(json, DIMS_TYPE);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse dims", e);
        }
    }

    static String hash(Event event) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String raw = event.userId() + "|" + event.eventType() + "|" + event.eventTime() + "|"
                    + (event.dims() == null ? "" : event.dims().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .reduce((a, b) -> a + "," + b).orElse(""));
            return HexFormat.of().formatHex(md.digest(raw.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
