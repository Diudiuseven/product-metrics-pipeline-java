package com.github.diudiuseven.productmetricspipelinejava.event;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Append-only durable event log. The log is the source of truth: builders
 * never mutate it, they only consume it.
 */
@Repository
public class EventLog {

    private final JdbcTemplate jdbcTemplate;

    public EventLog(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Appends an event idempotently. Identical re-PUTs return the existing
     * sequence; changed payloads append a CORRECT record; DELETE appends a
     * tombstone.
     */
    public long append(Event event) {
        // TODO implement in next step
        return 0L;
    }

    /** Events with logSeq greater than {@code seq}, in log order. */
    public List<StoredEvent> findAfterSeq(long seq, int limit) {
        // TODO implement
        return List.of();
    }

    /** All active (latest-op-wins) events for a user, ordered by event time. */
    public List<StoredEvent> findActiveByUser(String userId) {
        // TODO implement
        return List.of();
    }

    /** All active events ordered by (event_time, log_seq) — full replay input. */
    public List<StoredEvent> findAllActiveOrdered() {
        // TODO implement
        return List.of();
    }

    public long maxSeq() {
        Long v = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(log_seq), 0) FROM event_log", Long.class);
        return v == null ? 0L : v;
    }

    public long countActive() {
        // TODO implement
        return 0L;
    }

    public Optional<StoredEvent> findByEventId(String eventId) {
        // TODO implement
        return Optional.empty();
    }
}
