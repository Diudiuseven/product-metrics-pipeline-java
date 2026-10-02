package com.github.diudiuseven.productmetricspipelinejava.metric;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Versioned definition registry. Definitions are persisted; old versions stay
 * ACTIVE and queryable; registering an existing id never overwrites history.
 */
@Service
public class DefinitionRegistry {

    private final JdbcTemplate jdbcTemplate;
    private final DefinitionValidator validator;
    private final DefinitionLoader loader;

    public DefinitionRegistry(JdbcTemplate jdbcTemplate, DefinitionValidator validator, DefinitionLoader loader) {
        this.jdbcTemplate = jdbcTemplate;
        this.validator = validator;
        this.loader = loader;
    }

    /** Validates, assigns the next version, persists. */
    public MetricDefinition register(MetricDefinition def) {
        // TODO implement
        return def;
    }

    public Optional<MetricDefinition> get(String id, int version) {
        // TODO implement
        return Optional.empty();
    }

    public Optional<MetricDefinition> getLatest(String id) {
        // TODO implement
        return Optional.empty();
    }

    public List<MetricDefinition> listAll() {
        // TODO implement
        return List.of();
    }

    public List<MetricDefinition> listActive() {
        // TODO implement
        return List.of();
    }

    /** Loads classpath + directory definitions at startup (idempotent). */
    public void init() {
        // TODO implement
    }
}
