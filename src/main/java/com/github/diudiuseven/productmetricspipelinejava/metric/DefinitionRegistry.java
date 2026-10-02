package com.github.diudiuseven.productmetricspipelinejava.metric;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
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
    private final PipelineProperties props;

    public DefinitionRegistry(JdbcTemplate jdbcTemplate, DefinitionValidator validator,
                              DefinitionLoader loader, PipelineProperties props) {
        this.jdbcTemplate = jdbcTemplate;
        this.validator = validator;
        this.loader = loader;
        this.props = props;
    }

    /**
     * Validates and persists a definition. If {@code def.version} is 0, the
     * next version number is assigned; otherwise the given version must not
     * already exist.
     */
    @Transactional
    public MetricDefinition register(MetricDefinition def) {
        validator.validateOrThrow(def);
        int maxVersion = maxVersion(def.getId());
        if (def.getVersion() > 0) {
            if (def.getVersion() <= maxVersion) {
                throw new DefinitionException(List.of(new DefinitionException.FieldError(
                        "version", "CONFLICT",
                        "metric " + def.getId() + " version " + def.getVersion()
                                + " already exists; versions are immutable", def.getVersion())));
            }
        } else {
            def.setVersion(maxVersion + 1);
        }
        if (def.getStatus() == null || def.getStatus().isBlank()) {
            def.setStatus(MetricDefinition.STATUS_ACTIVE);
        }
        def.setCreatedAt(Instant.now());
        jdbcTemplate.update(
                "INSERT INTO metric_def (id, version, spec, status, created_at) VALUES (?, ?, ?, ?, ?)",
                def.getId(), def.getVersion(), loader.toJson(def), def.getStatus(),
                Timestamp.from(def.getCreatedAt()));
        return def;
    }

    public Optional<MetricDefinition> get(String id, int version) {
        List<MetricDefinition> list = jdbcTemplate.query(
                "SELECT spec FROM metric_def WHERE id = ? AND version = ?",
                (rs, n) -> loader.parse(rs.getString("spec")), id, version);
        return list.stream().findFirst();
    }

    public Optional<MetricDefinition> getLatest(String id) {
        List<MetricDefinition> list = jdbcTemplate.query(
                "SELECT spec FROM metric_def WHERE id = ? ORDER BY version DESC LIMIT 1",
                (rs, n) -> loader.parse(rs.getString("spec")), id);
        return list.stream().findFirst();
    }

    public int maxVersion(String id) {
        Integer v = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(version), 0) FROM metric_def WHERE id = ?", Integer.class, id);
        return v == null ? 0 : v;
    }

    public List<MetricDefinition> listAll() {
        return jdbcTemplate.query("SELECT spec FROM metric_def ORDER BY id, version",
                (rs, n) -> loader.parse(rs.getString("spec")));
    }

    public List<MetricDefinition> listActive() {
        return jdbcTemplate.query(
                "SELECT spec FROM metric_def WHERE status = 'ACTIVE' ORDER BY id, version",
                (rs, n) -> loader.parse(rs.getString("spec")));
    }

    /** Loads classpath + directory definitions at startup (idempotent). */
    public void init() {
        List<MetricDefinition> toLoad = new ArrayList<>();
        toLoad.addAll(loader.loadFromClasspath());
        toLoad.addAll(loader.loadFromDirectory(java.nio.file.Path.of(props.getMetricsDir())));
        for (MetricDefinition def : toLoad) {
            if (maxVersion(def.getId()) > 0) {
                continue; // already registered: never override
            }
            if (def.getVersion() == 0) {
                def.setVersion(1);
            }
            try {
                register(def);
            } catch (DefinitionException e) {
                // a definition file is invalid: fail fast with field-level errors
                throw e;
            }
        }
    }
}
