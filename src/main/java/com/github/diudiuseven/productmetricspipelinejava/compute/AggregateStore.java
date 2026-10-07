package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Transactional access to snapshots, checkpoints and recompute tasks.
 * Every publish is a single transaction: readers never see a mixed state.
 */
@Repository
public class AggregateStore {

    public record CommitRecord(long commitId, String metricId, int version, long logSeqFrom,
                               long logSeqTo, long eventCount, long affectedUsers,
                               String trigger, String status, String summary, Instant createdAt) {
    }

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public AggregateStore(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    // ---------------- checkpoint ----------------

    public Optional<BuilderCheckpoint> loadCheckpoint(String metricId, int version) {
        List<BuilderCheckpoint> list = jdbcTemplate.query(
                "SELECT * FROM builder_checkpoint WHERE metric_id = ? AND version = ?",
                (rs, n) -> new BuilderCheckpoint(rs.getString("metric_id"), rs.getInt("version"),
                        rs.getLong("log_seq"), rs.getLong("last_commit_id"),
                        rs.getTimestamp("updated_at").toInstant()),
                metricId, version);
        return list.stream().findFirst();
    }

    public void saveCheckpoint(BuilderCheckpoint cp) {
        jdbcTemplate.update(
                "MERGE INTO builder_checkpoint (metric_id, version, log_seq, last_commit_id, updated_at) "
                        + "KEY(metric_id, version) VALUES (?, ?, ?, ?, ?)",
                cp.metricId(), cp.version(), cp.logSeq(), cp.lastCommitId(),
                Timestamp.from(cp.updatedAt()));
    }

    // ---------------- current snapshot reads ----------------

    public Map<String, FunnelAggregate> loadCurrentFunnel(String metricId, int version) {
        Map<String, FunnelAggregate> map = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT * FROM funnel_agg WHERE metric_id = ? AND version = ?",
                (RowMapper<Void>) (rs, n) -> {
                    FunnelAggregate a = mapFunnelAgg(rs);
                    map.put(a.bucket() + "\u0000" + a.dims(), a);
                    return null;
                }, metricId, version);
        return map;
    }

    public Map<String, RetentionAggregate> loadCurrentRetention(String metricId, int version) {
        Map<String, RetentionAggregate> map = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT * FROM retention_agg WHERE metric_id = ? AND version = ?",
                (RowMapper<Void>) (rs, n) -> {
                    RetentionAggregate a = mapRetentionAgg(rs);
                    map.put(a.cohort() + "\u0000" + a.dims(), a);
                    return null;
                }, metricId, version);
        return map;
    }

    /** Distinct dimension tuples currently materialised as detail groups. */
    public java.util.Set<String> loadCurrentFunnelDims(String metricId, int version) {
        return new java.util.TreeSet<>(jdbcTemplate.queryForList(
                "SELECT DISTINCT dims FROM funnel_agg WHERE metric_id = ? AND version = ?",
                String.class, metricId, version));
    }

    /** Distinct dimension tuples currently materialised as detail groups. */
    public java.util.Set<String> loadCurrentRetentionDims(String metricId, int version) {
        return new java.util.TreeSet<>(jdbcTemplate.queryForList(
                "SELECT DISTINCT dims FROM retention_agg WHERE metric_id = ? AND version = ?",
                String.class, metricId, version));
    }

    public Map<String, FunnelContribution> loadFunnelUsers(String metricId, int version,
                                                            Collection<String> userIds) {
        Map<String, FunnelContribution> map = new LinkedHashMap<>();
        if (userIds.isEmpty()) {
            return map;
        }        // H2 does not support array parameters; batch via IN with placeholders
        List<String> ids = List.copyOf(userIds);
        int batch = 500;
        for (int i = 0; i < ids.size(); i += batch) {
            List<String> part = ids.subList(i, Math.min(i + batch, ids.size()));
            String placeholders = String.join(",", java.util.Collections.nCopies(part.size(), "?"));
            jdbcTemplate.query("SELECT * FROM funnel_user WHERE metric_id = ? AND version = ? AND user_id IN ("
                            + placeholders + ")",
                    (RowMapper<Void>) (rs, n) -> {
                        map.put(rs.getString("user_id"), new FunnelContribution(
                                rs.getString("bucket"), rs.getString("dims"),
                                rs.getInt("step_mask"),
                                FunnelVerdict.valueOf(rs.getString("verdict"))));
                        return null;
                    }, buildArgs(metricId, version, part));
        }
        return map;
    }

    public Map<String, RetentionContribution> loadRetentionUsers(String metricId, int version,
                                                                Collection<String> userIds) {
        Map<String, RetentionContribution> map = new LinkedHashMap<>();
        if (userIds.isEmpty()) {
            return map;
        }
        List<String> ids = List.copyOf(userIds);
        int batch = 500;
        for (int i = 0; i < ids.size(); i += batch) {
            List<String> part = ids.subList(i, Math.min(i + batch, ids.size()));
            String placeholders = String.join(",", java.util.Collections.nCopies(part.size(), "?"));
            jdbcTemplate.query("SELECT * FROM retention_user WHERE metric_id = ? AND version = ? AND user_id IN ("
                            + placeholders + ")",
                    (RowMapper<Void>) (rs, n) -> {
                        map.put(rs.getString("user_id"), new RetentionContribution(
                                rs.getString("cohort"), rs.getString("dims"), rs.getInt("mask")));
                        return null;
                    }, buildArgs(metricId, version, part));
        }
        return map;
    }

    /** All committed per-user funnel contributions (verification only; callers
     *  must bound input size via pipeline.consistency-max-events first). */
    public Map<String, FunnelContribution> loadAllFunnelUsers(String metricId, int version) {
        Map<String, FunnelContribution> map = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT * FROM funnel_user WHERE metric_id = ? AND version = ?",
                (RowMapper<Void>) (rs, n) -> {
                    map.put(rs.getString("user_id"), new FunnelContribution(
                            rs.getString("bucket"), rs.getString("dims"), rs.getInt("step_mask"),
                            FunnelVerdict.valueOf(rs.getString("verdict"))));
                    return null;
                }, metricId, version);
        return map;
    }

    /** All committed per-user retention contributions (verification only). */
    public Map<String, RetentionContribution> loadAllRetentionUsers(String metricId, int version) {
        Map<String, RetentionContribution> map = new LinkedHashMap<>();
        jdbcTemplate.query("SELECT * FROM retention_user WHERE metric_id = ? AND version = ?",
                (RowMapper<Void>) (rs, n) -> {
                    map.put(rs.getString("user_id"), new RetentionContribution(
                            rs.getString("cohort"), rs.getString("dims"), rs.getInt("mask")));
                    return null;
                }, metricId, version);
        return map;
    }

    // ---------------- consistency verification reports ----------------

    public long saveVerificationReport(String metricId, int version, long logSeq,
                                       VerificationStatus status, String diffs, Long commitId) {
        jdbcTemplate.update(
                "INSERT INTO verification_report (metric_id, version, checked_at, log_seq, status, diffs, commit_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                metricId, version, Timestamp.from(Instant.now()), logSeq, status.name(), diffs, commitId);
        Long id = jdbcTemplate.queryForObject("SELECT MAX(report_id) FROM verification_report", Long.class);
        return id == null ? 0L : id;
    }

    public List<VerificationReport> listVerificationReports(String metricId, int version, int limit) {
        String sql = "SELECT * FROM verification_report WHERE metric_id = ? "
                + (version > 0 ? "AND version = ? " : "")
                + "ORDER BY report_id DESC LIMIT ?";
        Object[] args = version > 0 ? new Object[]{metricId, version, limit} : new Object[]{metricId, limit};
        return jdbcTemplate.query(sql, (rs, n) -> new VerificationReport(
                rs.getLong("report_id"), rs.getString("metric_id"), rs.getInt("version"),
                rs.getTimestamp("checked_at").toInstant(), rs.getLong("log_seq"),
                VerificationStatus.valueOf(rs.getString("status")),
                rs.getString("diffs"),
                rs.getObject("commit_id") == null ? null : rs.getLong("commit_id")), args);
    }

    /** Latest verification status per metric, for health surfacing. */
    public Optional<VerificationReport> latestVerification(String metricId, int version) {
        return listVerificationReports(metricId, version, 1).stream().findFirst();
    }

    public void deleteFunnelUsers(String metricId, int version, Collection<String> userIds) {
        if (userIds.isEmpty()) {
            return;
        }
        List<String> ids = List.copyOf(userIds);
        for (int i = 0; i < ids.size(); i += 500) {
            List<String> part = ids.subList(i, Math.min(i + 500, ids.size()));
            String placeholders = String.join(",", java.util.Collections.nCopies(part.size(), "?"));
            Object[] args = buildArgs(metricId, version, part);
            jdbcTemplate.update("DELETE FROM funnel_user WHERE metric_id = ? AND version = ? AND user_id IN ("
                    + placeholders + ")", args);
        }
    }

    public void deleteRetentionUsers(String metricId, int version, Collection<String> userIds) {
        if (userIds.isEmpty()) {
            return;
        }
        List<String> ids = List.copyOf(userIds);
        for (int i = 0; i < ids.size(); i += 500) {
            List<String> part = ids.subList(i, Math.min(i + 500, ids.size()));
            String placeholders = String.join(",", java.util.Collections.nCopies(part.size(), "?"));
            Object[] args = buildArgs(metricId, version, part);
            jdbcTemplate.update("DELETE FROM retention_user WHERE metric_id = ? AND version = ? AND user_id IN ("
                    + placeholders + ")", args);
        }
    }

    // ---------------- commits ----------------

    public long insertCommit(CommitRecord commit) {
        jdbcTemplate.update(
                "INSERT INTO snapshot_commit (metric_id, version, log_seq_from, log_seq_to, event_count, "
                        + "affected_users, trigger, status, summary, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                commit.metricId(), commit.version(), commit.logSeqFrom(), commit.logSeqTo(),
                commit.eventCount(), commit.affectedUsers(), commit.trigger(), commit.status(),
                commit.summary(), Timestamp.from(commit.createdAt()));
        Long id = jdbcTemplate.queryForObject("SELECT MAX(commit_id) FROM snapshot_commit", Long.class);
        return id == null ? 0L : id;
    }

    public List<CommitRecord> listCommits(String metricId, int version, int limit) {
        String sql = "SELECT * FROM snapshot_commit WHERE metric_id = ? "
                + (version > 0 ? "AND version = ? " : "")
                + "ORDER BY commit_id DESC LIMIT ?";
        Object[] args = version > 0 ? new Object[]{metricId, version, limit} : new Object[]{metricId, limit};
        return jdbcTemplate.query(sql, (rs, n) -> new CommitRecord(
                rs.getLong("commit_id"), rs.getString("metric_id"), rs.getInt("version"),
                rs.getLong("log_seq_from"), rs.getLong("log_seq_to"), rs.getLong("event_count"),
                rs.getLong("affected_users"), rs.getString("trigger"), rs.getString("status"),
                rs.getString("summary"), rs.getTimestamp("created_at").toInstant()), args);
    }

    // ---------------- recompute tasks ----------------

    public long createRecomputeTask(String metricId, int version, long fromSeq, long toSeq) {
        jdbcTemplate.update(
                "INSERT INTO recompute_task (metric_id, version, log_seq_from, log_seq_to, status, started_at) "
                        + "VALUES (?, ?, ?, ?, 'RUNNING', ?)",
                metricId, version, fromSeq, toSeq, Timestamp.from(Instant.now()));
        Long id = jdbcTemplate.queryForObject("SELECT MAX(task_id) FROM recompute_task", Long.class);
        return id == null ? 0L : id;
    }

    public void finishRecomputeTask(long taskId, RecomputeTaskStatus status, String note) {
        jdbcTemplate.update(
                "UPDATE recompute_task SET status = ?, finished_at = ?, note = ? WHERE task_id = ?",
                status.name(), Timestamp.from(Instant.now()), note, taskId);
    }

    public List<Long> findRunningTasks() {
        return jdbcTemplate.queryForList(
                "SELECT task_id FROM recompute_task WHERE status = 'RUNNING'", Long.class);
    }

    public void deleteScratch(long taskId) {
        jdbcTemplate.update("DELETE FROM funnel_agg_scratch WHERE task_id = ?", taskId);
        jdbcTemplate.update("DELETE FROM funnel_user_scratch WHERE task_id = ?", taskId);
        jdbcTemplate.update("DELETE FROM retention_agg_scratch WHERE task_id = ?", taskId);
        jdbcTemplate.update("DELETE FROM retention_user_scratch WHERE task_id = ?", taskId);
    }

    // ---------------- publish ----------------

    /**
     * Publishes an incremental snapshot in one transaction: upserts the
     * changed aggregate rows and per-user contributions, deletes tombstoned
     * user rows, inserts the commit row and advances the checkpoint. Readers
     * therefore only ever see a self-consistent committed whole.
     */
    @Transactional
    public long publishIncremental(MetricDefinition def, long fromSeq, long toSeq, long eventCount,
                                   Collection<FunnelAggregate> funnelAggUpserts,
                                   Collection<RetentionAggregate> retentionAggUpserts,
                                   Map<String, FunnelContribution> funnelUserUpserts,
                                   Map<String, RetentionContribution> retentionUserUpserts,
                                   Collection<String> deletedFunnelUsers,
                                   Collection<String> deletedRetentionUsers,
                                   long affectedUsers, String trigger) {
        long commitId = insertCommit(new CommitRecord(0, def.getId(), def.getVersion(), fromSeq, toSeq,
                eventCount, affectedUsers, trigger, "COMMITTED", null, Instant.now()));
        for (FunnelAggregate a : funnelAggUpserts) {
            jdbcTemplate.update(
                    "MERGE INTO funnel_agg (metric_id, version, bucket, dims, entered, converted, skipped, "
                            + "missing, out_of_window, pending, step_counts) KEY(metric_id, version, bucket, dims) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    def.getId(), def.getVersion(), a.bucket(), a.dims(), a.entered(), a.converted(),
                    a.skipped(), a.missing(), a.outOfWindow(), a.pending(), writeLongs(a.stepCounts()));
        }
        for (Map.Entry<String, FunnelContribution> e : funnelUserUpserts.entrySet()) {
            FunnelContribution c = e.getValue();
            jdbcTemplate.update(
                    "MERGE INTO funnel_user (metric_id, version, user_id, bucket, dims, step_mask, verdict) "
                            + "KEY(metric_id, version, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    def.getId(), def.getVersion(), e.getKey(), c.bucket(), c.dims(), c.stepMask(),
                    c.verdict().name());
        }
        for (RetentionAggregate a : retentionAggUpserts) {
            jdbcTemplate.update(
                    "MERGE INTO retention_agg (metric_id, version, cohort, dims, cohort_size, retained) "
                            + "KEY(metric_id, version, cohort, dims) VALUES (?, ?, ?, ?, ?, ?)",
                    def.getId(), def.getVersion(), a.cohort(), a.dims(), a.cohortSize(),
                    writeLongs(a.retained()));
        }
        for (Map.Entry<String, RetentionContribution> e : retentionUserUpserts.entrySet()) {
            RetentionContribution c = e.getValue();
            jdbcTemplate.update(
                    "MERGE INTO retention_user (metric_id, version, user_id, cohort, dims, mask) "
                            + "KEY(metric_id, version, user_id) VALUES (?, ?, ?, ?, ?, ?)",
                    def.getId(), def.getVersion(), e.getKey(), c.cohort(), c.dims(), c.mask());
        }
        deleteFunnelUsers(def.getId(), def.getVersion(), deletedFunnelUsers);
        deleteRetentionUsers(def.getId(), def.getVersion(), deletedRetentionUsers);
        saveCheckpoint(new BuilderCheckpoint(def.getId(), def.getVersion(), toSeq, commitId, Instant.now()));
        return commitId;
    }

    /**
     * Publishes a recompute in one transaction: replaces current rows with the
     * scratch rows for the task, inserts the commit row, advances the
     * checkpoint, drops scratch and marks the task DONE.
     */
    @Transactional
    public long publishRecompute(MetricDefinition def, long taskId, long fromSeq, long toSeq,
                                 long eventCount, ReplayResult result, String trigger) {
        long commitId = insertCommit(new CommitRecord(0, def.getId(), def.getVersion(), fromSeq, toSeq,
                eventCount, result.funnelUsers().size() + result.retentionUsers().size(),
                trigger, "COMMITTED", null, Instant.now()));
        jdbcTemplate.update("DELETE FROM funnel_agg WHERE metric_id = ? AND version = ?",
                def.getId(), def.getVersion());
        jdbcTemplate.update("DELETE FROM funnel_user WHERE metric_id = ? AND version = ?",
                def.getId(), def.getVersion());
        jdbcTemplate.update("DELETE FROM retention_agg WHERE metric_id = ? AND version = ?",
                def.getId(), def.getVersion());
        jdbcTemplate.update("DELETE FROM retention_user WHERE metric_id = ? AND version = ?",
                def.getId(), def.getVersion());
        jdbcTemplate.update(
                "INSERT INTO funnel_agg (metric_id, version, bucket, dims, entered, converted, skipped, "
                        + "missing, out_of_window, pending, step_counts) "
                        + "SELECT ?, ?, bucket, dims, entered, converted, skipped, missing, out_of_window, "
                        + "pending, step_counts FROM funnel_agg_scratch WHERE task_id = ?",
                def.getId(), def.getVersion(), taskId);
        jdbcTemplate.update(
                "INSERT INTO funnel_user (metric_id, version, user_id, bucket, dims, step_mask, verdict) "
                        + "SELECT ?, ?, user_id, bucket, dims, step_mask, verdict FROM funnel_user_scratch "
                        + "WHERE task_id = ?", def.getId(), def.getVersion(), taskId);
        jdbcTemplate.update(
                "INSERT INTO retention_agg (metric_id, version, cohort, dims, cohort_size, retained) "
                        + "SELECT ?, ?, cohort, dims, cohort_size, retained FROM retention_agg_scratch "
                        + "WHERE task_id = ?", def.getId(), def.getVersion(), taskId);
        jdbcTemplate.update(
                "INSERT INTO retention_user (metric_id, version, user_id, cohort, dims, mask) "
                        + "SELECT ?, ?, user_id, cohort, dims, mask FROM retention_user_scratch WHERE task_id = ?",
                def.getId(), def.getVersion(), taskId);
        deleteScratch(taskId);
        finishRecomputeTask(taskId, RecomputeTaskStatus.DONE, "published commit " + commitId);
        saveCheckpoint(new BuilderCheckpoint(def.getId(), def.getVersion(), toSeq, commitId, Instant.now()));
        return commitId;
    }

    // ---------------- scratch writes (used by RecomputeService) ----------------

    public void writeFunnelScratch(long taskId, String userId, FunnelContribution c) {
        jdbcTemplate.update(
                "MERGE INTO funnel_user_scratch (task_id, user_id, bucket, dims, step_mask, verdict) "
                        + "KEY(task_id, user_id) VALUES (?, ?, ?, ?, ?, ?)",
                taskId, userId, c.bucket(), c.dims(), c.stepMask(), c.verdict().name());
    }

    public void writeRetentionScratch(long taskId, String userId, RetentionContribution c) {
        jdbcTemplate.update(
                "MERGE INTO retention_user_scratch (task_id, user_id, cohort, dims, mask) "
                        + "KEY(task_id, user_id) VALUES (?, ?, ?, ?, ?)",
                taskId, userId, c.cohort(), c.dims(), c.mask());
    }

    public void writeFunnelAggScratch(long taskId, FunnelAggregate a) {
        jdbcTemplate.update(
                "MERGE INTO funnel_agg_scratch (task_id, bucket, dims, entered, converted, skipped, missing, "
                        + "out_of_window, pending, step_counts) KEY(task_id, bucket, dims) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                taskId, a.bucket(), a.dims(), a.entered(), a.converted(), a.skipped(), a.missing(),
                a.outOfWindow(), a.pending(), writeLongs(a.stepCounts()));
    }

    public void writeRetentionAggScratch(long taskId, RetentionAggregate a) {
        jdbcTemplate.update(
                "MERGE INTO retention_agg_scratch (task_id, cohort, dims, cohort_size, retained) "
                        + "KEY(task_id, cohort, dims) VALUES (?, ?, ?, ?, ?)",
                taskId, a.cohort(), a.dims(), a.cohortSize(), writeLongs(a.retained()));
    }

    // ---------------- mappers / helpers ----------------

    private FunnelAggregate mapFunnelAgg(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new FunnelAggregate(rs.getString("bucket"), rs.getString("dims"),
                rs.getLong("entered"), rs.getLong("converted"), rs.getLong("skipped"),
                rs.getLong("missing"), rs.getLong("out_of_window"), rs.getLong("pending"),
                readLongs(rs.getString("step_counts")));
    }

    private RetentionAggregate mapRetentionAgg(java.sql.ResultSet rs) throws java.sql.SQLException {
        return new RetentionAggregate(rs.getString("cohort"), rs.getString("dims"),
                rs.getLong("cohort_size"), readLongs(rs.getString("retained")));
    }

    private static Object[] buildArgs(String metricId, int version, List<String> part) {
        Object[] args = new Object[part.size() + 2];
        args[0] = metricId;
        args[1] = version;
        for (int i = 0; i < part.size(); i++) {
            args[i + 2] = part.get(i);
        }
        return args;
    }

    private String writeLongs(long[] values) {
        try {
            return objectMapper.writeValueAsString(values == null ? new long[0] : values);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long[] readLongs(String json) {
        if (json == null || json.isBlank()) {
            return new long[0];
        }
        try {
            return objectMapper.readValue(json, long[].class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
