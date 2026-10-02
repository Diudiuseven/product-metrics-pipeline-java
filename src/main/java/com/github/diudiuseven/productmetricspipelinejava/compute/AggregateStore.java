package com.github.diudiuseven.productmetricspipelinejava.compute;

import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
                               String trigger, String status, String summary, java.time.Instant createdAt) {
    }

    private final JdbcTemplate jdbcTemplate;

    public AggregateStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<BuilderCheckpoint> loadCheckpoint(String metricId, int version) {
        // TODO implement
        return Optional.empty();
    }

    public void saveCheckpoint(BuilderCheckpoint cp) {
        // TODO implement
    }

    public Map<String, FunnelAggregate> loadCurrentFunnel(String metricId, int version) {
        // TODO implement
        return Map.of();
    }

    public Map<String, RetentionAggregate> loadCurrentRetention(String metricId, int version) {
        // TODO implement
        return Map.of();
    }

    public Map<String, FunnelContribution> loadCurrentFunnelUsers(String metricId, int version) {
        // TODO implement
        return Map.of();
    }

    public Map<String, RetentionContribution> loadCurrentRetentionUsers(String metricId, int version) {
        // TODO implement
        return Map.of();
    }

    /** Inserts a commit row and returns its id. */
    public long insertCommit(CommitRecord commit) {
        // TODO implement
        return 0L;
    }

    public List<CommitRecord> listCommits(String metricId, int version, int limit) {
        // TODO implement
        return List.of();
    }

    public long createRecomputeTask(String metricId, int version, long fromSeq, long toSeq) {
        // TODO implement
        return 0L;
    }

    public void finishRecomputeTask(long taskId, RecomputeTaskStatus status, String note) {
        // TODO implement
    }

    public List<Long> findRunningTasks() {
        // TODO implement
        return List.of();
    }

    public void deleteScratch(long taskId) {
        // TODO implement
    }

    /**
     * Publishes an incremental snapshot: applies per-user diffs to the current
     * tables, inserts the commit row and updates the checkpoint — one transaction.
     */
    public long publishIncremental(MetricDefinition def, long fromSeq, long toSeq,
                                   Map<String, FunnelAggregate> funnelAgg,
                                   Map<String, RetentionAggregate> retentionAgg,
                                   Map<String, FunnelContribution> funnelUsers,
                                   Map<String, RetentionContribution> retentionUsers,
                                   String trigger) {
        // TODO implement
        return 0L;
    }

    /**
     * Publishes a recompute: atomically replaces current rows with scratch rows
     * and inserts the commit row.
     */
    public long publishRecompute(MetricDefinition def, long taskId, long fromSeq, long toSeq,
                                 ReplayResult result, String trigger) {
        // TODO implement
        return 0L;
    }
}
