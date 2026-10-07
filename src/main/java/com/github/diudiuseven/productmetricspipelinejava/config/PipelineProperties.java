package com.github.diudiuseven.productmetricspipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Tunable pipeline bounds. See docs/configuration.md for semantics.
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    public enum BucketGranularity { HOUR, DAY }

    /** How often the builder polls the event log for new events. */
    private Duration batchInterval = Duration.ofMillis(200);
    /** Max events consumed in one incremental batch. */
    private int maxBatchEvents = 5000;
    /** Safety fuse: per-user events beyond this force a full recompute. */
    private int maxEventsPerUser = 10000;
    /** Max distinct dimension tuples per metric (overflow is counted separately). */
    private int maxDimCombinations = 50000;
    /** Max distinct users re-evaluated in one incremental batch. */
    private int maxAffectedUsers = 2000;
    /** Safety fuse: events beyond this still recompute but with a warning. */
    private long recomputeMaxEvents = 2_000_000L;
    /** Worker threads for parallel metric builders. */
    private int threadPoolSize = 4;
    /** Min interval between two verification runs of the same metric. */
    private Duration verificationInterval = Duration.ofSeconds(30);
    /** Max diff entries recorded in one consistency report. */
    private int verificationMaxDiffs = 50;
    /** Hard query timeout; timed-out queries return no rows at all. */
    private Duration queryTimeout = Duration.ofSeconds(5);
    private int defaultPageSize = 100;
    private int maxPageSize = 1000;
    /** Directory of metric definition JSON files. */
    private String metricsDir = "./metrics";
    private BucketGranularity bucketGranularity = BucketGranularity.DAY;

    public Duration getBatchInterval() { return batchInterval; }
    public void setBatchInterval(Duration batchInterval) { this.batchInterval = batchInterval; }
    public int getMaxBatchEvents() { return maxBatchEvents; }
    public void setMaxBatchEvents(int maxBatchEvents) { this.maxBatchEvents = maxBatchEvents; }
    public int getMaxEventsPerUser() { return maxEventsPerUser; }
    public void setMaxEventsPerUser(int maxEventsPerUser) { this.maxEventsPerUser = maxEventsPerUser; }
    public int getMaxDimCombinations() { return maxDimCombinations; }
    public void setMaxDimCombinations(int maxDimCombinations) { this.maxDimCombinations = maxDimCombinations; }
    public int getMaxAffectedUsers() { return maxAffectedUsers; }
    public void setMaxAffectedUsers(int maxAffectedUsers) { this.maxAffectedUsers = maxAffectedUsers; }
    public long getRecomputeMaxEvents() { return recomputeMaxEvents; }
    public void setRecomputeMaxEvents(long recomputeMaxEvents) { this.recomputeMaxEvents = recomputeMaxEvents; }
    public int getThreadPoolSize() { return threadPoolSize; }
    public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }
    public Duration getVerificationInterval() { return verificationInterval; }
    public void setVerificationInterval(Duration verificationInterval) { this.verificationInterval = verificationInterval; }
    public int getVerificationMaxDiffs() { return verificationMaxDiffs; }
    public void setVerificationMaxDiffs(int verificationMaxDiffs) { this.verificationMaxDiffs = verificationMaxDiffs; }
    public Duration getQueryTimeout() { return queryTimeout; }
    public void setQueryTimeout(Duration queryTimeout) { this.queryTimeout = queryTimeout; }
    public int getDefaultPageSize() { return defaultPageSize; }
    public void setDefaultPageSize(int defaultPageSize) { this.defaultPageSize = defaultPageSize; }
    public int getMaxPageSize() { return maxPageSize; }
    public void setMaxPageSize(int maxPageSize) { this.maxPageSize = maxPageSize; }
    public String getMetricsDir() { return metricsDir; }
    public void setMetricsDir(String metricsDir) { this.metricsDir = metricsDir; }
    public BucketGranularity getBucketGranularity() { return bucketGranularity; }
    public void setBucketGranularity(BucketGranularity bucketGranularity) { this.bucketGranularity = bucketGranularity; }
}
