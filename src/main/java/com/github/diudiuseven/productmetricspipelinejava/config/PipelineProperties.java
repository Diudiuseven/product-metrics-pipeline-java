package com.github.diudiuseven.productmetricspipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bounded resource configuration for the pipeline. Every tunable here is an
 * explicit upper bound on memory, concurrency or latency; see application
 * properties / docs for units and defaults.
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    /** Directory holding the durable event log, snapshots and definitions. */
    private String dataDir = "data";

    /** Events pulled from the log per drain iteration by each aggregator. */
    private int drainBatchSize = 500;

    /** Aggregator worker threads (multi-metric parallelism ceiling). */
    private int workerThreads = 4;

    /** Max queued/held events accounted per metric before backpressure. */
    private long maxEventsPerMetric = 5_000_000L;

    /** Max dimension groups per metric; overflow groups collapse deterministically. */
    private int maxDimensionGroups = 10_000;

    /** Events older than now - horizonMillis at ingest are rejected as too-late. */
    private long maxOutOfOrderHorizonMillis = 7L * 24 * 3600 * 1000L;

    /** Max wall-clock budget for one paged query request in millis. */
    private long queryTimeoutMillis = 2_000L;

    /** Maximum rows materialised in one query page. */
    private int queryPageSize = 1_000;

    /** Drain loop idle sleep when no new events (millis). */
    private long drainIntervalMillis = 20L;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String v) { this.dataDir = v; }
    public int getDrainBatchSize() { return drainBatchSize; }
    public void setDrainBatchSize(int v) { this.drainBatchSize = v; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int v) { this.workerThreads = v; }
    public long getMaxEventsPerMetric() { return maxEventsPerMetric; }
    public void setMaxEventsPerMetric(long v) { this.maxEventsPerMetric = v; }
    public int getMaxDimensionGroups() { return maxDimensionGroups; }
    public void setMaxDimensionGroups(int v) { this.maxDimensionGroups = v; }
    public long getMaxOutOfOrderHorizonMillis() { return maxOutOfOrderHorizonMillis; }
    public void setMaxOutOfOrderHorizonMillis(long v) { this.maxOutOfOrderHorizonMillis = v; }
    public long getQueryTimeoutMillis() { return queryTimeoutMillis; }
    public void setQueryTimeoutMillis(long v) { this.queryTimeoutMillis = v; }
    public int getQueryPageSize() { return queryPageSize; }
    public void setQueryPageSize(int v) { this.queryPageSize = v; }
    public long getDrainIntervalMillis() { return drainIntervalMillis; }
    public void setDrainIntervalMillis(long v) { this.drainIntervalMillis = v; }
}
