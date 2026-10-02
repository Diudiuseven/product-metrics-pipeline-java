package com.github.diudiuseven.productmetricspipelinejava.metric;

import java.time.Instant;

/**
 * A versioned metric definition. New versions never change the semantics of
 * already-produced results: aggregates are keyed by (id, version).
 */
public class MetricDefinition {

    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_ARCHIVED = "ARCHIVED";

    private String id;
    private int version;
    private MetricType type;
    private FunnelSpec funnel;
    private RetentionSpec retention;
    private String status = STATUS_ACTIVE;
    private Instant createdAt;

    public MetricDefinition() {
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    public MetricType getType() { return type; }
    public void setType(MetricType type) { this.type = type; }
    public FunnelSpec getFunnel() { return funnel; }
    public void setFunnel(FunnelSpec funnel) { this.funnel = funnel; }
    public RetentionSpec getRetention() { return retention; }
    public void setRetention(RetentionSpec retention) { this.retention = retention; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
