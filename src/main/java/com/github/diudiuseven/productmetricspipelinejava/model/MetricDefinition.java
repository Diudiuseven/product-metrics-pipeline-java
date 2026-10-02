package com.github.diudiuseven.productmetricspipelinejava.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Versioned definition of a funnel or retention metric.
 *
 * <p>Definitions are immutable once validated. The pair ({@link #id},
 * {@link #version}) is the identity; loading the same id with a different
 * payload at the same version is rejected as a conflict.
 */
public final class MetricDefinition {

    private final String id;
    private final int version;
    private final MetricType type;
    private final List<FunnelStep> steps;
    private final long totalWindowMillis;
    private final long stepWindowMillis;
    private final String anchorEvent;
    private final String retainedEvent;
    private final List<Long> retentionPeriodsMillis;
    private final List<String> dimensionKeys;
    private final int maxDimensionGroups;
    private final long cohortBucketMillis;

    private MetricDefinition(Builder b) {
        this.id = b.id;
        this.version = b.version;
        this.type = b.type;
        this.steps = b.steps == null ? List.of() : List.copyOf(b.steps);
        this.totalWindowMillis = b.totalWindowMillis;
        this.stepWindowMillis = b.stepWindowMillis;
        this.anchorEvent = b.anchorEvent;
        this.retainedEvent = b.retainedEvent;
        this.retentionPeriodsMillis = b.retentionPeriodsMillis == null ? List.of() : List.copyOf(b.retentionPeriodsMillis);
        this.dimensionKeys = b.dimensionKeys == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(b.dimensionKeys));
        this.maxDimensionGroups = b.maxDimensionGroups;
        this.cohortBucketMillis = b.cohortBucketMillis;
    }

    public String id() { return id; }
    public int version() { return version; }
    public MetricType type() { return type; }
    public List<FunnelStep> steps() { return steps; }
    public long totalWindowMillis() { return totalWindowMillis; }
    public long stepWindowMillis() { return stepWindowMillis; }
    public String anchorEvent() { return anchorEvent; }
    public String retainedEvent() { return retainedEvent; }
    public List<Long> retentionPeriodsMillis() { return retentionPeriodsMillis; }
    public List<String> dimensionKeys() { return dimensionKeys; }
    public int maxDimensionGroups() { return maxDimensionGroups; }
    public long cohortBucketMillis() { return cohortBucketMillis; }

    /**
     * Validates field-by-field and either returns nothing or throws with every
     * located problem.
     *
     * @throws DefinitionException with {@link DefinitionException.Problem}s
     *         pointing at concrete fields.
     */
    public void validate() {
        List<DefinitionException.Problem> problems = new ArrayList<>();
        if (id == null || id.isBlank()) {
            problems.add(new DefinitionException.Problem("id", "must be a non-blank string"));
        }
        if (version < 1) {
            problems.add(new DefinitionException.Problem("version", "must be >= 1"));
        }
        if (type == null) {
            problems.add(new DefinitionException.Problem("type", "must be FUNNEL or RETENTION"));
        }
        if (maxDimensionGroups < 1) {
            problems.add(new DefinitionException.Problem("maxDimensionGroups", "must be >= 1"));
        }
        for (String k : dimensionKeys) {
            if (k == null || k.isBlank()) {
                problems.add(new DefinitionException.Problem("dimensionKeys", "contains a blank key"));
            }
        }
        if (dimensionKeys.size() != new ArrayList<>(dimensionKeys).stream().distinct().count()) {
            problems.add(new DefinitionException.Problem("dimensionKeys", "must not contain duplicates"));
        }
        if (type == MetricType.FUNNEL) {
            validateFunnel(problems);
        } else if (type == MetricType.RETENTION) {
            validateRetention(problems);
        }
        if (!problems.isEmpty()) {
            throw new DefinitionException("invalid definition for metric " + id, problems);
        }
    }

    private void validateFunnel(List<DefinitionException.Problem> problems) {
        if (steps.size() < 2) {
            problems.add(new DefinitionException.Problem("steps", "funnel requires at least 2 ordered steps"));
        }
        for (int i = 0; i < steps.size(); i++) {
            FunnelStep s = steps.get(i);
            if (s == null) {
                problems.add(new DefinitionException.Problem("steps[" + i + "]", "is null"));
                continue;
            }
            if (s.order() != i) {
                problems.add(new DefinitionException.Problem("steps[" + i + "].order",
                        "steps must be contiguous starting at 0, expected " + i + " but was " + s.order()));
            }
            if (s.eventName() == null || s.eventName().isBlank()) {
                problems.add(new DefinitionException.Problem("steps[" + i + "].eventName", "must be non-blank"));
            }
        }
        long names = steps.stream().map(FunnelStep::eventName).filter(Objects::nonNull).distinct().count();
        if (names != steps.size()) {
            problems.add(new DefinitionException.Problem("steps", "step event names must be unique within a funnel"));
        }
        if (totalWindowMillis <= 0) {
            problems.add(new DefinitionException.Problem("totalWindowMillis", "must be > 0"));
        }
        if (stepWindowMillis <= 0) {
            problems.add(new DefinitionException.Problem("stepWindowMillis", "must be > 0"));
        } else if (totalWindowMillis > 0 && stepWindowMillis > totalWindowMillis) {
            problems.add(new DefinitionException.Problem("stepWindowMillis",
                    "must be <= totalWindowMillis"));
        }
    }

    private void validateRetention(List<DefinitionException.Problem> problems) {
        if (anchorEvent == null || anchorEvent.isBlank()) {
            problems.add(new DefinitionException.Problem("anchorEvent", "must be non-blank"));
        }
        if (retainedEvent == null || retainedEvent.isBlank()) {
            problems.add(new DefinitionException.Problem("retainedEvent", "must be non-blank"));
        }
        if (retentionPeriodsMillis.isEmpty()) {
            problems.add(new DefinitionException.Problem("retentionPeriodsMillis", "must list at least one period"));
        }
        if (cohortBucketMillis <= 0) {
            problems.add(new DefinitionException.Problem("cohortBucketMillis", "must be > 0"));
        }
        long last = 0;
        for (int i = 0; i < retentionPeriodsMillis.size(); i++) {
            long p = retentionPeriodsMillis.get(i);
            if (p <= 0) {
                problems.add(new DefinitionException.Problem("retentionPeriodsMillis[" + i + "]", "must be > 0"));
            } else if (p <= last) {
                problems.add(new DefinitionException.Problem("retentionPeriodsMillis[" + i + "]",
                        "periods must be strictly increasing"));
            }
            last = p;
        }
    }

    /** Structural equality of payload (used to reject same-version conflicts). */
    public boolean samePayload(MetricDefinition o) {
        return Objects.equals(type, o.type)
                && Objects.equals(steps, o.steps)
                && totalWindowMillis == o.totalWindowMillis
                && stepWindowMillis == o.stepWindowMillis
                && Objects.equals(anchorEvent, o.anchorEvent)
                && Objects.equals(retainedEvent, o.retainedEvent)
                && Objects.equals(retentionPeriodsMillis, o.retentionPeriodsMillis)
                && Objects.equals(dimensionKeys, o.dimensionKeys)
                && maxDimensionGroups == o.maxDimensionGroups
                && cohortBucketMillis == o.cohortBucketMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Mutable construction helper (parsing layer builds into this). */
    public static final class Builder {
        private String id;
        private int version = 1;
        private MetricType type;
        private List<FunnelStep> steps = new ArrayList<>();
        private long totalWindowMillis;
        private long stepWindowMillis;
        private String anchorEvent;
        private String retainedEvent;
        private List<Long> retentionPeriodsMillis = new ArrayList<>();
        private List<String> dimensionKeys = new ArrayList<>();
        private int maxDimensionGroups = 10_000;
        private long cohortBucketMillis = 24L * 3600 * 1000L;

        public Builder id(String v) { this.id = v; return this; }
        public Builder version(int v) { this.version = v; return this; }
        public Builder type(MetricType v) { this.type = v; return this; }
        public Builder steps(List<FunnelStep> v) { this.steps = v; return this; }
        public Builder totalWindowMillis(long v) { this.totalWindowMillis = v; return this; }
        public Builder stepWindowMillis(long v) { this.stepWindowMillis = v; return this; }
        public Builder anchorEvent(String v) { this.anchorEvent = v; return this; }
        public Builder retainedEvent(String v) { this.retainedEvent = v; return this; }
        public Builder retentionPeriodsMillis(List<Long> v) { this.retentionPeriodsMillis = v; return this; }
        public Builder dimensionKeys(List<String> v) { this.dimensionKeys = v; return this; }
        public Builder maxDimensionGroups(int v) { this.maxDimensionGroups = v; return this; }
        public Builder cohortBucketMillis(long v) { this.cohortBucketMillis = v; return this; }

        public MetricDefinition build() {
            MetricDefinition d = new MetricDefinition(this);
            d.validate();
            return d;
        }
    }
}
