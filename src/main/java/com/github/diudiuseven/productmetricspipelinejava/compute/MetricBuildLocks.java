package com.github.diudiuseven.productmetricspipelinejava.compute;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-(metric, version) build mutex. Incremental publishes and full
 * recomputes of the SAME metric never overlap, so a commit is always built
 * on top of the latest committed state; different metrics still build fully
 * in parallel. Queries and ingestion never take this lock.
 */
@Component
public class MetricBuildLocks {

    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public Object of(String metricId, int version) {
        return locks.computeIfAbsent(metricId + "\n" + version, k -> new Object());
    }
}
