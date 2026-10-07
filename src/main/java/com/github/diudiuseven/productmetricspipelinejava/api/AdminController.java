package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.ComputeScheduler;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.verify.ConsistencyReport;
import com.github.diudiuseven.productmetricspipelinejava.verify.MetricGate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final ComputeScheduler scheduler;
    private final AggregateStore store;
    private final DefinitionRegistry registry;

    public AdminController(ComputeScheduler scheduler, AggregateStore store, DefinitionRegistry registry) {
        this.scheduler = scheduler;
        this.store = store;
        this.registry = registry;
    }

    @PostMapping("/recompute/{id}")
    public Map<String, Object> recompute(@PathVariable("id") String metricId,
                                          @RequestParam(required = false) Integer version) {
        scheduler.requestRecompute(metricId, version, "MANUAL");
        return Map.of("status", "RECOMPUTE_REQUESTED", "metricId", metricId);
    }

    @GetMapping("/commits/{id}")
    public List<AggregateStore.CommitRecord> commits(@PathVariable("id") String metricId,
                                                     @RequestParam(required = false) Integer version) {
        return store.listCommits(metricId, version == null ? 0 : version, 20);
    }

    /**
     * Consistency status of a metric: the publish gate plus the most recent
     * verification reports. Failure reasons are machine-readable:
     * CONSISTENCY_MISMATCH / DECISION_BASIS_UNDETERMINED / BOUNDS_EXCEEDED.
     */
    @GetMapping("/consistency/{id}")
    public Map<String, Object> consistency(@PathVariable("id") String metricId,
                                           @RequestParam(required = false) Integer version) {
        MetricDefinition def = version == null
                ? registry.getLatest(metricId).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId))
                : registry.get(metricId, version).orElseThrow(() ->
                        new IllegalArgumentException("unknown metric: " + metricId + " v" + version));
        MetricGate gate = store.loadGate(def.getId(), def.getVersion())
                .orElse(MetricGate.open(def.getId(), def.getVersion()));
        List<ConsistencyReport> reports = store.listReports(def.getId(), def.getVersion(), 10);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("metricId", def.getId());
        body.put("version", def.getVersion());
        body.put("gate", Map.of(
                "status", gate.blocked() ? "BLOCKED" : "OPEN",
                "reason", gate.reason() == null ? "" : gate.reason().name()));
        body.put("latestReport", reports.isEmpty() ? null : reports.get(0));
        body.put("recentReports", reports);
        return body;
    }
}
