package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.ComputeScheduler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final ComputeScheduler scheduler;
    private final AggregateStore store;

    public AdminController(ComputeScheduler scheduler, AggregateStore store) {
        this.scheduler = scheduler;
        this.store = store;
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
     * Recent consistency-check reports for a metric. The {@code status} field
     * gives the distinguishable failure reason: VERIFIED / MISMATCH /
     * SKIPPED_BOUNDS / HORIZON_UNDETERMINED; MISMATCH reports carry per-user
     * and per-group diff detail in {@code diffs}.
     */
    @GetMapping("/verification/{id}")
    public List<com.github.diudiuseven.productmetricspipelinejava.compute.VerificationReport> verification(
            @PathVariable("id") String metricId,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false, defaultValue = "20") int limit) {
        return store.listVerificationReports(metricId, version == null ? 0 : version, limit);
    }

    /** Triggers an on-demand consistency check against the committed snapshot. */
    @PostMapping("/verify/{id}")
    public Map<String, Object> verify(@PathVariable("id") String metricId,
                                      @RequestParam(required = false) Integer version) {
        scheduler.requestVerification(metricId, version);
        return Map.of("status", "VERIFICATION_REQUESTED", "metricId", metricId);
    }
}
