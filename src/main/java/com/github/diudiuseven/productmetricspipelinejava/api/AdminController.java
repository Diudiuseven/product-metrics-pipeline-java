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
}
