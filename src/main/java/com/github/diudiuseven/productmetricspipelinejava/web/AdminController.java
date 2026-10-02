package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Operational endpoints: lag observability and full recomputation. */
@RestController
public class AdminController {

    private final PipelineOrchestrator orchestrator;

    public AdminController(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @GetMapping("/api/status/lag")
    public List<?> lag() {
        return orchestrator.lag();
    }

    @PostMapping("/api/admin/recompute/{metricId}")
    public Object recompute(@PathVariable String metricId,
                            @RequestParam(required = false) Integer version) {
        return orchestrator.recompute(metricId, version);
    }
}
