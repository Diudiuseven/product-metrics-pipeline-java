package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/definitions")
public class DefinitionsController {

    private final PipelineOrchestrator orchestrator;

    public DefinitionsController(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public WebDtos.DefinitionDto register(@RequestBody WebDtos.DefinitionDto dto) {
        if (dto == null || dto.id() == null || dto.id().isBlank()) {
            throw new IllegalArgumentException("definition requires an id");
        }
        MetricDefinition def = toDefinition(dto);
        MetricDefinition saved = orchestrator.registerDefinition(def);
        return toDto(saved);
    }

    @GetMapping("/{metricId}")
    public List<WebDtos.DefinitionDto> versions(@PathVariable String metricId) {
        return orchestrator.definitions().versions(metricId).stream().map(DefinitionsController::toDto).toList();
    }

    static MetricDefinition toDefinition(WebDtos.DefinitionDto d) {
        MetricType type = MetricType.valueOf(d.type()); // IllegalArgumentException -> 400
        var b = MetricDefinition.builder()
                .id(d.id())
                .version(d.version() == null ? 1 : d.version())
                .type(type)
                .dimensionKeys(d.dimensionKeys() == null ? List.of() : d.dimensionKeys())
                .maxDimensionGroups(d.maxDimensionGroups() == null ? 10_000 : d.maxDimensionGroups());
        if (d.totalWindowMillis() != null) {
            b.totalWindowMillis(d.totalWindowMillis());
        }
        if (d.stepWindowMillis() != null) {
            b.stepWindowMillis(d.stepWindowMillis());
        }
        if (d.anchorEvent() != null) {
            b.anchorEvent(d.anchorEvent());
        }
        if (d.retainedEvent() != null) {
            b.retainedEvent(d.retainedEvent());
        }
        if (d.retentionPeriodsMillis() != null) {
            b.retentionPeriodsMillis(d.retentionPeriodsMillis());
        }
        if (d.steps() != null) {
            List<FunnelStep> steps = new ArrayList<>();
            for (int i = 0; i < d.steps().size(); i++) {
                var s = d.steps().get(i);
                steps.add(new FunnelStep(s.order() == null ? i : s.order(), s.eventName()));
            }
            b.steps(steps);
        }
        return b.build(); // field-located validation happens here
    }

    static WebDtos.DefinitionDto toDto(MetricDefinition d) {
        List<WebDtos.StepDto> steps = d.steps().stream()
                .map(s -> new WebDtos.StepDto(s.order(), s.eventName())).toList();
        return new WebDtos.DefinitionDto(d.id(), d.version(), d.type().name(), steps,
                d.totalWindowMillis() == 0 ? null : d.totalWindowMillis(),
                d.stepWindowMillis() == 0 ? null : d.stepWindowMillis(),
                d.anchorEvent(), d.retainedEvent(),
                d.retentionPeriodsMillis().isEmpty() ? null : d.retentionPeriodsMillis(),
                d.dimensionKeys().isEmpty() ? null : d.dimensionKeys(),
                d.maxDimensionGroups());
    }
}
