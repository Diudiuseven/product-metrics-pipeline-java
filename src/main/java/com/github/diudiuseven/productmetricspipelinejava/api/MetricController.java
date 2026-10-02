package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionLoader;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/metrics")
public class MetricController {

    private final DefinitionRegistry registry;
    private final DefinitionLoader loader;

    public MetricController(DefinitionRegistry registry, DefinitionLoader loader) {
        this.registry = registry;
        this.loader = loader;
    }

    @GetMapping
    public List<MetricDefinition> list() {
        return registry.listAll();
    }

    @GetMapping("/{id}")
    public MetricDefinition get(@PathVariable String id,
                                @RequestParam(required = false) Integer version) {
        return (version == null ? registry.getLatest(id) : registry.get(id, version))
                .orElseThrow(() -> new IllegalArgumentException("unknown metric: " + id));
    }

    @PostMapping
    public MetricDefinition register(@RequestBody String json) {
        MetricDefinition def = loader.parse(json);
        return registry.register(def);
    }
}
