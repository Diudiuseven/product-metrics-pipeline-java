package com.github.diudiuseven.productmetricspipelinejava.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.List;

/**
 * Loads metric definitions from JSON files (classpath:metrics/*.json and the
 * configured metrics directory).
 */
@Component
public class DefinitionLoader {

    private final ObjectMapper objectMapper;

    public DefinitionLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<MetricDefinition> loadFromDirectory(Path dir) {
        // TODO implement
        return List.of();
    }

    public List<MetricDefinition> loadFromClasspath() {
        // TODO implement
        return List.of();
    }

    public MetricDefinition parse(String json) {
        // TODO implement
        return null;
    }

    public String toJson(MetricDefinition def) {
        // TODO implement
        return null;
    }
}
