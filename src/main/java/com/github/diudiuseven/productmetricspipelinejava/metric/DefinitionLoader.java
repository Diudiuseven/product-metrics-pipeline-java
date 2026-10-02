package com.github.diudiuseven.productmetricspipelinejava.metric;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public List<MetricDefinition> loadFromDirectory(Path dir) {
        List<MetricDefinition> defs = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return defs;
        }
        try (var stream = Files.list(dir)) {
            List<Path> files = stream.filter(p -> p.toString().endsWith(".json")).sorted().toList();
            for (Path f : files) {
                try {
                    defs.add(parse(Files.readString(f)));
                } catch (DefinitionException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DefinitionException(List.of(new DefinitionException.FieldError(
                            "file:" + f.getFileName(), "PARSE",
                            "cannot parse definition file: " + e.getMessage(), null)));
                }
            }
        } catch (DefinitionException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("cannot list metrics directory: " + dir, e);
        }
        return defs;
    }

    public List<MetricDefinition> loadFromClasspath() {
        List<MetricDefinition> defs = new ArrayList<>();
        try {
            Resource[] resources = new PathMatchingResourcePatternResolver()
                    .getResources("classpath:metrics/*.json");
            for (Resource r : resources) {
                try {
                    defs.add(parse(new String(r.getInputStream().readAllBytes())));
                } catch (DefinitionException e) {
                    throw e;
                } catch (Exception e) {
                    throw new DefinitionException(List.of(new DefinitionException.FieldError(
                            "classpath:" + r.getFilename(), "PARSE",
                            "cannot parse classpath definition: " + e.getMessage(), null)));
                }
            }
        } catch (DefinitionException e) {
            throw e;
        } catch (Exception e) {
            // no classpath metrics: fine
        }
        return defs;
    }

    public MetricDefinition parse(String json) {
        try {
            return objectMapper.readValue(json, MetricDefinition.class);
        } catch (Exception e) {
            throw new DefinitionException(List.of(new DefinitionException.FieldError(
                    "body", "PARSE", "malformed definition JSON: " + e.getMessage(), null)));
        }
    }

    public String toJson(MetricDefinition def) {
        try {
            return objectMapper.writeValueAsString(def);
        } catch (Exception e) {
            throw new IllegalStateException("cannot serialize definition", e);
        }
    }
}
