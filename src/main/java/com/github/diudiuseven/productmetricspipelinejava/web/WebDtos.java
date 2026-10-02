package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.model.Event;

import java.util.List;
import java.util.Map;

/** Request/response DTOs for the HTTP API. */
public final class WebDtos {

    private WebDtos() {
    }

    public record IngestRequest(List<Event> events) {
    }

    public record IngestResponse(long lastSeq, int appended, List<String> duplicateIds) {
    }

    public record DefinitionDto(String id,
                                Integer version,
                                String type,
                                List<StepDto> steps,
                                Long totalWindowMillis,
                                Long stepWindowMillis,
                                String anchorEvent,
                                String retainedEvent,
                                List<Long> retentionPeriodsMillis,
                                List<String> dimensionKeys,
                                Integer maxDimensionGroups) {
    }

    public record StepDto(Integer order, String eventName) {
    }

    public record ErrorResponse(String error, Map<String, String> fields) {
    }
}
