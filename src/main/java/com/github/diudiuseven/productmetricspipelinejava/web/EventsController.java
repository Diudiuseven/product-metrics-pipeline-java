package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.store.IngestResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/events")
public class EventsController {

    private final PipelineOrchestrator orchestrator;

    public EventsController(PipelineOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public WebDtos.IngestResponse ingest(@RequestBody WebDtos.IngestRequest request) {
        if (request == null || request.events() == null || request.events().isEmpty()) {
            throw new IllegalArgumentException("events must be a non-empty list");
        }
        IngestResult result = orchestrator.ingest(request.events());
        return new WebDtos.IngestResponse(result.lastSeq(), result.appended().size(), result.duplicateIds());
    }
}
