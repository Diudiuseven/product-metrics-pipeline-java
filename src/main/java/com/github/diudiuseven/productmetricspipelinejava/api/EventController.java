package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.event.Event;
import com.github.diudiuseven.productmetricspipelinejava.event.EventIngestService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventIngestService ingestService;

    public EventController(EventIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    public EventIngestService.IngestResult post(@RequestBody Event event) {
        return ingestService.ingest(event);
    }

    @PostMapping("/batch")
    public List<EventIngestService.IngestResult> postBatch(@RequestBody List<Event> events) {
        return ingestService.ingestBatch(events);
    }
}
