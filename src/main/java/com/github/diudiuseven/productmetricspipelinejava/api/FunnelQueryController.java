package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.query.FunnelQueryService;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/metrics/{id}/funnel")
public class FunnelQueryController {

    private final FunnelQueryService queryService;

    public FunnelQueryController(FunnelQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public QueryPage<FunnelQueryService.FunnelRow> query(
            @PathVariable("id") String metricId,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) String bucketFrom,
            @RequestParam(required = false) String bucketTo,
            @RequestParam(required = false) Map<String, String> dimFilter,
            @RequestParam(required = false) Integer pageSize,
            @RequestParam(required = false) String pageToken) {
        return queryService.query(metricId, version, bucketFrom, bucketTo, dimFilter, pageSize, pageToken);
    }
}
