package com.github.diudiuseven.productmetricspipelinejava.api;

import com.github.diudiuseven.productmetricspipelinejava.query.QueryPage;
import com.github.diudiuseven.productmetricspipelinejava.query.RetentionQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/metrics/{id}/retention")
public class RetentionQueryController {

    private final RetentionQueryService queryService;

    public RetentionQueryController(RetentionQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public QueryPage<RetentionQueryService.RetentionRow> query(
            @PathVariable("id") String metricId,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) String cohortFrom,
            @RequestParam(required = false) String cohortTo,
            @RequestParam(required = false) String dimFilter,
            @RequestParam(required = false) Integer pageSize,
            @RequestParam(required = false) String pageToken) {
        return queryService.query(metricId, version, cohortFrom, cohortTo, dimFilter, pageSize, pageToken);
    }
}
