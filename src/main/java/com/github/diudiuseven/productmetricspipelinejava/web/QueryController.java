package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.query.Page;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryRequest;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/query")
public class QueryController {

    private final QueryService queryService;
    private final PipelineProperties props;

    public QueryController(QueryService queryService, PipelineProperties props) {
        this.queryService = queryService;
        this.props = props;
    }

    private QueryRequest request(String metricId, Integer version, long from, long to,
                                 String cursor, Integer pageSize) {
        int size = pageSize == null ? props.getQueryPageSize() : pageSize;
        if (size < 1 || size > props.getQueryPageSize() * 10) {
            throw new IllegalArgumentException("pageSize out of allowed range");
        }
        long deadline = System.nanoTime() + props.getQueryTimeoutMillis() * 1_000_000L;
        return new QueryRequest(metricId, version, from, to, cursor, size, deadline);
    }

    @GetMapping("/funnel/users")
    public Page<?> funnelUsers(@RequestParam String metricId,
                               @RequestParam(required = false) Integer version,
                               @RequestParam long from,
                               @RequestParam long to,
                               @RequestParam(required = false) String cursor,
                               @RequestParam(required = false) Integer pageSize) {
        return queryService.funnelUsers(request(metricId, version, from, to, cursor, pageSize));
    }

    @GetMapping("/funnel/report")
    public Page<?> funnelReport(@RequestParam String metricId,
                                @RequestParam(required = false) Integer version,
                                @RequestParam long from,
                                @RequestParam long to,
                                @RequestParam(required = false) String cursor,
                                @RequestParam(required = false) Integer pageSize) {
        return queryService.funnelReport(request(metricId, version, from, to, cursor, pageSize));
    }

    @GetMapping("/retention")
    public Page<?> retention(@RequestParam String metricId,
                             @RequestParam(required = false) Integer version,
                             @RequestParam long from,
                             @RequestParam long to,
                             @RequestParam(required = false) String cursor,
                             @RequestParam(required = false) Integer pageSize) {
        return queryService.retention(request(metricId, version, from, to, cursor, pageSize));
    }
}
