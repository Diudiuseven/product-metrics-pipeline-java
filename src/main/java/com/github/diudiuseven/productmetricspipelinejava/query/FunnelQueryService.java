package com.github.diudiuseven.productmetricspipelinejava.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Funnel queries against the latest committed snapshot. Keyset paging: cost is
 * O(page size), never O(total rows).
 */
@Service
public class FunnelQueryService {

    public record FunnelRow(String bucket, String dims, long entered, long converted,
                            long skipped, long missing, long outOfWindow, long pending,
                            long[] stepCounts, double[] stepRates) {
    }

    private final JdbcTemplate jdbcTemplate;

    public FunnelQueryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public QueryPage<FunnelRow> query(String metricId, Integer version, String bucketFrom,
                                      String bucketTo, Map<String, String> dimFilter,
                                      Integer pageSize, String pageToken) {
        // TODO implement
        return new QueryPage<>(List.of(), null, 0L, 0L, 0L);
    }
}
