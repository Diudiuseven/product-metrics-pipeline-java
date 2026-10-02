package com.github.diudiuseven.productmetricspipelinejava.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Retention cohort queries against the latest committed snapshot.
 */
@Service
public class RetentionQueryService {

    public record RetentionRow(String cohort, String dims, long cohortSize,
                               long[] retained, double[] retentionRates) {
    }

    private final JdbcTemplate jdbcTemplate;

    public RetentionQueryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public QueryPage<RetentionRow> query(String metricId, Integer version, String cohortFrom,
                                         String cohortTo, Map<String, String> dimFilter,
                                         Integer pageSize, String pageToken) {
        // TODO implement
        return new QueryPage<>(List.of(), null, 0L, 0L, 0L);
    }
}
