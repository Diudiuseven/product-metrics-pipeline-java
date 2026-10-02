package com.github.diudiuseven.productmetricspipelinejava.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.compute.AggregateStore;
import com.github.diudiuseven.productmetricspipelinejava.compute.ReplayEngine;
import com.github.diudiuseven.productmetricspipelinejava.config.PipelineProperties;
import com.github.diudiuseven.productmetricspipelinejava.metric.DefinitionRegistry;
import com.github.diudiuseven.productmetricspipelinejava.metric.FunnelSpec;
import com.github.diudiuseven.productmetricspipelinejava.metric.MetricDefinition;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Funnel queries against the latest committed snapshot. Keyset paging: cost is
 * O(page size) via the primary-key index, never O(total rows).
 */
@Service
public class FunnelQueryService {

    public record FunnelRow(String bucket, String dims, long entered, long converted,
                            long skipped, long missing, long outOfWindow, long pending,
                            long[] stepCounts, double[] stepRates, double conversionRate) {
    }

    private record PageToken(String bucket, String dims) {
    }

    private static final TypeReference<Map<String, String>> DIMS_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final AggregateStore store;
    private final DefinitionRegistry registry;
    private final PipelineProperties props;
    private final ObjectMapper objectMapper;

    public FunnelQueryService(JdbcTemplate jdbcTemplate, AggregateStore store, DefinitionRegistry registry,
                              PipelineProperties props, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.store = store;
        this.registry = registry;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    public QueryPage<FunnelRow> query(String metricId, Integer version, String bucketFrom,
                                      String bucketTo, String dimFilterJson,
                                      Integer pageSize, String pageToken) {
        MetricDefinition def = resolve(metricId, version);
        int size = resolvePageSize(pageSize);
        PageToken token = parseToken(pageToken);
        Map<String, String> dimFilter = parseDimFilter(dimFilterJson);
        String dimsParam = dimFilter.isEmpty() ? null
                : ReplayEngine.dimsTuple(dimFilter, def.getFunnel().dimensions());

        StringBuilder sql = new StringBuilder(
                "SELECT * FROM funnel_agg WHERE metric_id = ? AND version = ?");
        List<Object> args = new ArrayList<>();
        args.add(def.getId());
        args.add(def.getVersion());
        if (bucketFrom != null) {
            sql.append(" AND bucket >= ?");
            args.add(bucketFrom);
        }
        if (bucketTo != null) {
            sql.append(" AND bucket <= ?");
            args.add(bucketTo);
        }
        if (dimsParam != null) {
            sql.append(" AND dims = ?");
            args.add(dimsParam);
        }
        if (token != null) {
            sql.append(" AND (bucket > ? OR (bucket = ? AND dims > ?))");
            args.add(token.bucket());
            args.add(token.bucket());
            args.add(token.dims());
        }
        sql.append(" ORDER BY bucket, dims LIMIT ?");
        args.add(size + 1);

        List<FunnelRow> fetched;
        try {
            fetched = jdbcTemplate.query(con -> {
                var ps = con.prepareStatement(sql.toString());
                for (int i = 0; i < args.size(); i++) {
                    ps.setObject(i + 1, args.get(i));
                }
                ps.setQueryTimeout((int) Math.max(1, props.getQueryTimeout().toSeconds()));
                return ps;
            }, rowMapper(def.getFunnel()));
        } catch (QueryTimeoutException e) {
            throw new com.github.diudiuseven.productmetricspipelinejava.query.QueryTimeoutException(
                    "funnel query timed out after " + props.getQueryTimeout());
        }

        boolean hasMore = fetched.size() > size;
        List<FunnelRow> rows = hasMore ? fetched.subList(0, size) : fetched;
        String nextToken = null;
        if (hasMore) {
            FunnelRow last = rows.get(rows.size() - 1);
            nextToken = encodeToken(new PageToken(last.bucket(), last.dims()));
        }
        long commitId = store.loadCheckpoint(def.getId(), def.getVersion())
                .map(cp -> cp.lastCommitId()).orElse(0L);
        long logSeqTo = store.loadCheckpoint(def.getId(), def.getVersion())
                .map(cp -> cp.logSeq()).orElse(0L);
        return new QueryPage<>(rows, nextToken, commitId, logSeqTo, rows.size());
    }

    private MetricDefinition resolve(String id, Integer version) {
        MetricDefinition def = version == null
                ? registry.getLatest(id).orElseThrow(() -> new IllegalArgumentException("unknown metric: " + id))
                : registry.get(id, version).orElseThrow(
                        () -> new IllegalArgumentException("unknown metric: " + id + " v" + version));
        if (def.getType() != com.github.diudiuseven.productmetricspipelinejava.metric.MetricType.FUNNEL) {
            throw new IllegalArgumentException("metric " + id + " is not a FUNNEL metric");
        }
        return def;
    }

    private int resolvePageSize(Integer requested) {
        int size = requested == null ? props.getDefaultPageSize() : requested;
        if (size <= 0) {
            throw new IllegalArgumentException("pageSize must be positive");
        }
        return Math.min(size, props.getMaxPageSize());
    }

    private RowMapper<FunnelRow> rowMapper(FunnelSpec spec) {
        return (rs, n) -> {
            long[] stepCounts = readLongs(rs.getString("step_counts"));
            long entered = rs.getLong("entered");
            double[] rates = new double[stepCounts.length];
            for (int i = 0; i < stepCounts.length; i++) {
                rates[i] = entered == 0 ? 0.0 : (double) stepCounts[i] / entered;
            }
            long converted = rs.getLong("converted");
            double convRate = entered == 0 ? 0.0 : (double) converted / entered;
            return new FunnelRow(rs.getString("bucket"), rs.getString("dims"), entered, converted,
                    rs.getLong("skipped"), rs.getLong("missing"), rs.getLong("out_of_window"),
                    rs.getLong("pending"), stepCounts, rates, convRate);
        };
    }

    private Map<String, String> parseDimFilter(String json) {
        if (json == null || json.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            return objectMapper.readValue(json, DIMS_TYPE);
        } catch (Exception e) {
            throw new IllegalArgumentException("dimFilter must be a JSON object of name->value");
        }
    }

    private PageToken parseToken(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            byte[] raw = Base64.getUrlDecoder().decode(token);
            return objectMapper.readValue(new String(raw, StandardCharsets.UTF_8), PageToken.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid pageToken");
        }
    }

    private String encodeToken(PageToken token) {
        try {
            String json = objectMapper.writeValueAsString(token);
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long[] readLongs(String json) {
        if (json == null || json.isBlank()) {
            return new long[0];
        }
        try {
            return objectMapper.readValue(json, long[].class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
