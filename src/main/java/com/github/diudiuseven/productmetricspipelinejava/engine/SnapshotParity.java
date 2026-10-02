package com.github.diudiuseven.productmetricspipelinejava.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelAggregator;
import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.FunnelState;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionAggregator;
import com.github.diudiuseven.productmetricspipelinejava.engine.retention.RetentionState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Checks that two states claiming the same log position contain identical
 * facts. Both serialisers are deterministic (users in lexicographic order,
 * events in total order), so equal SHA-256 over the NDJSON bodies proves
 * row-for-row equality — the recompute/correction consistency guarantee.
 */
final class SnapshotParity {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SnapshotParity() {
    }

    static boolean equalsSerialized(MetricAggregator live, AggregatorState rebuilt) {
        String liveBody = live instanceof FunnelAggregator fa
                ? body(fa.funnelState())
                : body(((RetentionAggregator) live).retentionState());
        String rebuiltBody = rebuilt instanceof FunnelState fs
                ? body(fs)
                : body((RetentionState) rebuilt);
        return liveBody.equals(rebuiltBody);
    }

    private static String body(FunnelState st) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            st.writeSnapshot(out, MAPPER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return stripHeaderAndHash(out.toByteArray());
    }

    private static String body(RetentionState st) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            st.writeSnapshot(out, MAPPER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return stripHeaderAndHash(out.toByteArray());
    }

    private static String stripHeaderAndHash(byte[] ndjson) {
        String text = new String(ndjson, StandardCharsets.UTF_8);
        int nl = text.indexOf('\n');
        String body = nl < 0 ? "" : text.substring(nl + 1);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
