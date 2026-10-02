package com.github.diudiuseven.productmetricspipelinejava.query;

import java.util.List;

/**
 * A keyset-paged result. {@code nextPageToken} is opaque to clients; queries
 * never read the whole result set into memory.
 */
public record QueryPage<T>(
        List<T> rows,
        String nextPageToken,
        long commitId,
        long logSeqTo,
        long totalOnPage
) {
    public QueryPage {
        rows = rows == null ? List.of() : List.copyOf(rows);
    }
}
