package com.github.diudiuseven.productmetricspipelinejava.store;

import java.util.List;

/** Result of a durable ingest: what was appended and which ids were duplicates. */
public record IngestResult(List<EventRecord> appended, List<String> duplicateIds) {
    public long lastSeq() {
        return appended.isEmpty() ? -1L : appended.get(appended.size() - 1).seq();
    }
}
