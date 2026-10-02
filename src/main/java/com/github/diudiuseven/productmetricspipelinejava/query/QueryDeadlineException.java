package com.github.diudiuseven.productmetricspipelinejava.query;

/**
 * Raised when a query exceeds its configured deadline or is cancelled. Callers
 * translate this to a 504-style error; a half-built page is never emitted, and
 * query threads do not touch aggregation state, so an aborted query cannot
 * affect ongoing ingestion.
 */
public class QueryDeadlineException extends RuntimeException {

    public QueryDeadlineException(String message) {
        super(message);
    }
}
