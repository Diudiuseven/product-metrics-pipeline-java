package com.github.diudiuseven.productmetricspipelinejava.query;

/**
 * Thrown when a query exceeds the configured timeout. No partial rows are
 * returned to the client.
 */
public class QueryTimeoutException extends RuntimeException {
    public QueryTimeoutException(String message) {
        super(message);
    }
}
