package com.github.diudiuseven.productmetricspipelinejava.metric;

import java.util.List;

/**
 * Thrown when a metric definition is invalid or conflicting. Every error
 * carries a field path locating the offending field.
 */
public class DefinitionException extends RuntimeException {

    public record FieldError(String field, String code, String message, Object rejectedValue) {
    }

    private final transient List<FieldError> errors;

    public DefinitionException(List<FieldError> errors) {
        super("Invalid metric definition: " + errors.size() + " field error(s): "
                + errors.stream().map(e -> e.field() + " [" + e.code() + "] " + e.message()).toList());
        this.errors = List.copyOf(errors);
    }

    public List<FieldError> getErrors() { return errors; }
}
