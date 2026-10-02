package com.github.diudiuseven.productmetricspipelinejava.model;

import java.util.Collections;
import java.util.List;

/**
 * Thrown when a metric definition file is invalid or conflicts with an already
 * loaded definition. Carries field-scoped error locations.
 */
public class DefinitionException extends RuntimeException {

    /** One field-scoped problem, e.g. {@code steps[2].eventName}. */
    public record Problem(String field, String reason) {}

    private final List<Problem> problems;

    public DefinitionException(String message) {
        this(message, List.of(new Problem("", message)));
    }

    public DefinitionException(String message, List<Problem> problems) {
        super(message);
        this.problems = List.copyOf(problems);
    }

    public List<Problem> problems() {
        return Collections.unmodifiableList(problems);
    }
}
