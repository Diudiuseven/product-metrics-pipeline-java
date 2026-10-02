package com.github.diudiuseven.productmetricspipelinejava.model;

import java.util.Objects;

/** One ordered step of a funnel definition. */
public record FunnelStep(int order, String eventName) {
    public FunnelStep {
        if (order < 0) {
            throw new IllegalArgumentException("step order must be >= 0");
        }
        Objects.requireNonNull(eventName, "eventName");
    }
}
