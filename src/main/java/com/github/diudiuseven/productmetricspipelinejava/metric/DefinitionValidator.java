package com.github.diudiuseven.productmetricspipelinejava.metric;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Field-level definition validation. Every error carries a field path such as
 * {@code funnel.steps[1].eventType}.
 */
@Component
public class DefinitionValidator {

    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{1,64}$");
    private static final Duration MAX_WINDOW = Duration.ofDays(31);
    private static final Duration MAX_PERIOD = Duration.ofDays(366);

    public List<DefinitionException.FieldError> validate(MetricDefinition def) {
        List<DefinitionException.FieldError> errors = new ArrayList<>();
        if (def == null) {
            errors.add(new DefinitionException.FieldError("", "REQUIRED", "definition must not be null", null));
            return errors;
        }
        validateId(def, errors);
        validateType(def, errors);
        if (def.getType() == MetricType.FUNNEL) {
            validateFunnel(def, errors);
        } else if (def.getType() == MetricType.RETENTION) {
            validateRetention(def, errors);
        }
        return errors;
    }

    public void validateOrThrow(MetricDefinition def) {
        List<DefinitionException.FieldError> errors = validate(def);
        if (!errors.isEmpty()) {
            throw new DefinitionException(errors);
        }
    }

    private void validateId(MetricDefinition def, List<DefinitionException.FieldError> errors) {
        if (isBlank(def.getId())) {
            errors.add(new DefinitionException.FieldError("id", "REQUIRED", "metric id is required", def.getId()));
        } else if (!ID_PATTERN.matcher(def.getId()).matches()) {
            errors.add(new DefinitionException.FieldError("id", "PATTERN",
                    "id must match [a-zA-Z0-9_-]{1,64}", def.getId()));
        }
    }

    private void validateType(MetricDefinition def, List<DefinitionException.FieldError> errors) {
        if (def.getType() == null) {
            errors.add(new DefinitionException.FieldError("type", "REQUIRED",
                    "type is required (FUNNEL or RETENTION)", null));
            return;
        }
        if (def.getType() == MetricType.FUNNEL && def.getRetention() != null) {
            errors.add(new DefinitionException.FieldError("retention", "CONFLICT",
                    "retention spec must not be set for type FUNNEL", null));
        }
        if (def.getType() == MetricType.RETENTION && def.getFunnel() != null) {
            errors.add(new DefinitionException.FieldError("funnel", "CONFLICT",
                    "funnel spec must not be set for type RETENTION", null));
        }
    }

    private void validateFunnel(MetricDefinition def, List<DefinitionException.FieldError> errors) {
        FunnelSpec f = def.getFunnel();
        if (f == null) {
            errors.add(new DefinitionException.FieldError("funnel", "REQUIRED",
                    "funnel spec is required for type FUNNEL", null));
            return;
        }
        if (f.steps() == null || f.steps().isEmpty()) {
            errors.add(new DefinitionException.FieldError("funnel.steps", "REQUIRED",
                    "funnel must have at least one step", null));
        } else {
            Set<String> stepIds = new HashSet<>();
            for (int i = 0; i < f.steps().size(); i++) {
                StepDefinition s = f.steps().get(i);
                String base = "funnel.steps[" + i + "]";
                if (s == null) {
                    errors.add(new DefinitionException.FieldError(base, "REQUIRED", "step must not be null", null));
                    continue;
                }
                if (isBlank(s.eventType())) {
                    errors.add(new DefinitionException.FieldError(base + ".eventType", "REQUIRED",
                            "step eventType is required", s.eventType()));
                }
                if (s.id() != null && !s.id().isBlank()) {
                    if (!stepIds.add(s.id())) {
                        errors.add(new DefinitionException.FieldError(base + ".id", "DUPLICATE",
                                "duplicate step id", s.id()));
                    }
                }
                if (s.dimFilters() != null) {
                    s.dimFilters().forEach((k, v) -> {
                        if (isBlank(k)) {
                            errors.add(new DefinitionException.FieldError(base + ".dimFilters", "REQUIRED",
                                    "dim filter name must not be blank", k));
                        }
                        if (isBlank(v)) {
                            errors.add(new DefinitionException.FieldError(base + ".dimFilters." + k, "REQUIRED",
                                    "dim filter value must not be blank", v));
                        }
                    });
                }
            }
        }
        boolean multiStep = f.steps() != null && f.steps().size() > 1;
        if (multiStep) {
            if (f.window() == null) {
                errors.add(new DefinitionException.FieldError("funnel.window", "REQUIRED",
                        "window is required for funnels with more than one step", null));
            } else {
                if (f.window().isZero() || f.window().isNegative()) {
                    errors.add(new DefinitionException.FieldError("funnel.window", "POSITIVE",
                            "window must be strictly positive", f.window()));
                } else if (f.window().compareTo(MAX_WINDOW) > 0) {
                    errors.add(new DefinitionException.FieldError("funnel.window", "OUT_OF_RANGE",
                            "window must not exceed 31 days", f.window()));
                }
            }
        }
        if (f.windowFrom() == null) {
            errors.add(new DefinitionException.FieldError("funnel.windowFrom", "REQUIRED",
                    "windowFrom is required (FIRST_STEP or PREVIOUS_STEP)", null));
        }
        validateDimensions(f.dimensions(), "funnel.dimensions", errors);
        if (f.bucketGranularity() != null && !isValidGranularity(f.bucketGranularity())) {
            errors.add(new DefinitionException.FieldError("funnel.bucketGranularity", "ENUM",
                    "bucketGranularity must be HOUR or DAY", f.bucketGranularity()));
        }
    }

    private void validateRetention(MetricDefinition def, List<DefinitionException.FieldError> errors) {
        RetentionSpec r = def.getRetention();
        if (r == null) {
            errors.add(new DefinitionException.FieldError("retention", "REQUIRED",
                    "retention spec is required for type RETENTION", null));
            return;
        }
        if (isBlank(r.anchorEventType())) {
            errors.add(new DefinitionException.FieldError("retention.anchorEventType", "REQUIRED",
                    "anchorEventType is required", r.anchorEventType()));
        }
        if (isBlank(r.returnEventType())) {
            errors.add(new DefinitionException.FieldError("retention.returnEventType", "REQUIRED",
                    "returnEventType is required", r.returnEventType()));
        }
        if (r.periods() == null || r.periods().isEmpty()) {
            errors.add(new DefinitionException.FieldError("retention.periods", "REQUIRED",
                    "at least one retention period is required", null));
        } else {
            Duration prev = null;
            for (int i = 0; i < r.periods().size(); i++) {
                Duration p = r.periods().get(i);
                String base = "retention.periods[" + i + "]";
                if (p == null || p.isZero() || p.isNegative()) {
                    errors.add(new DefinitionException.FieldError(base, "POSITIVE",
                            "retention period must be strictly positive", p));
                    continue;
                }
                if (p.compareTo(MAX_PERIOD) > 0) {
                    errors.add(new DefinitionException.FieldError(base, "OUT_OF_RANGE",
                            "retention period must not exceed 366 days", p));
                }
                if (prev != null && p.compareTo(prev) <= 0) {
                    errors.add(new DefinitionException.FieldError(base, "ASCENDING",
                            "retention periods must be strictly ascending", p));
                }
                prev = p;
            }
        }
        if (r.returnWindow() != null && (r.returnWindow().isZero() || r.returnWindow().isNegative())) {
            errors.add(new DefinitionException.FieldError("retention.returnWindow", "POSITIVE",
                    "returnWindow must be strictly positive", r.returnWindow()));
        }
        validateDimensions(r.dimensions(), "retention.dimensions", errors);
        if (r.cohortGranularity() != null && !isValidGranularity(r.cohortGranularity())) {
            errors.add(new DefinitionException.FieldError("retention.cohortGranularity", "ENUM",
                    "cohortGranularity must be HOUR or DAY", r.cohortGranularity()));
        }
    }

    private void validateDimensions(List<String> dims, String field, List<DefinitionException.FieldError> errors) {
        if (dims == null) {
            return;
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < dims.size(); i++) {
            String d = dims.get(i);
            if (isBlank(d)) {
                errors.add(new DefinitionException.FieldError(field + "[" + i + "]", "REQUIRED",
                        "dimension name must not be blank", d));
            } else if (!seen.add(d)) {
                errors.add(new DefinitionException.FieldError(field, "DUPLICATE",
                        "duplicate dimension name", d));
            }
        }
    }

    private static boolean isValidGranularity(String g) {
        return "HOUR".equals(g) || "DAY".equals(g);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
