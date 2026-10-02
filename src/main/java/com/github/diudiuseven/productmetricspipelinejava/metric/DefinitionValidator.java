package com.github.diudiuseven.productmetricspipelinejava.metric;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Field-level definition validation. Every error carries a field path such as
 * {@code funnel.steps[1].eventType}.
 */
@Component
public class DefinitionValidator {

    public List<DefinitionException.FieldError> validate(MetricDefinition def) {
        List<DefinitionException.FieldError> errors = new ArrayList<>();
        // TODO implement
        return errors;
    }

    public void validateOrThrow(MetricDefinition def) {
        List<DefinitionException.FieldError> errors = validate(def);
        if (!errors.isEmpty()) {
            throw new DefinitionException(errors);
        }
    }
}
