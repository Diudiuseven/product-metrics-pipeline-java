package com.github.diudiuseven.productmetricspipelinejava.web;

import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryDeadlineException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/** Translates domain errors to HTTP responses with field-located reasons. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(DefinitionException.class)
    public ResponseEntity<WebDtos.ErrorResponse> invalidDefinition(DefinitionException ex) {
        Map<String, String> fields = new LinkedHashMap<>();
        ex.problems().forEach(p -> fields.put(p.field(), p.reason()));
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new WebDtos.ErrorResponse("invalid metric definition", fields));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<WebDtos.ErrorResponse> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(new WebDtos.ErrorResponse(ex.getMessage(), Map.of()));
    }

    @ExceptionHandler(QueryDeadlineException.class)
    public ResponseEntity<WebDtos.ErrorResponse> timeout(QueryDeadlineException ex) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(new WebDtos.ErrorResponse(ex.getMessage(), Map.of()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<WebDtos.ErrorResponse> conflict(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new WebDtos.ErrorResponse(ex.getMessage(), Map.of()));
    }
}
