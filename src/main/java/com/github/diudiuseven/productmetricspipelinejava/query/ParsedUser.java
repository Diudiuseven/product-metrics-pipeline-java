package com.github.diudiuseven.productmetricspipelinejava.query;

import com.github.diudiuseven.productmetricspipelinejava.engine.funnel.UserEvent;

import java.util.List;

/** One per-user NDJSON snapshot row decoded for the read side. */
public record ParsedUser(String userId, List<UserEvent> events) {
}
