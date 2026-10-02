package com.github.diudiuseven.productmetricspipelinejava.event;

/**
 * An event as stored in the log: its durable position plus payload.
 */
public record StoredEvent(long logSeq, Event event) {
}
