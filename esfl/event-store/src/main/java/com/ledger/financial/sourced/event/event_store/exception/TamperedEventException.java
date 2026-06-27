package com.ledger.financial.sourced.event.event_store.exception;

import java.util.UUID;

/**
 * Exception thrown when a domain event's checksum does not match the stored checksum,
 * indicating that the event payload or metadata has been tampered with.
 */
public class TamperedEventException extends RuntimeException {
    private final UUID aggregateId;
    private final int version;

    public TamperedEventException(UUID aggregateId, int version) {
        super(String.format("Event tampering detected for aggregate %s at version %d", aggregateId, version));
        this.aggregateId = aggregateId;
        this.version = version;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public int getVersion() {
        return version;
    }
}
