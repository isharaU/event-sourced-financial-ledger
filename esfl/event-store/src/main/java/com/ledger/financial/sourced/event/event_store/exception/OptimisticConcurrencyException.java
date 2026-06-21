package com.ledger.financial.sourced.event.event_store.exception;

import java.util.UUID;

public class OptimisticConcurrencyException extends RuntimeException {
    
    private final UUID aggregateId;
    private final int expectedVersion;

    public OptimisticConcurrencyException(UUID aggregateId, int expectedVersion, Throwable cause) {
        super(String.format("Optimistic concurrency conflict for aggregate %s. Expected version: %d", aggregateId, expectedVersion), cause);
        this.aggregateId = aggregateId;
        this.expectedVersion = expectedVersion;
    }

    public OptimisticConcurrencyException(UUID aggregateId, int expectedVersion) {
        super(String.format("Optimistic concurrency conflict for aggregate %s. Expected version: %d", aggregateId, expectedVersion));
        this.aggregateId = aggregateId;
        this.expectedVersion = expectedVersion;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }
}
