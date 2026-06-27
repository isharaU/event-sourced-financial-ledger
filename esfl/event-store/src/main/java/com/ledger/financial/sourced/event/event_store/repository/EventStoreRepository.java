package com.ledger.financial.sourced.event.event_store.repository;

import java.util.List;
import java.util.UUID;

import com.ledger.financial.sourced.event.contracts.DomainEvent;

public interface EventStoreRepository {
    
    /**
     * Appends a sequence of domain events to the event store.
     * 
     * @param aggregateId     The aggregate ID
     * @param expectedVersion The current expected version of the aggregate before these events are appended.
     *                        If the aggregate is new, this should be 0.
     * @param events          The list of events to append
     */
    void appendEvents(UUID aggregateId, int expectedVersion, List<DomainEvent> events);
}
