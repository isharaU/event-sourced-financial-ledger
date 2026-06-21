package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public interface DomainEvent {
    UUID eventId();
    String eventType();
    UUID aggregateId();
    int aggregateVersion();
    Instant occurredAt();
    int schemaVersion();
}
