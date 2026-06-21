package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public record AccountClosed(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        int aggregateVersion,
        Instant occurredAt,
        int schemaVersion
) implements DomainEvent {
    public AccountClosed {
        if (eventType == null) eventType = "AccountClosed";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
