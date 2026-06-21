package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public record AccountCreated(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        int aggregateVersion,
        Instant occurredAt,
        int schemaVersion,
        UUID ownerId,
        String currency
) implements DomainEvent {
    public AccountCreated {
        if (eventType == null) eventType = "AccountCreated";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
