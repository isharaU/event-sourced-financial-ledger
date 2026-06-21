package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public record AccountSuspended(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        int aggregateVersion,
        Instant occurredAt,
        int schemaVersion
) implements DomainEvent {
    public AccountSuspended {
        if (eventType == null) eventType = "AccountSuspended";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
