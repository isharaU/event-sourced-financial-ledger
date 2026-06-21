package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public record FundsWithdrawn(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        int aggregateVersion,
        Instant occurredAt,
        int schemaVersion,
        long amount,
        UUID transactionId
) implements DomainEvent {
    public FundsWithdrawn {
        if (eventType == null) eventType = "FundsWithdrawn";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
