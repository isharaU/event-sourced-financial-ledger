package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

public record FundsDeposited(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        int aggregateVersion,
        Instant occurredAt,
        int schemaVersion,
        long amount,
        UUID transactionId
) implements DomainEvent {
    public FundsDeposited {
        if (eventType == null) eventType = "FundsDeposited";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
