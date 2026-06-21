package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record FundsDebited(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("eventType") String eventType,
        @JsonProperty("aggregateId") UUID aggregateId,
        @JsonProperty("aggregateVersion") int aggregateVersion,
        @JsonProperty("occurredAt") Instant occurredAt,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("amount") long amount,
        @JsonProperty("transactionId") UUID transactionId
) implements DomainEvent {
    @JsonCreator
    public FundsDebited {
        if (eventType == null) eventType = "FundsDebited";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
