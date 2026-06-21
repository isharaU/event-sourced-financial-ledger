package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record FundsRefunded(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("eventType") String eventType,
        @JsonProperty("aggregateId") UUID aggregateId,
        @JsonProperty("aggregateVersion") int aggregateVersion,
        @JsonProperty("occurredAt") Instant occurredAt,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("amount") long amount,
        @JsonProperty("transactionId") UUID transactionId,
        @JsonProperty("originalTransactionId") UUID originalTransactionId
) implements DomainEvent {
    @JsonCreator
    public FundsRefunded {
        if (eventType == null) eventType = "FundsRefunded";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
