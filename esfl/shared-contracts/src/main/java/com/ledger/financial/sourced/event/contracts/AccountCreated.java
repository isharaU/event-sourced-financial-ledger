package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record AccountCreated(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("eventType") String eventType,
        @JsonProperty("aggregateId") UUID aggregateId,
        @JsonProperty("aggregateVersion") int aggregateVersion,
        @JsonProperty("occurredAt") Instant occurredAt,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("ownerId") UUID ownerId,
        @JsonProperty("currency") String currency
) implements DomainEvent {
    @JsonCreator
    public AccountCreated {
        if (eventType == null) eventType = "AccountCreated";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
