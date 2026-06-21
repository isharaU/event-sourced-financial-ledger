package com.ledger.financial.sourced.event.contracts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public record AccountClosed(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("eventType") String eventType,
        @JsonProperty("aggregateId") UUID aggregateId,
        @JsonProperty("aggregateVersion") int aggregateVersion,
        @JsonProperty("occurredAt") Instant occurredAt,
        @JsonProperty("schemaVersion") int schemaVersion
) implements DomainEvent {
    @JsonCreator
    public AccountClosed {
        if (eventType == null) eventType = "AccountClosed";
        if (schemaVersion == 0) schemaVersion = 1;
    }
}
