package com.ledger.financial.sourced.event.contracts;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME, 
    include = JsonTypeInfo.As.EXISTING_PROPERTY, 
    property = "eventType", 
    visible = true
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AccountCreated.class, name = "AccountCreated"),
    @JsonSubTypes.Type(value = FundsDeposited.class, name = "FundsDeposited"),
    @JsonSubTypes.Type(value = FundsWithdrawn.class, name = "FundsWithdrawn"),
    @JsonSubTypes.Type(value = FundsDebited.class, name = "FundsDebited"),
    @JsonSubTypes.Type(value = FundsCredited.class, name = "FundsCredited"),
    @JsonSubTypes.Type(value = FundsRefunded.class, name = "FundsRefunded"),
    @JsonSubTypes.Type(value = AccountClosed.class, name = "AccountClosed"),
    @JsonSubTypes.Type(value = AccountSuspended.class, name = "AccountSuspended")
})
public sealed interface DomainEvent permits 
    AccountCreated, FundsDeposited, FundsWithdrawn, FundsDebited, FundsCredited, FundsRefunded, AccountClosed, AccountSuspended {
    
    UUID eventId();
    String eventType();
    UUID aggregateId();
    int aggregateVersion();
    Instant occurredAt();
    int schemaVersion();
}
