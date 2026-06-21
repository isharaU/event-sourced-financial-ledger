package com.ledger.financial.sourced.event.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class DomainEventSerializationTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void testAccountCreatedSerialization() throws Exception {
        AccountCreated event = new AccountCreated(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                1,
                Instant.now(),
                0,
                UUID.randomUUID(),
                "USD"
        );
        verifySerialization(event);
    }

    @Test
    void testFundsDepositedSerialization() throws Exception {
        FundsDeposited event = new FundsDeposited(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                2,
                Instant.now(),
                0,
                1000L,
                UUID.randomUUID()
        );
        verifySerialization(event);
    }

    @Test
    void testFundsWithdrawnSerialization() throws Exception {
        FundsWithdrawn event = new FundsWithdrawn(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                3,
                Instant.now(),
                0,
                500L,
                UUID.randomUUID()
        );
        verifySerialization(event);
    }

    @Test
    void testFundsDebitedSerialization() throws Exception {
        FundsDebited event = new FundsDebited(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                4,
                Instant.now(),
                0,
                200L,
                UUID.randomUUID()
        );
        verifySerialization(event);
    }

    @Test
    void testFundsCreditedSerialization() throws Exception {
        FundsCredited event = new FundsCredited(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                5,
                Instant.now(),
                0,
                300L,
                UUID.randomUUID()
        );
        verifySerialization(event);
    }

    @Test
    void testFundsRefundedSerialization() throws Exception {
        FundsRefunded event = new FundsRefunded(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                6,
                Instant.now(),
                0,
                100L,
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        verifySerialization(event);
    }

    @Test
    void testAccountClosedSerialization() throws Exception {
        AccountClosed event = new AccountClosed(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                7,
                Instant.now(),
                0
        );
        verifySerialization(event);
    }

    @Test
    void testAccountSuspendedSerialization() throws Exception {
        AccountSuspended event = new AccountSuspended(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                8,
                Instant.now(),
                0
        );
        verifySerialization(event);
    }

    @Test
    void testPolymorphicDeserialization() throws Exception {
        AccountCreated event = new AccountCreated(
                UUID.randomUUID(),
                null,
                UUID.randomUUID(),
                1,
                Instant.now(),
                0,
                UUID.randomUUID(),
                "GBP"
        );
        
        String json = objectMapper.writeValueAsString(event);
        DomainEvent deserialized = objectMapper.readValue(json, DomainEvent.class);
        
        assertThat(deserialized).isInstanceOf(AccountCreated.class);
        assertThat(deserialized.eventId()).isEqualTo(event.eventId());
        assertThat(deserialized.schemaVersion()).isEqualTo(1);
    }

    private void verifySerialization(DomainEvent original) throws Exception {
        String json = objectMapper.writeValueAsString(original);
        DomainEvent deserialized = objectMapper.readValue(json, original.getClass());

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.schemaVersion()).isEqualTo(1);
    }
}
