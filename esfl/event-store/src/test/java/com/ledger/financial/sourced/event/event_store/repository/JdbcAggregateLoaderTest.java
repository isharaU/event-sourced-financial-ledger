package com.ledger.financial.sourced.event.event_store.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ledger.financial.sourced.event.contracts.AccountCreated;
import com.ledger.financial.sourced.event.contracts.DomainEvent;
import com.ledger.financial.sourced.event.contracts.FundsDeposited;
import com.ledger.financial.sourced.event.domain.account.Account;
import com.ledger.financial.sourced.event.domain.account.AccountStatus;
import com.ledger.financial.sourced.event.event_store.exception.EventSerializationException;
import com.ledger.financial.sourced.event.event_store.exception.TamperedEventException;

class JdbcAggregateLoaderTest {

    private JdbcTemplate jdbcTemplate;
    private ObjectMapper objectMapper;
    private JdbcAggregateLoader loader;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        loader = new JdbcAggregateLoader(jdbcTemplate, objectMapper);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnEmptyWhenNoSnapshotAndNoEventsExist() {
        UUID accountId = UUID.randomUUID();

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId)))
                .thenReturn(Collections.emptyList());

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId), eq(0)))
                .thenReturn(Collections.emptyList());

        Optional<Account> result = loader.load(accountId);

        assertThat(result).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHydrateFromEventsOnlyWhenNoSnapshotExists() throws Exception {
        UUID accountId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        String currency = "USD";

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId)))
                .thenReturn(Collections.emptyList());

        AccountCreated createdEvent = new AccountCreated(UUID.randomUUID(), "AccountCreated", accountId, 1, Instant.now(), 1, ownerId, currency);
        FundsDeposited depositedEvent = new FundsDeposited(UUID.randomUUID(), "FundsDeposited", accountId, 2, Instant.now(), 1, 1000L, UUID.randomUUID());

        String createdPayload = objectMapper.writeValueAsString(createdEvent);
        String depositedPayload = objectMapper.writeValueAsString(depositedEvent);

        String createdChecksum = computeChecksum(accountId, 1, "AccountCreated", createdPayload);
        String depositedChecksum = computeChecksum(accountId, 2, "FundsDeposited", depositedPayload);

        List<JdbcAggregateLoader.EventRow> mockEventRows = List.of(
                createEventRow("AccountCreated", createdPayload, createdChecksum, 1),
                createEventRow("FundsDeposited", depositedPayload, depositedChecksum, 2)
        );

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId), eq(0)))
                .thenReturn(mockEventRows);

        Optional<Account> result = loader.load(accountId);

        assertThat(result).isPresent();
        Account account = result.get();
        assertThat(account.getAccountId()).isEqualTo(accountId);
        assertThat(account.getOwnerId()).isEqualTo(ownerId);
        assertThat(account.getCurrency()).isEqualTo(currency);
        assertThat(account.getBalance()).isEqualTo(1000L);
        assertThat(account.getAggregateVersion()).isEqualTo(2);
        assertThat(account.getStatus()).isEqualTo(AccountStatus.ACTIVE);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHydrateFromSnapshotAndSubsequentEvents() throws Exception {
        UUID accountId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        String currency = "USD";

        Account snapshotAccount = new Account();
        snapshotAccount.apply(new AccountCreated(UUID.randomUUID(), "AccountCreated", accountId, 1, Instant.now(), 1, ownerId, currency));
        for (int i = 2; i <= 5; i++) {
            snapshotAccount.apply(new FundsDeposited(UUID.randomUUID(), "FundsDeposited", accountId, i, Instant.now(), 1, 500L, UUID.randomUUID()));
        }

        // Configure ObjectMapper used inside loader to support deserializing Account private fields
        // Since loader copies the objectMapper in constructor, we configure the one passed to it.
        String snapshotData = objectMapper.copy()
                .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD, com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
                .writeValueAsString(snapshotAccount);

        List<JdbcAggregateLoader.SnapshotRow> mockSnapshotRows = List.of(createSnapshotRow(5, snapshotData));

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId)))
                .thenReturn(mockSnapshotRows);

        FundsDeposited depositedEvent = new FundsDeposited(UUID.randomUUID(), "FundsDeposited", accountId, 6, Instant.now(), 1, 1000L, UUID.randomUUID());
        String depositedPayload = objectMapper.writeValueAsString(depositedEvent);
        String depositedChecksum = computeChecksum(accountId, 6, "FundsDeposited", depositedPayload);

        List<JdbcAggregateLoader.EventRow> mockEventRows = List.of(
                createEventRow("FundsDeposited", depositedPayload, depositedChecksum, 6)
        );

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId), eq(5)))
                .thenReturn(mockEventRows);

        Optional<Account> result = loader.load(accountId);

        assertThat(result).isPresent();
        Account account = result.get();
        assertThat(account.getBalance()).isEqualTo(3000L); // 2000 from snapshot + 1000 from event
        assertThat(account.getAggregateVersion()).isEqualTo(6);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowTamperedEventExceptionWhenChecksumMismatch() throws Exception {
        UUID accountId = UUID.randomUUID();

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId)))
                .thenReturn(Collections.emptyList());

        List<JdbcAggregateLoader.EventRow> mockEventRows = List.of(
                createEventRow("AccountCreated", "{}", "wrong-checksum", 1)
        );

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId), eq(0)))
                .thenReturn(mockEventRows);

        assertThatThrownBy(() -> loader.load(accountId))
                .isInstanceOf(TamperedEventException.class)
                .hasMessageContaining("Event tampering detected");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowEventSerializationExceptionWhenSnapshotDeserializationFails() {
        UUID accountId = UUID.randomUUID();

        String snapshotData = "invalid-json";
        List<JdbcAggregateLoader.SnapshotRow> mockSnapshotRows = List.of(createSnapshotRow(5, snapshotData));

        when(jdbcTemplate.query(any(String.class), any(RowMapper.class), eq(accountId)))
                .thenReturn(mockSnapshotRows);

        assertThatThrownBy(() -> loader.load(accountId))
                .isInstanceOf(EventSerializationException.class)
                .hasMessageContaining("Failed to deserialize account snapshot");
    }

    private JdbcAggregateLoader.SnapshotRow createSnapshotRow(int version, String data) {
        return new JdbcAggregateLoader.SnapshotRow(version, data);
    }

    private JdbcAggregateLoader.EventRow createEventRow(String eventType, String payload, String checksum, int version) {
        return new JdbcAggregateLoader.EventRow(eventType, payload, checksum, version);
    }

    private String computeChecksum(UUID aggregateId, int version, String eventType, String payload) throws Exception {
        String input = aggregateId.toString() + version + eventType + payload;
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
