package com.ledger.financial.sourced.event.event_store.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.ledger.financial.sourced.event.contracts.AccountCreated;
import com.ledger.financial.sourced.event.contracts.DomainEvent;
import com.ledger.financial.sourced.event.contracts.FundsDeposited;
import com.ledger.financial.sourced.event.domain.account.Account;
import com.ledger.financial.sourced.event.domain.account.AccountStatus;
import com.ledger.financial.sourced.event.event_store.exception.TamperedEventException;

@SpringBootTest
@Testcontainers
class AggregateLoaderIT {

    @SuppressWarnings("resource")
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("app_db")
            .withUsername("app_user")
            .withPassword("password");

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private EventStoreRepository eventStoreRepository;

    @Autowired
    private AggregateLoader aggregateLoader;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldReturnEmptyForNonexistentAccount() {
        UUID accountId = UUID.randomUUID();
        Optional<Account> result = aggregateLoader.load(accountId);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldVerifyConsistencyAfterReplaying100Events() {
        UUID accountId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        String currency = "LKR";

        DomainEvent createdEvent = new AccountCreated(
                UUID.randomUUID(),
                "AccountCreated",
                accountId,
                1,
                Instant.now(),
                1,
                ownerId,
                currency
        );
        eventStoreRepository.appendEvents(accountId, 0, List.of(createdEvent));

        long totalDeposited = 0;
        int currentVersion = 1;
        for (int i = 0; i < 99; i++) {
            long depositAmount = 100L;
            totalDeposited += depositAmount;
            DomainEvent depositEvent = new FundsDeposited(
                    UUID.randomUUID(),
                    "FundsDeposited",
                    accountId,
                    currentVersion + 1,
                    Instant.now(),
                    1,
                    depositAmount,
                    UUID.randomUUID()
            );
            eventStoreRepository.appendEvents(accountId, currentVersion, List.of(depositEvent));
            currentVersion++;
        }

        Optional<Account> loadedAccountOpt = aggregateLoader.load(accountId);

        assertThat(loadedAccountOpt).isPresent();
        Account account = loadedAccountOpt.get();
        assertThat(account.getAccountId()).isEqualTo(accountId);
        assertThat(account.getOwnerId()).isEqualTo(ownerId);
        assertThat(account.getCurrency()).isEqualTo(currency);
        assertThat(account.getBalance()).isEqualTo(totalDeposited);
        assertThat(account.getAggregateVersion()).isEqualTo(100);
        assertThat(account.getStatus()).isEqualTo(AccountStatus.ACTIVE);
    }

    @Test
    void shouldLoadSnapshotFirstThenReplaySubsequentEvents() {
        UUID accountId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();
        String currency = "LKR";

        String snapshotData = String.format(
                "{\"accountId\":\"%s\",\"ownerId\":\"%s\",\"balance\":5000,\"currency\":\"%s\",\"status\":\"ACTIVE\",\"aggregateVersion\":5}",
                accountId, ownerId, currency
        );

        jdbcTemplate.update(
                "INSERT INTO account_snapshots (snapshot_id, aggregate_id, aggregate_version, snapshot_data) VALUES (?, ?, ?, ?::jsonb)",
                UUID.randomUUID(),
                accountId,
                5,
                snapshotData
        );

        String eventId = UUID.randomUUID().toString();
        String payload = "{\"amount\":1500,\"transactionId\":\"" + UUID.randomUUID() + "\"}";
        String checksumInput = accountId.toString() + "6" + "FundsDeposited" + payload;
        String checksum = computeSha256(checksumInput);

        jdbcTemplate.update(
                "INSERT INTO domain_events (event_id, aggregate_id, aggregate_version, event_type, payload, checksum) VALUES (?::uuid, ?::uuid, ?, ?, ?::jsonb, ?)",
                eventId,
                accountId,
                6,
                "FundsDeposited",
                payload,
                checksum
        );

        Optional<Account> result = aggregateLoader.load(accountId);

        assertThat(result).isPresent();
        Account account = result.get();
        assertThat(account.getAccountId()).isEqualTo(accountId);
        assertThat(account.getOwnerId()).isEqualTo(ownerId);
        assertThat(account.getCurrency()).isEqualTo(currency);
        assertThat(account.getBalance()).isEqualTo(6500L);
        assertThat(account.getAggregateVersion()).isEqualTo(6);
    }

    @Test
    void shouldThrowTamperedEventExceptionWhenChecksumMismatch() {
        UUID accountId = UUID.randomUUID();

        jdbcTemplate.update(
                "INSERT INTO domain_events (event_id, aggregate_id, aggregate_version, event_type, payload, checksum) VALUES (?::uuid, ?::uuid, ?, ?, ?::jsonb, ?)",
                UUID.randomUUID(),
                accountId,
                1,
                "AccountCreated",
                "{}",
                "wrong-checksum"
        );

        assertThatThrownBy(() -> aggregateLoader.load(accountId))
                .isInstanceOf(TamperedEventException.class)
                .hasMessageContaining("Event tampering detected");
    }

    private String computeSha256(String input) {
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
