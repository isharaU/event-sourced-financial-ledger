package com.ledger.financial.sourced.event.event_store.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
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
import com.ledger.financial.sourced.event.event_store.exception.OptimisticConcurrencyException;

@SpringBootTest
@Testcontainers
class EventStoreRepositoryIT {

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
    private EventStoreRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldAppendEventsSuccessfully() {
        UUID aggregateId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();

        DomainEvent event1 = new AccountCreated(
                UUID.randomUUID(),
                "AccountCreated",
                aggregateId,
                1,
                Instant.now(),
                1,
                ownerId,
                "LKR"
        );

        repository.appendEvents(aggregateId, 0, List.of(event1));

        // Verify inserted
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM domain_events WHERE aggregate_id = ?",
                Integer.class,
                aggregateId
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldDetectOccConflictAndRetry() {
        UUID aggregateId = UUID.randomUUID();
        UUID ownerId = UUID.randomUUID();

        DomainEvent event1 = new AccountCreated(
                UUID.randomUUID(),
                "AccountCreated",
                aggregateId,
                1,
                Instant.now(),
                1,
                ownerId,
                "LKR"
        );

        // First append succeeds
        repository.appendEvents(aggregateId, 0, List.of(event1));

        // Create a new event that tries to use the SAME expected version (1)
        // Which means it thinks it is version 1, but the DB already has version 1.
        DomainEvent event2 = new AccountCreated(
                UUID.randomUUID(),
                "AccountCreated",
                aggregateId,
                1,
                Instant.now(),
                1,
                ownerId,
                "LKR"
        );

        // It should throw OptimisticConcurrencyException after retrying 3 times
        List<DomainEvent> eventsToAppend = List.of(event2);
        assertThatThrownBy(() -> repository.appendEvents(aggregateId, 0, eventsToAppend))
                .isInstanceOf(OptimisticConcurrencyException.class)
                .hasMessageContaining("Optimistic concurrency conflict");
    }
}
