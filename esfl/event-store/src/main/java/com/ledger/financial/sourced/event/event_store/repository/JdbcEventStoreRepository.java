package com.ledger.financial.sourced.event.event_store.repository;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.financial.sourced.event.contracts.DomainEvent;
import com.ledger.financial.sourced.event.event_store.exception.EventSerializationException;
import com.ledger.financial.sourced.event.event_store.exception.OptimisticConcurrencyException;

@Repository
public class JdbcEventStoreRepository implements EventStoreRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public JdbcEventStoreRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional(isolation = Isolation.SERIALIZABLE)
    @Retryable(
            retryFor = OptimisticConcurrencyException.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 50, multiplier = 2.0)
    )
    public void appendEvents(UUID aggregateId, int expectedVersion, List<DomainEvent> events) {
        int currentVersion = expectedVersion;

        for (DomainEvent event : events) {
            currentVersion++;
            
            // Validate the event's internal version matches the expected sequential version
            if (event.aggregateVersion() != currentVersion) {
                throw new IllegalArgumentException(
                        String.format("Event version mismatch. Expected %d but got %d", currentVersion, event.aggregateVersion())
                );
            }

            if (!event.aggregateId().equals(aggregateId)) {
                throw new IllegalArgumentException(
                        String.format("Event aggregate ID mismatch. Expected %s but got %s", aggregateId, event.aggregateId())
                );
            }

            try {
                String payload = objectMapper.writeValueAsString(event);
                String checksum = computeChecksum(aggregateId, currentVersion, event.eventType(), payload);

                String sql = """
                        INSERT INTO domain_events (
                            event_id, aggregate_id, aggregate_version, event_type, payload, checksum, schema_version, occurred_at
                        ) VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                        """;

                jdbcTemplate.update(sql,
                        event.eventId(),
                        aggregateId,
                        currentVersion,
                        event.eventType(),
                        payload,
                        checksum,
                        event.schemaVersion(),
                        Timestamp.from(event.occurredAt())
                );
            } catch (DataIntegrityViolationException e) {
                // If it's a unique constraint violation on (aggregate_id, aggregate_version), throw OCC
                throw new OptimisticConcurrencyException(aggregateId, expectedVersion, e);
            } catch (JsonProcessingException e) {
                throw new EventSerializationException("Failed to serialize domain event payload", e);
            }
        }
    }

    private String computeChecksum(UUID aggregateId, int version, String eventType, String payload) {
        try {
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
        } catch (NoSuchAlgorithmException e) {
            throw new EventSerializationException("SHA-256 algorithm not found", e);
        }
    }
}
