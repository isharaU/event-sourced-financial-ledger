package com.ledger.financial.sourced.event.event_store.repository;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.financial.sourced.event.contracts.DomainEvent;
import com.ledger.financial.sourced.event.domain.account.Account;
import com.ledger.financial.sourced.event.event_store.exception.EventSerializationException;
import com.ledger.financial.sourced.event.event_store.exception.TamperedEventException;

/**
 * JDBC-based implementation of {@link AggregateLoader}.
 */
@Repository
public class JdbcAggregateLoader implements AggregateLoader {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public JdbcAggregateLoader(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper.copy()
                .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD, 
                               com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY);
    }

    @Override
    public Optional<Account> load(UUID accountId) {
        Optional<SnapshotRow> latestSnapshot = loadLatestSnapshot(accountId);

        Account account;
        int startVersion;

        if (latestSnapshot.isPresent()) {
            try {
                account = objectMapper.readValue(latestSnapshot.get().data(), Account.class);
                startVersion = latestSnapshot.get().version();
            } catch (JsonProcessingException e) {
                throw new EventSerializationException("Failed to deserialize account snapshot", e);
            }
        } else {
            account = new Account();
            startVersion = 0;
        }

        List<EventRow> events = loadEventsAfterVersion(accountId, startVersion);

        if (startVersion == 0 && events.isEmpty()) {
            return Optional.empty();
        }

        for (EventRow row : events) {
            String computedChecksum = computeChecksum(accountId, row.version(), row.eventType(), row.payload());
            if (!computedChecksum.equalsIgnoreCase(row.checksum())) {
                throw new TamperedEventException(accountId, row.version());
            }

            try {
                DomainEvent event = objectMapper.readValue(row.payload(), DomainEvent.class);
                account.apply(event);
            } catch (JsonProcessingException e) {
                throw new EventSerializationException("Failed to deserialize domain event", e);
            }
        }

        return Optional.of(account);
    }

    private Optional<SnapshotRow> loadLatestSnapshot(UUID accountId) {
        String sql = """
                SELECT aggregate_version, snapshot_data
                FROM account_snapshots
                WHERE aggregate_id = ?
                ORDER BY aggregate_version DESC
                LIMIT 1
                """;

        List<SnapshotRow> results = jdbcTemplate.query(sql, (rs, rowNum) -> new SnapshotRow(
                rs.getInt("aggregate_version"),
                rs.getString("snapshot_data")
        ), accountId);

        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    private List<EventRow> loadEventsAfterVersion(UUID accountId, int version) {
        String sql = """
                SELECT event_type, payload, checksum, aggregate_version
                FROM domain_events
                WHERE aggregate_id = ? AND aggregate_version > ?
                ORDER BY aggregate_version ASC
                """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> new EventRow(
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getString("checksum"),
                rs.getInt("aggregate_version")
        ), accountId, version);
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

    static record SnapshotRow(int version, String data) {}

    static record EventRow(String eventType, String payload, String checksum, int version) {}
}
