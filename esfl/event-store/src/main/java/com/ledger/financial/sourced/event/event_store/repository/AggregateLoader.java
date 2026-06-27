package com.ledger.financial.sourced.event.event_store.repository;

import java.util.Optional;
import java.util.UUID;

import com.ledger.financial.sourced.event.domain.account.Account;

/**
 * Service/Repository responsible for loading and hydrating the Account aggregate
 * state from the event store (including snapshot-assisted replay).
 */
public interface AggregateLoader {

    /**
     * Loads and reconstructs the current state of an Account aggregate by replaying its event history.
     * If a snapshot exists, it loads the latest snapshot first and applies only events after the snapshot version.
     *
     * @param accountId The unique identifier of the account aggregate
     * @return An Optional containing the reconstructed Account aggregate, or Optional.empty() if no events exist
     * @throws com.ledger.financial.sourced.event.event_store.exception.TamperedEventException if checksum verification fails
     */
    Optional<Account> load(UUID accountId);
}
