package com.ledger.financial.sourced.event.domain.account;

import java.time.Instant;
import java.util.UUID;

import com.ledger.financial.sourced.event.contracts.AccountClosed;
import com.ledger.financial.sourced.event.contracts.AccountCreated;
import com.ledger.financial.sourced.event.contracts.AccountSuspended;
import com.ledger.financial.sourced.event.contracts.DomainEvent;
import com.ledger.financial.sourced.event.contracts.FundsDeposited;
import com.ledger.financial.sourced.event.contracts.FundsWithdrawn;
import com.ledger.financial.sourced.event.domain.exception.AccountClosureNotAllowedException;
import com.ledger.financial.sourced.event.domain.exception.AccountNotActiveException;
import com.ledger.financial.sourced.event.domain.exception.InsufficientFundsException;
import com.ledger.financial.sourced.event.domain.exception.InvalidAmountException;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class Account {
    private UUID accountId;
    private UUID ownerId;
    private long balance;
    private String currency;
    private AccountStatus status;
    private int aggregateVersion;

    public AccountCreated create(UUID accountId, UUID ownerId, String currency) {
        return new AccountCreated(
                UUID.randomUUID(), "AccountCreated", accountId, 1, Instant.now(), 1, ownerId, currency
        );
    }

    public FundsDeposited deposit(long amount, UUID transactionId) {
        if (amount <= 0 || amount > 9_999_999_999L) {
            throw new InvalidAmountException("Deposit amount must be between 1 and 9,999,999,999 cents.");
        }
        if (this.status != AccountStatus.ACTIVE) {
            throw new AccountNotActiveException("Cannot deposit to an inactive account.");
        }
        return new FundsDeposited(
                UUID.randomUUID(), "FundsDeposited", this.accountId, this.aggregateVersion + 1, Instant.now(), 1, amount, transactionId
        );
    }

    public FundsWithdrawn withdraw(long amount, UUID transactionId) {
        if (amount <= 0 || amount > 9_999_999_999L) {
            throw new InvalidAmountException("Withdrawal amount must be between 1 and 9,999,999,999 cents.");
        }
        if (this.status != AccountStatus.ACTIVE) {
            throw new AccountNotActiveException("Cannot withdraw from an inactive account.");
        }
        if (this.balance - amount < 0) {
            throw new InsufficientFundsException("Insufficient funds for withdrawal.");
        }
        return new FundsWithdrawn(
                UUID.randomUUID(), "FundsWithdrawn", this.accountId, this.aggregateVersion + 1, Instant.now(), 1, amount, transactionId
        );
    }

    public AccountClosed close() {
        if (this.status == AccountStatus.CLOSED) {
            throw new AccountClosureNotAllowedException("Account is already closed.");
        }
        if (this.balance != 0) {
            throw new AccountClosureNotAllowedException("Account balance must be zero to close.");
        }
        return new AccountClosed(
                UUID.randomUUID(), "AccountClosed", this.accountId, this.aggregateVersion + 1, Instant.now(), 1
        );
    }

    public AccountSuspended suspend() {
        if (this.status != AccountStatus.ACTIVE) {
            throw new AccountNotActiveException("Only active accounts can be suspended.");
        }
        return new AccountSuspended(
                UUID.randomUUID(), "AccountSuspended", this.accountId, this.aggregateVersion + 1, Instant.now(), 1
        );
    }

    public void apply(DomainEvent event) {
        if (event instanceof AccountCreated e) {
            this.accountId = e.aggregateId();
            this.ownerId = e.ownerId();
            this.currency = e.currency();
            this.balance = 0;
            this.status = AccountStatus.ACTIVE;
        } else if (event instanceof FundsDeposited e) {
            this.balance += e.amount();
        } else if (event instanceof FundsWithdrawn e) {
            this.balance -= e.amount();
        } else if (event instanceof AccountClosed) {
            this.status = AccountStatus.CLOSED;
        } else if (event instanceof AccountSuspended) {
            this.status = AccountStatus.SUSPENDED;
        }
        this.aggregateVersion = event.aggregateVersion();
    }
}
