package com.ledger.financial.sourced.event.domain.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

class AccountTest {

    private Account account;
    private final UUID accountId = UUID.randomUUID();
    private final UUID ownerId = UUID.randomUUID();
    private static final String CURRENCY = "USD";
    private static final String ACCOUNT_CREATED = "AccountCreated";

    @BeforeEach
    void setUp() {
        account = new Account();
        AccountCreated createdEvent = new AccountCreated(
                UUID.randomUUID(), ACCOUNT_CREATED, accountId, 1, Instant.now(), 1, ownerId, CURRENCY
        );
        account.apply(createdEvent);
    }

    @Test
    void shouldCreateAccount() {
        Account newAccount = new Account();
        AccountCreated event = newAccount.create(accountId, ownerId, CURRENCY);
        
        assertEquals(accountId, event.aggregateId());
        assertEquals(ownerId, event.ownerId());
        assertEquals(CURRENCY, event.currency());
        assertEquals(ACCOUNT_CREATED, event.eventType());

        newAccount.apply(event);
        assertEquals(accountId, newAccount.getAccountId());
        assertEquals(ownerId, newAccount.getOwnerId());
        assertEquals(CURRENCY, newAccount.getCurrency());
        assertEquals(AccountStatus.ACTIVE, newAccount.getStatus());
        assertEquals(0L, newAccount.getBalance());
        assertEquals(1, newAccount.getAggregateVersion());
    }

    @Test
    void shouldDepositSuccessfully() {
        UUID txId = UUID.randomUUID();
        FundsDeposited event = account.deposit(1000L, txId);
        
        assertEquals(1000L, event.amount());
        assertEquals(txId, event.transactionId());
        
        account.apply(event);
        assertEquals(1000L, account.getBalance());
        assertEquals(2, account.getAggregateVersion());
    }

    @Test
    void shouldThrowWhenDepositAmountInvalid() {
        UUID txId = UUID.randomUUID();
        assertThrows(InvalidAmountException.class, () -> account.deposit(0L, txId));
        assertThrows(InvalidAmountException.class, () -> account.deposit(-100L, txId));
        assertThrows(InvalidAmountException.class, () -> account.deposit(10_000_000_000L, txId));
    }

    @Test
    void shouldThrowWhenDepositToInactiveAccount() {
        account.apply(account.suspend());
        UUID txId1 = UUID.randomUUID();
        assertThrows(AccountNotActiveException.class, () -> account.deposit(100L, txId1));

        account.apply(new AccountClosed(UUID.randomUUID(), "AccountClosed", accountId, 3, Instant.now(), 1));
        UUID txId2 = UUID.randomUUID();
        assertThrows(AccountNotActiveException.class, () -> account.deposit(100L, txId2));
    }

    @Test
    void shouldWithdrawSuccessfully() {
        account.apply(account.deposit(2000L, UUID.randomUUID()));
        
        UUID txId = UUID.randomUUID();
        FundsWithdrawn event = account.withdraw(500L, txId);
        
        assertEquals(500L, event.amount());
        assertEquals(txId, event.transactionId());
        
        account.apply(event);
        assertEquals(1500L, account.getBalance());
        assertEquals(3, account.getAggregateVersion());
    }

    @Test
    void shouldThrowWhenWithdrawAmountInvalid() {
        account.apply(account.deposit(2000L, UUID.randomUUID()));
        UUID txId = UUID.randomUUID();
        assertThrows(InvalidAmountException.class, () -> account.withdraw(0L, txId));
        assertThrows(InvalidAmountException.class, () -> account.withdraw(-50L, txId));
        assertThrows(InvalidAmountException.class, () -> account.withdraw(10_000_000_000L, txId));
    }

    @Test
    void shouldThrowWhenWithdrawInsufficientFunds() {
        account.apply(account.deposit(500L, UUID.randomUUID()));
        UUID txId = UUID.randomUUID();
        assertThrows(InsufficientFundsException.class, () -> account.withdraw(1000L, txId));
    }

    @Test
    void shouldThrowWhenWithdrawFromInactiveAccount() {
        account.apply(account.deposit(500L, UUID.randomUUID()));
        account.apply(account.suspend());
        
        UUID txId = UUID.randomUUID();
        assertThrows(AccountNotActiveException.class, () -> account.withdraw(100L, txId));
    }

    @Test
    void shouldCloseAccountSuccessfully() {
        AccountClosed event = account.close();
        account.apply(event);
        
        assertEquals(AccountStatus.CLOSED, account.getStatus());
        assertEquals(2, account.getAggregateVersion());
    }

    @Test
    void shouldCloseSuspendedAccount() {
        account.apply(account.suspend());
        AccountClosed event = account.close();
        account.apply(event);
        
        assertEquals(AccountStatus.CLOSED, account.getStatus());
    }

    @Test
    void shouldThrowWhenCloseWithNonZeroBalance() {
        account.apply(account.deposit(100L, UUID.randomUUID()));
        assertThrows(AccountClosureNotAllowedException.class, () -> account.close());
    }

    @Test
    void shouldThrowWhenCloseAlreadyClosedAccount() {
        account.apply(account.close());
        assertThrows(AccountClosureNotAllowedException.class, () -> account.close());
    }

    @Test
    void shouldSuspendAccountSuccessfully() {
        AccountSuspended event = account.suspend();
        account.apply(event);
        
        assertEquals(AccountStatus.SUSPENDED, account.getStatus());
        assertEquals(2, account.getAggregateVersion());
    }

    @Test
    void shouldThrowWhenSuspendInactiveAccount() {
        account.apply(account.suspend());
        assertThrows(AccountNotActiveException.class, () -> account.suspend());
        
        account = new Account();
        account.apply(new AccountCreated(UUID.randomUUID(), ACCOUNT_CREATED, accountId, 1, Instant.now(), 1, ownerId, CURRENCY));
        account.apply(account.close());
        assertThrows(AccountNotActiveException.class, () -> account.suspend());
    }

    @Test
    void shouldIgnoreUnknownDomainEventOnApply() {
        DomainEvent dummyEvent = new com.ledger.financial.sourced.event.contracts.FundsDebited(
                UUID.randomUUID(), "FundsDebited", accountId, 99, Instant.now(), 1, 100L, UUID.randomUUID()
        );
        
        int oldVersion = account.getAggregateVersion();
        AccountStatus oldStatus = account.getStatus();
        long oldBalance = account.getBalance();
        
        account.apply(dummyEvent);
        
        assertEquals(99, account.getAggregateVersion());
        assertEquals(oldStatus, account.getStatus());
        assertEquals(oldBalance, account.getBalance());
    }
}
