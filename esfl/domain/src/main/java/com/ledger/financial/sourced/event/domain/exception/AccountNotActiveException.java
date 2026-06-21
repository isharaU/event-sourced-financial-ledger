package com.ledger.financial.sourced.event.domain.exception;

public class AccountNotActiveException extends RuntimeException {
    public AccountNotActiveException(String message) {
        super(message);
    }
}
