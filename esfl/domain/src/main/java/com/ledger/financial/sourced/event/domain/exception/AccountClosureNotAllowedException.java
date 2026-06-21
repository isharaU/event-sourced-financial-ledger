package com.ledger.financial.sourced.event.domain.exception;

public class AccountClosureNotAllowedException extends RuntimeException {
    public AccountClosureNotAllowedException(String message) {
        super(message);
    }
}
