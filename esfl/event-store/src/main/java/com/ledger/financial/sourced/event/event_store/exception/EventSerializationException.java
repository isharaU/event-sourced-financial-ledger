package com.ledger.financial.sourced.event.event_store.exception;


public class EventSerializationException extends RuntimeException {

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
