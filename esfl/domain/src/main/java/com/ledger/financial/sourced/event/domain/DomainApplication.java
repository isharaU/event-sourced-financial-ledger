package com.ledger.financial.sourced.event.domain;

/**
 * Marker class for the domain module.
 * This is a pure Java library — no Spring Boot entry point.
 * Other modules (command-api, query-api) depend on this jar.
 */
public final class DomainApplication {
    private DomainApplication() {
        // utility class – not instantiable
    }
}
