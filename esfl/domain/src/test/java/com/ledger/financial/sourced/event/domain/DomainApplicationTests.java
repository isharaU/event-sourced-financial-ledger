package com.ledger.financial.sourced.event.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Basic sanity test for the domain module.
 * Uses plain JUnit 5 — no Spring context required.
 */
class DomainApplicationTests {

	@Test
	void domainModuleLoads() {
		assertDoesNotThrow(() -> {
			// domain module compiles and loads without Spring
		});
	}

}
