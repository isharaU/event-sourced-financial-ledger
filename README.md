# Event-Sourced Financial Ledger

An **Event-Sourced Financial Ledger** backend system designed to manage digital wallet and banking-style operations with strong guarantees around consistency, auditability, and scalability.

## Architecture Highlights

This system adopts two proven architectural patterns:

*   **Event Sourcing**: All state changes are persisted as an immutable, ordered sequence of domain events. The current state of any account is derived by replaying its event history.
*   **CQRS (Command Query Responsibility Segregation)**: The write path (commands that mutate state) is fully separated from the read path (queries that return projections), enabling each to be optimized independently.
*   **Apache Kafka**: Serves as the durable event streaming backbone, decoupling producers (the command/write side) from consumers (read model projections, notification services, audit systems).

## Key Features

*   **Core Financial Operations**: Account creation, deposits, withdrawals, and cross-account fund transfers (using sagas).
*   **Tamper-Evident Audit Trail**: Every financial operation is an immutable event appended to an ordered log.
*   **Time-Travel Queries**: Reconstruct the exact state of any account at any point in history.
*   **Idempotency & Concurrency**: Built-in idempotency to prevent duplicate processing, and optimistic concurrency control to prevent conflicting concurrent updates.
*   **Independent Scaling**: Write and read workloads scale independently, reducing operational costs and improving resilience.

## Documentation

Comprehensive project documentation is available in the `docs/` directory:

*   [Business Requirements Document (BRD)](docs/01_BRD_event_sourced_financial_ledger.md)
*   [Software Requirements Specification (SRS)](docs/02_SRS_event_sourced_financial_ledger.md)
*   [High-Level Design (HLD)](docs/03_HLD_event_sourced_financial_ledger.md)
*   [Agile Delivery Plan (ADP)](docs/04_ADP_event_sourced_financial_ledger.md)

## Future Enhancements

*   Multi-Currency Support
*   Fraud Detection Engine
*   Scheduled / Recurring Transfers
*   Account Statements
*   Notification Service