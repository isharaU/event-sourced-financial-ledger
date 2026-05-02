# Business Requirements Document (BRD)
## Event-Sourced Financial Ledger using CQRS

---

| Field | Detail |
|---|---|
| **Document Version** | 1.0 |
| **Status** | Draft |
| **Date** | 2026-05-02 |
| **Author** | Solutions Architecture Team |
| **Audience** | Engineering, Product, Compliance, QA |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Goals & Non-Goals](#3-goals--non-goals)
4. [Stakeholders](#4-stakeholders)
5. [Functional Requirements](#5-functional-requirements)
6. [Non-Functional Requirements](#6-non-functional-requirements)
7. [Domain Model](#7-domain-model-conceptual)
8. [Event Design](#8-event-design)
9. [CQRS Architecture Overview](#9-cqrs-architecture-overview)
10. [Data Flow (Step-by-Step)](#10-data-flow-step-by-step)
11. [Edge Cases & Constraints](#11-edge-cases--constraints)
12. [Assumptions](#12-assumptions)
13. [Future Enhancements](#13-future-enhancements)

---

## 1. Executive Summary

This document defines the business and technical requirements for an **Event-Sourced Financial Ledger** — a backend system designed to manage digital wallet and banking-style operations with strong guarantees around consistency, auditability, and scalability.

The system adopts two proven architectural patterns:

- **Event Sourcing** — All state changes are persisted as an immutable, ordered sequence of domain events. The current state of any account is derived by replaying its event history.
- **CQRS (Command Query Responsibility Segregation)** — The write path (commands that mutate state) is fully separated from the read path (queries that return projections), enabling each to be optimized independently.

**Apache Kafka** serves as the durable event streaming backbone, decoupling producers (the command/write side) from consumers (read model projections, notification services, audit systems).

### Business Value

- Provides a **tamper-evident, fully auditable** trail of every financial operation — a regulatory and compliance necessity.
- Enables **time-travel queries** — reconstruct the exact state of any account at any point in history.
- Supports **high-throughput, concurrent** transaction processing without sacrificing correctness.
- Scales write and read workloads independently, reducing operational costs and improving resilience.

---

## 2. Problem Statement

Traditional CRUD-based ledger systems suffer from several critical shortcomings when applied to financial domains:

| Problem | Impact |
|---|---|
| **Mutable state** — records are overwritten in place | No history; impossible to audit what happened and when |
| **Monolithic read/write model** | Read-heavy loads degrade write performance; schema must serve all use cases |
| **No built-in event history** | Regulatory compliance (e.g., audits, dispute resolution) requires expensive workarounds |
| **Tight coupling** | A slow downstream system (e.g., reporting) blocks the transaction pipeline |
| **Race conditions** | Concurrent balance updates without strict ordering can lead to double-spends or negative balances |
| **State reconstruction** | Recovering from data corruption requires full database backups; partial recovery is not possible |

This system directly addresses all of the above by treating every financial operation as an **immutable event** appended to an ordered log — the single source of truth from which all derived state is built.

---

## 3. Goals & Non-Goals

### 3.1 Goals

- **G1** — Provide a reliable API for core financial operations: account creation, deposit, withdrawal, and fund transfer.
- **G2** — Persist all domain events immutably in an append-only event store.
- **G3** — Propagate events through Apache Kafka to decouple write operations from downstream consumers.
- **G4** — Maintain eventually-consistent read models (projections) for querying account balances and transaction history.
- **G5** — Support full event replay to reconstruct account state at any point in time.
- **G6** — Enforce idempotency to prevent duplicate processing of commands.
- **G7** — Guarantee optimistic concurrency control to prevent conflicting concurrent updates.
- **G8** — Provide a complete audit log accessible to compliance and operations teams.

### 3.2 Non-Goals

- **NG1** — This system does **not** implement a payment gateway or integrate with external banking rails (e.g., SWIFT, ACH, SEPA).
- **NG2** — Multi-currency exchange rates and FX conversion are **out of scope** for v1.
- **NG3** — End-user mobile/web front-end applications are **not** part of this document.
- **NG4** — Fraud detection and anti-money laundering (AML) rule engines are **not** included in v1 (noted as a future enhancement).
- **NG5** — KYC/identity verification workflows are **not** in scope.
- **NG6** — This document does **not** prescribe a specific programming language or database technology; it defines requirements and architecture.

---

## 4. Stakeholders

| Stakeholder | Role | Primary Concern |
|---|---|---|
| **End Users** | Account holders performing transactions | Fast, reliable, and correct balance operations |
| **Backend Engineers** | Build and maintain the system | Clear API contracts, event schemas, testability |
| **Product Manager** | Defines feature scope and priorities | Business alignment, delivery timelines |
| **Compliance Officer** | Regulatory oversight | Full audit trail, immutability, data retention |
| **Financial Auditor** | External / internal audit | Event replay, historical queries, tamper-evidence |
| **DevOps / SRE** | Infrastructure and reliability | Observability, uptime SLAs, disaster recovery |
| **QA Engineer** | Test coverage | Testable requirements, edge case documentation |
| **Data Analyst** | Reporting and insights | Read model availability, query performance |

---

## 5. Functional Requirements

### 5.1 Account Management

| ID | Requirement |
|---|---|
| FR-ACC-01 | The system shall allow the creation of a new account with a unique account ID, owner name, and initial balance of zero. |
| FR-ACC-02 | The system shall reject account creation requests where the account ID already exists. |
| FR-ACC-03 | The system shall allow an account to be closed, provided its balance is zero at the time of closure. |
| FR-ACC-04 | The system shall maintain a queryable read model of all accounts, including current balance and status. |

### 5.2 Transaction Processing

| ID | Requirement |
|---|---|
| FR-TXN-01 | The system shall allow a deposit of a positive monetary amount to an active account. |
| FR-TXN-02 | The system shall reject deposits of zero or negative amounts. |
| FR-TXN-03 | The system shall allow a withdrawal from an active account, provided sufficient funds are available. |
| FR-TXN-04 | The system shall reject withdrawals that would result in a negative balance. |
| FR-TXN-05 | The system shall allow a fund transfer from one active account to another, atomically debiting the source and crediting the destination. |
| FR-TXN-06 | The system shall reject a transfer if the source account has insufficient funds. |
| FR-TXN-07 | The system shall reject any transaction against a closed or suspended account. |
| FR-TXN-08 | Each transaction shall be assigned a globally unique transaction ID. |

### 5.3 Event Storage

| ID | Requirement |
|---|---|
| FR-EVT-01 | Every state-changing operation shall produce one or more domain events persisted to an append-only event store. |
| FR-EVT-02 | Events shall be stored with a monotonically increasing sequence number scoped to each aggregate (account). |
| FR-EVT-03 | Events shall be immutable once written; no update or delete operations shall be permitted on stored events. |
| FR-EVT-04 | Each event shall carry a timestamp (UTC), event type, aggregate ID, sequence number, correlation ID, and payload. |

### 5.4 Event Streaming (Kafka)

| ID | Requirement |
|---|---|
| FR-STR-01 | All committed domain events shall be published to the appropriate Kafka topic. |
| FR-STR-02 | Kafka topics shall be partitioned by account ID to preserve per-account event ordering. |
| FR-STR-03 | Event consumers (projection builders, audit service) shall be independent Kafka consumer groups. |
| FR-STR-04 | The system shall guarantee at-least-once delivery; consumers shall be idempotent. |

### 5.5 Event Replay

| ID | Requirement |
|---|---|
| FR-RPL-01 | The system shall support replaying all events for a given account to reconstruct its current state. |
| FR-RPL-02 | The system shall support replaying events up to a specified sequence number or timestamp to reconstruct historical state. |
| FR-RPL-03 | Snapshots of aggregate state may be created at configurable intervals to optimise replay performance. |
| FR-RPL-04 | Read model projections shall be rebuildable by replaying events from the event store. |

### 5.6 Read Model Queries

| ID | Requirement |
|---|---|
| FR-QRY-01 | The system shall expose a query endpoint to retrieve the current balance of an account. |
| FR-QRY-02 | The system shall expose a query endpoint to retrieve the full transaction history of an account, with pagination. |
| FR-QRY-03 | The system shall expose a query endpoint to retrieve a single transaction by its transaction ID. |
| FR-QRY-04 | The system shall expose a query endpoint listing all transactions within a date range for an account. |
| FR-QRY-05 | Read model queries shall not impact the write-side performance. |

### 5.7 Audit & Compliance

| ID | Requirement |
|---|---|
| FR-AUD-01 | The system shall retain all events indefinitely (or per configured data retention policy). |
| FR-AUD-02 | An audit log query shall allow filtering events by account ID, event type, date range, and operator ID. |
| FR-AUD-03 | Every command submitted shall record the identity of the requesting principal (user or service). |

---

## 6. Non-Functional Requirements

### 6.1 Consistency

- The write side shall enforce **strong consistency** within a single aggregate (account) using optimistic concurrency control (version/sequence number checks).
- The read side is **eventually consistent** with the write side; the acceptable propagation lag is < 500ms under normal load.
- Cross-account transfers shall use a **saga / process manager** pattern to coordinate two aggregate updates reliably without distributed transactions.

### 6.2 Performance

| Metric | Target |
|---|---|
| Command processing (deposit/withdraw) | p99 latency < 200ms |
| Fund transfer (end-to-end) | p99 latency < 500ms |
| Read model query (balance) | p99 latency < 50ms |
| Event store write throughput | ≥ 5,000 events/second per partition |
| Kafka consumer lag | < 1 second under steady-state load |

### 6.3 Scalability

- The command side shall scale horizontally; multiple command handler instances may run concurrently.
- Kafka partitioning by account ID ensures per-account ordering while enabling parallel processing across accounts.
- Read model stores shall support horizontal read replicas.
- The system shall support at least **1 million active accounts** and **50 million events** without architectural changes.

### 6.4 Fault Tolerance & Availability

- The event store shall be replicated with a minimum replication factor of 3.
- Kafka shall be configured with a replication factor of 3 and `min.insync.replicas = 2`.
- A single node failure shall not result in data loss or service unavailability.
- The system shall achieve **99.9% monthly uptime** (≤ 43 minutes downtime/month) for the write API.
- Failed commands shall be retried with exponential backoff; unrecoverable failures shall be routed to a dead-letter queue (DLQ).

### 6.5 Security

- All API endpoints shall require authenticated and authorised requests (e.g., JWT / OAuth 2.0).
- A user shall only access their own account data unless granted explicit operator-level permissions.
- All data in transit shall be encrypted (TLS 1.2+).
- All data at rest (event store, Kafka topics) shall be encrypted.
- Sensitive fields (e.g., account holder PII) shall not be embedded in event payloads; use references to a secure identity store.

### 6.6 Auditability

- Every event in the event store shall be cryptographically hashable for tamper detection.
- The system shall provide tooling to verify event store integrity (hash chain validation).
- Event data shall be retained for a minimum of **7 years** to satisfy financial regulatory requirements.

---

## 7. Domain Model (Conceptual)

### 7.1 Core Entities

**Account (Aggregate Root)**
- The central entity representing a user's wallet/account.
- Holds the current balance (derived from replayed events).
- Enforces all business invariants (e.g., no negative balance).
- Identified by a globally unique `accountId`.
- Carries a `version` (sequence number) for optimistic concurrency.

**Transaction**
- Represents a financial operation: deposit, withdrawal, or transfer leg.
- Not stored as a mutable record — its existence is implied by domain events.
- Identified by a unique `transactionId` (correlation ID across events).

**Domain Event**
- An immutable fact that something happened in the domain.
- Carries enough data to reconstruct state without requiring external lookups.
- Belongs to exactly one aggregate.

**Account Snapshot**
- A point-in-time capture of an Account's state (balance + version).
- Used to optimise replay; replaying begins from the latest snapshot rather than event zero.

**Projection / Read Model**
- A denormalised, query-optimised view built by consuming domain events.
- Examples: `AccountSummaryView`, `TransactionHistoryView`.
- Rebuilt at any time by replaying the event stream.

**Saga / Process Manager**
- Coordinates multi-step workflows that span multiple aggregates (e.g., fund transfer).
- Listens to events and issues compensating commands on failure.

### 7.2 Relationships

```
User ──< Account (1 user may have multiple accounts)
Account ──< DomainEvent (1 account has an ordered event stream)
Account ──< AccountSnapshot (periodic state captures)
DomainEvent ──> Projection (events feed read models via Kafka)
TransferSaga ──> Account (coordinates debit + credit across two accounts)
```

---

## 8. Event Design

All events follow a common envelope schema before the type-specific payload.

### 8.1 Common Event Envelope

| Field | Type | Description |
|---|---|---|
| `eventId` | UUID | Globally unique event identifier |
| `eventType` | String | Discriminator (e.g., `FundsDeposited`) |
| `aggregateId` | UUID | The account this event belongs to |
| `aggregateVersion` | Long | Monotonic sequence number within the aggregate |
| `occurredAt` | ISO-8601 UTC | When the event occurred |
| `correlationId` | UUID | Links related events (e.g., both legs of a transfer) |
| `causationId` | UUID | ID of the command that caused this event |
| `issuedBy` | String | Principal (user ID or service name) that triggered the command |

---

### 8.2 Core Domain Events

#### `AccountCreated`
Emitted when a new account is successfully opened.

| Field | Description |
|---|---|
| `accountId` | Newly created account identifier |
| `ownerName` | Display name of the account holder |
| `currency` | Account currency (e.g., `USD`) |
| `initialBalance` | Always `0.00` at creation |

---

#### `FundsDeposited`
Emitted when money is credited to an account.

| Field | Description |
|---|---|
| `accountId` | Target account |
| `amount` | Positive monetary amount deposited |
| `currency` | Currency of the amount |
| `transactionId` | Unique ID for this deposit transaction |
| `description` | Optional human-readable note |

---

#### `FundsWithdrawn`
Emitted when money is debited from an account.

| Field | Description |
|---|---|
| `accountId` | Source account |
| `amount` | Positive monetary amount withdrawn |
| `currency` | Currency of the amount |
| `transactionId` | Unique ID for this withdrawal transaction |
| `description` | Optional human-readable note |

---

#### `TransferInitiated`
Emitted by the saga when a cross-account transfer begins.

| Field | Description |
|---|---|
| `transferId` | Unique saga/transfer identifier |
| `sourceAccountId` | Account to be debited |
| `destinationAccountId` | Account to be credited |
| `amount` | Amount to transfer |
| `currency` | Transfer currency |

---

#### `FundsDebited` *(Transfer leg — source)*
Emitted against the source account once the debit is confirmed.

| Field | Description |
|---|---|
| `accountId` | Source account |
| `transferId` | Associated transfer |
| `amount` | Amount debited |

---

#### `FundsCredited` *(Transfer leg — destination)*
Emitted against the destination account once the credit is confirmed.

| Field | Description |
|---|---|
| `accountId` | Destination account |
| `transferId` | Associated transfer |
| `amount` | Amount credited |

---

#### `TransferCompleted`
Emitted by the saga when both legs have succeeded.

| Field | Description |
|---|---|
| `transferId` | Transfer identifier |
| `sourceAccountId` | Debited account |
| `destinationAccountId` | Credited account |
| `amount` | Total amount transferred |

---

#### `TransferFailed`
Emitted by the saga when the transfer cannot be completed (e.g., insufficient funds, account not found). Triggers compensating events if the debit was already applied.

| Field | Description |
|---|---|
| `transferId` | Transfer identifier |
| `reason` | Human-readable failure reason |
| `failedStep` | Which step in the saga failed |

---

#### `FundsRefunded` *(Compensating event)*
Emitted to reverse a debit when a transfer fails after the debit leg has been applied.

| Field | Description |
|---|---|
| `accountId` | Account receiving the refund |
| `transferId` | Original transfer reference |
| `amount` | Amount refunded |

---

#### `AccountClosed`
Emitted when an account is successfully closed.

| Field | Description |
|---|---|
| `accountId` | Closed account |
| `closedAt` | Timestamp of closure |
| `reason` | Optional closure reason |

---

## 9. CQRS Architecture Overview

### 9.1 Conceptual Split

```
┌──────────────────────────────────────────────────────────────────┐
│                          WRITE SIDE                              │
│                                                                  │
│  Client → Command API → Command Handler → Aggregate              │
│                              │                  │                │
│                         Validates          Enforces              │
│                         command            invariants            │
│                              │                  │                │
│                              └──── Event Store ─┘                │
│                                        │                         │
└────────────────────────────────────────┼─────────────────────────┘
                                         │ Publish Events
                                         ▼
                               ┌──────────────────┐
                               │   Apache Kafka   │
                               │  (Event Stream)  │
                               └──────────────────┘
                                         │ Consume Events
┌────────────────────────────────────────┼─────────────────────────┐
│                          READ SIDE     │                          │
│                                        ▼                         │
│              Projection Handlers → Read Model Store              │
│                                        │                         │
│              Client → Query API ───────┘                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 9.2 Write Side

- **Command API** — Exposes REST (or gRPC) endpoints that accept commands (e.g., `DepositFunds`, `WithdrawFunds`).
- **Command Handler** — Validates the command (authorization, basic input validation), loads the aggregate from the event store, delegates to the aggregate for business rule enforcement, and persists new events.
- **Aggregate (Account)** — Contains all business logic. Raises domain events on successful state transitions. Rejects invalid commands by throwing domain exceptions.
- **Event Store** — Append-only, ordered log of events per aggregate. Acts as the **single source of truth**.

### 9.3 Role of Apache Kafka

- After events are committed to the event store, they are **published to Kafka topics** (typically in the same transaction or via an outbox pattern to avoid dual-write inconsistency).
- Kafka acts as the **distribution bus** — completely decoupling event producers from consumers.
- Each consumer group processes events independently at its own pace.
- Kafka's durable log allows consumers to reprocess events (e.g., to rebuild projections) by resetting offsets.
- Topics are **partitioned by `accountId`** to guarantee per-account ordering while enabling parallelism.

### 9.4 Read Side

- **Projection Handlers** — Kafka consumers that listen to account event topics and update one or more read models.
- **Read Model Store** — A separate datastore (e.g., relational DB, document store) optimised for read queries. Not the source of truth — always rebuildable from events.
- **Query API** — Exposes endpoints that serve data exclusively from the read model store.

### 9.5 Command → Event → Projection Flow

```
Command Received
      │
      ▼
Command Handler validates and loads aggregate
      │
      ▼
Aggregate enforces business rules
      │
      ├─── Failure → Return error to caller (no event written)
      │
      └─── Success → Append event(s) to Event Store
                            │
                            ▼
                   Outbox / Transactional Publish → Kafka
                            │
                            ▼
                   Projection Handler consumes event
                            │
                            ▼
                   Read Model updated
                            │
                            ▼
                   Query API serves updated data
```

---

## 10. Data Flow (Step-by-Step)

### 10.1 Deposit Money

1. **Client** sends `DepositFunds` command: `{ accountId, amount, currency, idempotencyKey, issuedBy }`.
2. **Command API** authenticates the caller and validates the request schema.
3. **Command Handler** checks the `idempotencyKey` — if already processed, return the cached result immediately.
4. **Command Handler** loads Account aggregate by replaying events (or loading from the latest snapshot + subsequent events).
5. **Account Aggregate** validates: account must be active; amount must be positive.
6. On success, the aggregate raises a `FundsDeposited` event.
7. **Event Store** appends the event with the next sequence number. Rejects the write if the expected version does not match (optimistic concurrency conflict → retry).
8. **Outbox / Publisher** publishes the `FundsDeposited` event to the Kafka topic `account-events` (partitioned by `accountId`).
9. **Command API** returns `202 Accepted` with the `transactionId` to the client.
10. **Projection Handler** (Kafka consumer) receives the `FundsDeposited` event and updates the `AccountSummaryView` (increments balance) and `TransactionHistoryView`.
11. **Client** may poll or receive a webhook confirming the updated balance.

---

### 10.2 Transfer Funds Between Accounts

1. **Client** sends `TransferFunds` command: `{ sourceAccountId, destinationAccountId, amount, currency, idempotencyKey, issuedBy }`.
2. **Command API** validates the request and checks the idempotency key.
3. **Transfer Saga** is instantiated (or resumed if already started) with a unique `transferId`.
4. **Step 1 — Debit Source:**
   - Saga issues a `DebitAccount` command to the source account's command handler.
   - Source aggregate validates: active, sufficient funds.
   - `FundsDebited` event is appended and published to Kafka.
5. **Step 2 — Credit Destination:**
   - Saga listens for `FundsDebited` on Kafka.
   - Saga issues a `CreditAccount` command to the destination account's command handler.
   - Destination aggregate validates: active.
   - `FundsCredited` event is appended and published to Kafka.
6. **Step 3 — Complete:**
   - Saga listens for `FundsCredited`.
   - Saga emits `TransferCompleted` event.
   - Both read model projections are updated.
7. **Failure Path:**
   - If the debit succeeds but the credit fails (e.g., destination account closed), the saga emits a `RefundFunds` compensating command.
   - Source account receives `FundsRefunded` event, restoring the debited amount.
   - Saga emits `TransferFailed` event.

---

## 11. Edge Cases & Constraints

### 11.1 Double-Spend Prevention

- The Account aggregate enforces the **no-negative-balance invariant** on every withdrawal or debit command.
- The event store uses **optimistic concurrency control**: before appending an event, the handler asserts that the aggregate's current version matches the expected version. If another process has concurrently modified the aggregate, the write is rejected with a conflict error.
- The rejected command handler retries by reloading the aggregate (with updated balance) and re-evaluating. If the balance is still insufficient, it returns an error to the caller.

### 11.2 Idempotency

- Every command must include an `idempotencyKey` (client-generated UUID).
- The command handler stores processed idempotency keys in a durable store with the resulting event ID.
- Duplicate commands with the same key return the original response without reprocessing.
- Kafka consumers are idempotent by design: processing the same event twice produces the same read model state (upsert semantics).

### 11.3 Concurrent Updates

- Concurrent writes to the same account are serialised by the optimistic concurrency version check on the event store.
- Concurrent writes to **different** accounts proceed in full parallel — there is no cross-account locking.
- Transfer sagas handle cross-account coordination asynchronously via Kafka, avoiding distributed locks.

### 11.4 Partial Transfer Failure

- If the credit step fails after the debit step has been committed, the saga automatically issues a compensating `RefundFunds` command.
- The compensating command is retried until it succeeds, ensuring the saga always reaches a terminal state (completed or fully reversed).
- If the saga itself crashes mid-flight, it is resumed from its persisted state upon restart (saga state is durable).

### 11.5 Event Store Write Failure

- If the event store write fails after business logic succeeds but before Kafka publication, the **outbox pattern** ensures the event is not lost.
- Events are written to an outbox table in the same transaction as the event store. A dedicated relay process reads the outbox and publishes to Kafka, then marks rows as published.

### 11.6 Kafka Consumer Lag / Projection Lag

- Read models are eventually consistent. Clients querying immediately after a command may receive a slightly stale balance.
- The Query API may expose an `asOfVersion` parameter allowing clients to poll until the read model has reached a specific aggregate version.
- Time-critical operations (e.g., checking balance before a withdrawal) always go through the write side (command handler loads the aggregate directly from the event store), not from the read model.

### 11.7 Event Schema Evolution

- Events are versioned. New optional fields may be added without breaking existing consumers.
- Removing or renaming fields requires a new event version and a migration strategy for consumers.
- The event store retains events in their original serialized form; consumers apply a schema upgrade transform during deserialization.

---

## 12. Assumptions

| # | Assumption |
|---|---|
| A1 | All monetary amounts are in a single currency (v1). Multi-currency is deferred to a future release. |
| A2 | Amounts are represented as integers (minor units, e.g., cents) to avoid floating-point precision issues. |
| A3 | User identity management and authentication are handled by an external Identity Provider (IdP); this system receives a verified JWT. |
| A4 | The event store supports optimistic concurrency (version-conditional writes) as a built-in capability. |
| A5 | Apache Kafka is deployed with adequate replication and retention configuration before system launch. |
| A6 | Network partitions between the event store and Kafka are handled by the outbox relay pattern. |
| A7 | Account holders are pre-registered users; user onboarding (KYC, registration) is handled by a separate service. |
| A8 | Snapshots are an optimisation, not a requirement; the system can function correctly without them. |
| A9 | Event retention in Kafka may be shorter than in the event store; the event store is the authoritative archive. |
| A10 | Service-to-service communication uses a secure internal network; mTLS is assumed for inter-service calls. |

---

## 13. Future Enhancements

| Priority | Enhancement | Description |
|---|---|---|
| High | **Multi-Currency Support** | Accounts denominated in different currencies; FX rate integration for cross-currency transfers. |
| High | **Fraud Detection Engine** | Real-time streaming analytics on the Kafka event stream to flag suspicious patterns (velocity checks, anomaly detection). |
| High | **Scheduled / Recurring Transfers** | Cron-triggered transfer commands for standing orders and direct debits. |
| Medium | **Account Statements** | Periodic (monthly/quarterly) statement generation from the transaction history projection. |
| Medium | **Notification Service** | Event-driven SMS/email notifications on significant account events (large withdrawals, login from new device). |
| Medium | **Aggregate Snapshotting** | Automated snapshot creation at configurable event count thresholds to reduce replay time. |
| Medium | **GraphQL Query API** | Flexible, client-driven query layer over the read models. |
| Low | **Regulatory Reporting** | Automated report generation for tax authorities and central banks, sourced from the immutable event log. |
| Low | **Interest Accrual** | Scheduled computation of interest based on daily balance derived from event replay. |
| Low | **Soft Delete / Account Archiving** | Archiving inactive accounts while preserving their full event history for compliance. |

---

*End of Document — v1.0*
