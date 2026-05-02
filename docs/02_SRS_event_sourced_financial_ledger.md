# Software Requirements Specification (SRS)
## Event-Sourced Financial Ledger using CQRS

---

| Field | Detail |
|---|---|
| **Document Version** | 1.0 |
| **Status** | Draft |
| **Date** | 2026-05-02 |
| **Based On** | BRD v1.0 — Event-Sourced Financial Ledger |
| **Author** | Solutions Architecture Team |
| **Audience** | Backend Engineers, QA, DevOps/SRE, Compliance |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Overall Description](#2-overall-description)
3. [System Features](#3-system-features)
4. [External Interface Requirements](#4-external-interface-requirements)
5. [Non-Functional Requirements](#5-non-functional-requirements)
6. [Data Requirements](#6-data-requirements)
7. [System Behavior & Workflows](#7-system-behavior--workflows)
8. [Concurrency & Consistency Handling](#8-concurrency--consistency-handling)
9. [Error Handling & Edge Cases](#9-error-handling--edge-cases)
10. [Assumptions & Dependencies](#10-assumptions--dependencies)

---

## 1. Introduction

### 1.1 Purpose

This Software Requirements Specification (SRS) defines the complete, implementation-ready technical requirements for the **Event-Sourced Financial Ledger** system. It translates the business requirements in BRD v1.0 into precise, testable, engineering-level specifications consumable by backend engineers, QA engineers, and DevOps teams.

### 1.2 Scope

The system is a backend financial platform that:

- Accepts commands (deposits, withdrawals, transfers, account management) via a REST API.
- Persists all state changes as immutable domain events in an append-only PostgreSQL event store.
- Publishes events to Apache Kafka for downstream consumption.
- Maintains eventually-consistent read models (projections) in a separate PostgreSQL read database.
- Exposes a Query API for account balances, transaction history, and audit queries.
- Supports full event replay to reconstruct any past or present account state.

**Out of scope:** Payment gateway integrations, FX conversion, mobile/web front-ends, KYC workflows, fraud detection, external banking rails.

### 1.3 Definitions and Acronyms

| Term | Definition |
|---|---|
| **CQRS** | Command Query Responsibility Segregation — separates the write path (commands) from the read path (queries) into independent models. |
| **Event Sourcing** | An architectural pattern where state is derived entirely from an ordered, immutable log of domain events rather than mutable records. |
| **Aggregate** | A cluster of domain objects (here: `Account`) treated as a single unit for data changes; enforces all business invariants. |
| **Aggregate Root** | The single entry point to an aggregate; `Account` is the aggregate root. |
| **Domain Event** | An immutable fact representing something that has occurred in the domain (e.g., `FundsDeposited`). |
| **Command** | A request to change system state (e.g., `DepositFunds`). May be rejected. |
| **Event Store** | An append-only, ordered log of domain events; the single source of truth. |
| **Projection / Read Model** | A denormalised, query-optimised view derived from replaying domain events. |
| **Saga / Process Manager** | A stateful coordinator for multi-step, cross-aggregate workflows (e.g., fund transfers). |
| **Outbox Pattern** | A reliability pattern where events are written to an outbox table in the same DB transaction as the event store, then relayed to Kafka by a separate process. |
| **Optimistic Concurrency Control (OCC)** | A concurrency strategy that checks an aggregate version number before committing, rejecting writes if a conflict is detected. |
| **Idempotency Key** | A client-supplied UUID ensuring that duplicate command submissions are processed exactly once. |
| **Dead Letter Queue (DLQ)** | A Kafka topic where unprocessable messages are routed for manual investigation. |
| **Kafka Consumer Group** | A set of consumers sharing a topic subscription; each partition is consumed by exactly one member. |
| **Snapshot** | A point-in-time capture of an aggregate's state used to optimise event replay. |
| **p99 Latency** | The 99th percentile latency; 99% of requests complete within this time. |
| **Minor Units** | Integer representation of currency (e.g., cents for USD) to avoid floating-point precision errors. |
| **mTLS** | Mutual TLS — bidirectional certificate-based authentication for inter-service communication. |
| **IdP** | Identity Provider — external system responsible for user authentication and JWT issuance. |
| **JWT** | JSON Web Token — a compact, signed token carrying user identity claims. |
| **DLT** | Distributed Ledger Technology (referenced for context only; this system does not use blockchain). |
| **UTC** | Coordinated Universal Time — all timestamps in the system use UTC. |

---

## 2. Overall Description

### 2.1 System Overview

The system implements a **CQRS + Event Sourcing** architecture with the following high-level components:

```
┌──────────────────────────────────────────────────────────┐
│  WRITE SIDE                                              │
│  REST Command API → Command Handler → Account Aggregate  │
│                          │                               │
│                    PostgreSQL Event Store                 │
│                    + Outbox Table                        │
└──────────────────────────────┬───────────────────────────┘
                               │ Outbox Relay
                               ▼
                      Apache Kafka Cluster
                      (account-events topic)
                               │ Consumer Groups
┌──────────────────────────────▼───────────────────────────┐
│  READ SIDE                                               │
│  Projection Handler → PostgreSQL Read Model DB           │
│  REST Query API ← Read Model DB                          │
│  Audit Handler  → Audit Log DB                           │
└──────────────────────────────────────────────────────────┘
```

### 2.2 User Classes and Characteristics

| User Class | Interaction Mode | Permissions |
|---|---|---|
| **Account Holder** | REST API (via client app) | Read/write own accounts only |
| **Operator / Admin** | REST API + internal tooling | Cross-account read; trigger replays |
| **Compliance Officer** | Audit Query API | Read-only; full audit log access |
| **Projection Service** | Internal Kafka consumer | Read events; write read models |
| **Outbox Relay Service** | Internal DB poller | Read outbox; publish to Kafka |
| **Saga Coordinator** | Internal Kafka consumer/producer | Orchestrate transfer workflows |

### 2.3 Operating Environment

| Component | Technology |
|---|---|
| **Application Runtime** | Java 21 (LTS), Spring Boot 3.x |
| **Build Tool** | Maven or Gradle |
| **Command/Query API** | Spring Web MVC (REST, JSON) |
| **Event Store** | PostgreSQL 15+ (append-only `domain_events` table) |
| **Read Model Store** | PostgreSQL 15+ (separate schema or database) |
| **Event Streaming** | Apache Kafka 3.x |
| **Kafka Client** | Spring Kafka |
| **ORM / DB Access** | Spring Data JPA + Hibernate or JOOQ |
| **Saga State Store** | PostgreSQL (saga state table) |
| **Idempotency Store** | PostgreSQL (idempotency keys table) |
| **Observability** | Micrometer + Prometheus + Grafana; distributed tracing via OpenTelemetry + Jaeger |
| **Security** | Spring Security + JWT (OAuth 2.0 Resource Server) |
| **Containerisation** | Docker; orchestrated via Kubernetes |

### 2.4 Design Constraints

- **DC-01:** The event store is strictly append-only. No `UPDATE` or `DELETE` operations shall be issued against the `domain_events` table.
- **DC-02:** All monetary amounts are stored as `BIGINT` in minor currency units (e.g., cents). Floating-point types (`FLOAT`, `DOUBLE`) are prohibited for monetary values.
- **DC-03:** All timestamps are stored and transmitted in UTC ISO-8601 format (`yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`).
- **DC-04:** Read model stores are non-authoritative. They may be dropped and fully rebuilt from the event store at any time.
- **DC-05:** No distributed transactions (XA/2PC) shall be used. Consistency across service boundaries is achieved via the Saga pattern and the Outbox pattern.
- **DC-06:** All inter-service communication (Kafka, internal APIs) uses mTLS.
- **DC-07:** Event payloads must not contain PII (Personally Identifiable Information). PII references are resolved via the external identity store.
- **DC-08:** The system targets Java 21 and Spring Boot 3.x; no deprecated APIs from prior major versions shall be used.

---

## 3. System Features

### 3.1 Account Management

#### 3.1.1 Description
The account management feature handles the lifecycle of financial accounts: creation and closure. An `Account` is the primary aggregate root. All business invariants regarding an account are enforced here.

#### 3.1.2 Functional Requirements

| ID | Requirement |
|---|---|
| **SRS-ACC-01** | The system SHALL create a new account upon receiving a valid `CreateAccount` command. The account SHALL be assigned a system-generated UUID as its `accountId`. |
| **SRS-ACC-02** | A `CreateAccount` command SHALL be rejected with HTTP `409 Conflict` if an account with the supplied `accountId` already exists in the event store. |
| **SRS-ACC-03** | A newly created account SHALL have an initial balance of exactly `0` (zero minor units). |
| **SRS-ACC-04** | The system SHALL reject a `CloseAccount` command with HTTP `422 Unprocessable Entity` if the account's current balance is not `0`. |
| **SRS-ACC-05** | The system SHALL reject any command targeting an account whose status is `CLOSED` or `SUSPENDED` with HTTP `422 Unprocessable Entity` and error code `ACCOUNT_INACTIVE`. |
| **SRS-ACC-06** | Account creation SHALL produce an `AccountCreated` event persisted to the event store and published to Kafka. |
| **SRS-ACC-07** | Account closure SHALL produce an `AccountClosed` event persisted to the event store and published to Kafka. |
| **SRS-ACC-08** | The `ownerName` field in `CreateAccount` SHALL be non-null and between 1 and 255 characters. |
| **SRS-ACC-09** | The `currency` field SHALL be a 3-letter ISO 4217 currency code. For v1, only `USD` is supported. |

#### 3.1.3 Input / Output

**CreateAccount Command (Request Body):**
```json
{
  "accountId": "optional-client-hint-uuid",
  "ownerName": "Jane Doe",
  "currency": "USD",
  "idempotencyKey": "550e8400-e29b-41d4-a716-446655440000",
  "issuedBy": "user-service-principal-id"
}
```

**Success Response — HTTP 202 Accepted:**
```json
{
  "accountId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "CREATED",
  "eventId": "evt-uuid-here"
}
```

#### 3.1.4 Validation Rules

| Field | Rule |
|---|---|
| `ownerName` | Non-null, non-empty, max 255 chars |
| `currency` | Must be `USD` (v1 constraint) |
| `idempotencyKey` | Must be a valid UUID v4 |
| `issuedBy` | Non-null; must match the authenticated JWT subject |

#### 3.1.5 Error Handling

| Scenario | HTTP Status | Error Code |
|---|---|---|
| Duplicate `accountId` | 409 | `ACCOUNT_ALREADY_EXISTS` |
| Close with non-zero balance | 422 | `BALANCE_NOT_ZERO` |
| Invalid currency | 400 | `INVALID_CURRENCY` |
| Missing required field | 400 | `VALIDATION_ERROR` |
| Optimistic concurrency conflict | 409 | `CONCURRENCY_CONFLICT` |

---

### 3.2 Transaction Processing

#### 3.2.1 Description
Handles all monetary operations: deposit, withdrawal, and fund transfer. All operations are modelled as commands processed by the Account aggregate. Business invariants (e.g., non-negative balance) are enforced exclusively within the aggregate.

#### 3.2.2 Functional Requirements

| ID | Requirement |
|---|---|
| **SRS-TXN-01** | The system SHALL process a `DepositFunds` command by appending a `FundsDeposited` event when: (a) the account is active, and (b) the amount is a positive integer in minor units. |
| **SRS-TXN-02** | The system SHALL reject a `DepositFunds` command with HTTP `400` and code `INVALID_AMOUNT` if the amount is ≤ 0. |
| **SRS-TXN-03** | The system SHALL process a `WithdrawFunds` command by appending a `FundsWithdrawn` event when: (a) the account is active, and (b) the resulting balance would be ≥ 0. |
| **SRS-TXN-04** | The system SHALL reject a `WithdrawFunds` command with HTTP `422` and code `INSUFFICIENT_FUNDS` if the withdrawal would result in a negative balance. |
| **SRS-TXN-05** | A `TransferFunds` command SHALL atomically debit the source account and credit the destination account via the Transfer Saga. |
| **SRS-TXN-06** | The system SHALL reject a `TransferFunds` command if the source account has insufficient funds, returning HTTP `422` and code `INSUFFICIENT_FUNDS`. |
| **SRS-TXN-07** | The system SHALL reject any transaction command against a `CLOSED` or `SUSPENDED` account with HTTP `422` and code `ACCOUNT_INACTIVE`. |
| **SRS-TXN-08** | Each transaction command SHALL be assigned a globally unique `transactionId` (UUID v4), either client-supplied or system-generated, stored as the `correlationId` in the event envelope. |
| **SRS-TXN-09** | The maximum single-transaction amount SHALL be `9,999,999,999` minor units (i.e., $99,999,999.99). Amounts exceeding this limit SHALL be rejected with `AMOUNT_EXCEEDS_LIMIT`. |
| **SRS-TXN-10** | Self-transfers (source equals destination) SHALL be rejected with HTTP `400` and code `SELF_TRANSFER_NOT_ALLOWED`. |

#### 3.2.3 Input / Output

**DepositFunds Command:**
```json
{
  "accountId": "a1b2c3d4-...",
  "amount": 500000,
  "currency": "USD",
  "description": "Salary credit",
  "idempotencyKey": "uuid-v4",
  "issuedBy": "user-principal-id"
}
```

**WithdrawFunds Command:**
```json
{
  "accountId": "a1b2c3d4-...",
  "amount": 10000,
  "currency": "USD",
  "description": "ATM withdrawal",
  "idempotencyKey": "uuid-v4",
  "issuedBy": "user-principal-id"
}
```

**TransferFunds Command:**
```json
{
  "sourceAccountId": "uuid-src",
  "destinationAccountId": "uuid-dst",
  "amount": 250000,
  "currency": "USD",
  "description": "Rent payment",
  "idempotencyKey": "uuid-v4",
  "issuedBy": "user-principal-id"
}
```

**Success Response — HTTP 202 Accepted:**
```json
{
  "transactionId": "txn-uuid",
  "status": "ACCEPTED",
  "eventId": "evt-uuid"
}
```

#### 3.2.4 Validation Rules

| Field | Rule |
|---|---|
| `amount` | Positive integer (`> 0`), max `9_999_999_999` |
| `currency` | Must be `USD` |
| `idempotencyKey` | Valid UUID v4, stored for deduplication |
| `sourceAccountId` ≠ `destinationAccountId` | Enforced for transfers |

#### 3.2.5 Error Handling

| Scenario | HTTP Status | Error Code |
|---|---|---|
| Amount ≤ 0 | 400 | `INVALID_AMOUNT` |
| Amount > max | 400 | `AMOUNT_EXCEEDS_LIMIT` |
| Insufficient funds | 422 | `INSUFFICIENT_FUNDS` |
| Account inactive | 422 | `ACCOUNT_INACTIVE` |
| Self-transfer | 400 | `SELF_TRANSFER_NOT_ALLOWED` |
| Destination not found | 404 | `ACCOUNT_NOT_FOUND` |
| Currency mismatch | 400 | `CURRENCY_MISMATCH` |

---

### 3.3 Event Store

#### 3.3.1 Description
The event store is the authoritative, append-only log of all domain events. It is implemented as a PostgreSQL table with strict write constraints. It serves as the single source of truth from which all read models are derived.

#### 3.3.2 Functional Requirements

| ID | Requirement |
|---|---|
| **SRS-EVT-01** | Every state-changing command that succeeds SHALL result in one or more domain events appended to the `domain_events` table. |
| **SRS-EVT-02** | Each event SHALL be assigned a monotonically increasing `aggregate_version` scoped to its `aggregate_id`. Version `1` is assigned to the first event of any aggregate. |
| **SRS-EVT-03** | The event store SHALL enforce a unique constraint on `(aggregate_id, aggregate_version)` at the database level to prevent version conflicts. |
| **SRS-EVT-04** | No `UPDATE` or `DELETE` statements SHALL be executable against the `domain_events` table. This SHALL be enforced via PostgreSQL row-level security or a dedicated write-only DB role. |
| **SRS-EVT-05** | Each event record SHALL contain: `event_id`, `event_type`, `aggregate_id`, `aggregate_version`, `occurred_at`, `correlation_id`, `causation_id`, `issued_by`, `payload` (JSONB), `schema_version`, `checksum`. |
| **SRS-EVT-06** | The `checksum` field SHALL contain the SHA-256 hash of `(aggregate_id + aggregate_version + event_type + payload)` for tamper detection. |
| **SRS-EVT-07** | The event store SHALL expose a read interface: `loadEvents(aggregateId, fromVersion, toVersion)` returning events ordered by `aggregate_version` ascending. |
| **SRS-EVT-08** | An outbox table (`event_outbox`) SHALL be written in the same database transaction as `domain_events`. A relay process publishes outbox records to Kafka and marks them as `PUBLISHED`. |

#### 3.3.3 Error Handling

| Scenario | Behaviour |
|---|---|
| Version conflict (duplicate `aggregate_version`) | DB unique constraint violation → command handler catches, reloads aggregate, retries up to 3 times |
| Event store unavailable | Command fails with HTTP `503`; no event emitted |
| Checksum mismatch on read | Log alert; mark event as `CORRUPTED`; do not apply to aggregate |

---

### 3.4 Event Replay Engine

#### 3.4.1 Description
The replay engine reconstructs aggregate state or read models by re-processing events from the event store in order. It supports full replay, partial replay (up to a sequence number or timestamp), and snapshot-assisted replay.

#### 3.4.2 Functional Requirements

| ID | Requirement |
|---|---|
| **SRS-RPL-01** | The system SHALL reconstruct the current state of an `Account` aggregate by applying all its events in `aggregate_version` order. |
| **SRS-RPL-02** | The system SHALL support replaying events up to a specified `aggregate_version` (inclusive) to reconstruct historical state. |
| **SRS-RPL-03** | The system SHALL support replaying events up to a specified `occurred_at` timestamp (inclusive, UTC). |
| **SRS-RPL-04** | If a snapshot exists for an aggregate, the replay engine SHALL load the most recent snapshot with `snapshot_version ≤ target_version`, then apply only subsequent events. |
| **SRS-RPL-05** | The replay engine SHALL expose an administrative endpoint `POST /admin/projections/{projectionName}/rebuild` that triggers a full read model rebuild from event store. |
| **SRS-RPL-06** | Projection rebuilds SHALL be executed without impacting live query availability. The rebuild SHALL write to a shadow table, then atomically swap with the live table on completion. |
| **SRS-RPL-07** | A snapshot SHALL be created automatically when an aggregate's event count since the last snapshot exceeds a configurable threshold (default: 50 events). |
| **SRS-RPL-08** | Snapshot creation SHALL NOT block command processing. Snapshots are created asynchronously post-event-commit. |

---

### 3.5 Read Model (Query Side)

#### 3.5.1 Description
The read side maintains denormalised, query-optimised views updated by Kafka consumer projection handlers. It serves all GET queries exclusively. It is not authoritative and can be fully rebuilt from the event store.

#### 3.5.2 Functional Requirements

| ID | Requirement |
|---|---|
| **SRS-QRY-01** | The Query API SHALL expose `GET /accounts/{accountId}/balance` returning the current balance and account status from the read model. |
| **SRS-QRY-02** | The Query API SHALL expose `GET /accounts/{accountId}/transactions` returning a paginated list of transactions, default page size 20, max 100. |
| **SRS-QRY-03** | The Query API SHALL expose `GET /transactions/{transactionId}` returning a single transaction record. |
| **SRS-QRY-04** | The Query API SHALL expose `GET /accounts/{accountId}/transactions?from={ISO-date}&to={ISO-date}` filtering transactions by date range. |
| **SRS-QRY-05** | The Query API SHALL expose `GET /accounts/{accountId}/history?atVersion={n}` returning the reconstructed account state at a specific aggregate version via event replay. |
| **SRS-QRY-06** | All Query API endpoints SHALL respond with p99 latency < 50ms under normal load (measured at the application layer, excluding network). |
| **SRS-QRY-07** | Query API endpoints SHALL never interact with the write-side event store for normal read operations. |
| **SRS-QRY-08** | The Query API SHALL support an `asOfVersion` query parameter on balance endpoints, blocking until the read model has caught up to the specified aggregate version (max wait: 5 seconds, then HTTP `202` with retry hint). |

---

## 4. External Interface Requirements

### 4.1 REST API — Command Side

**Base Path:** `/api/v1`
**Auth:** Bearer JWT (OAuth 2.0) on all endpoints.
**Content-Type:** `application/json`

| Method | Path | Command | Description |
|---|---|---|---|
| `POST` | `/accounts` | `CreateAccount` | Create a new account |
| `DELETE` | `/accounts/{accountId}` | `CloseAccount` | Close account (balance must be 0) |
| `POST` | `/accounts/{accountId}/deposit` | `DepositFunds` | Credit funds to account |
| `POST` | `/accounts/{accountId}/withdraw` | `WithdrawFunds` | Debit funds from account |
| `POST` | `/transfers` | `TransferFunds` | Initiate cross-account transfer |

**Standard Error Response Body:**
```json
{
  "errorCode": "INSUFFICIENT_FUNDS",
  "message": "Account balance is insufficient for the requested withdrawal.",
  "traceId": "trace-uuid",
  "timestamp": "2026-05-02T08:00:00.000Z"
}
```

### 4.2 REST API — Query Side

**Base Path:** `/api/v1`

| Method | Path | Description |
|---|---|---|
| `GET` | `/accounts/{accountId}/balance` | Current balance and status |
| `GET` | `/accounts/{accountId}/transactions` | Paginated transaction history |
| `GET` | `/accounts/{accountId}/transactions?from=&to=` | Date-range filtered transactions |
| `GET` | `/transactions/{transactionId}` | Single transaction detail |
| `GET` | `/accounts/{accountId}/history?atVersion={n}` | State at specific version |
| `GET` | `/admin/audit?accountId=&eventType=&from=&to=&issuedBy=` | Audit log query (operator only) |
| `POST` | `/admin/projections/{name}/rebuild` | Trigger projection rebuild (operator only) |

**Balance Response:**
```json
{
  "accountId": "uuid",
  "balance": 500000,
  "currency": "USD",
  "status": "ACTIVE",
  "version": 12,
  "lastUpdatedAt": "2026-05-02T07:45:00.000Z"
}
```

**Transaction History Response:**
```json
{
  "accountId": "uuid",
  "page": 1,
  "pageSize": 20,
  "totalElements": 145,
  "transactions": [
    {
      "transactionId": "uuid",
      "type": "DEPOSIT",
      "amount": 500000,
      "currency": "USD",
      "description": "Salary credit",
      "occurredAt": "2026-05-02T07:30:00.000Z",
      "aggregateVersion": 5
    }
  ]
}
```

### 4.3 Kafka Interface

#### 4.3.1 Topics

| Topic | Partitioned By | Retention | Description |
|---|---|---|---|
| `account-events` | `accountId` | 90 days (configurable) | All account domain events |
| `transfer-saga-events` | `transferId` | 30 days | Transfer saga coordination events |
| `account-events-dlq` | N/A | 365 days | Unprocessable events |

#### 4.3.2 Producer Configuration

| Setting | Value |
|---|---|
| `acks` | `all` (all in-sync replicas must acknowledge) |
| `enable.idempotence` | `true` |
| `max.in.flight.requests.per.connection` | `5` |
| `retries` | `Integer.MAX_VALUE` |
| `compression.type` | `lz4` |

#### 4.3.3 Consumer Configuration

| Setting | Value |
|---|---|
| `auto.offset.reset` | `earliest` |
| `enable.auto.commit` | `false` (manual offset commit after processing) |
| `isolation.level` | `read_committed` |
| `max.poll.records` | `500` |

#### 4.3.4 Kafka Message Envelope

```json
{
  "eventId": "uuid",
  "eventType": "FundsDeposited",
  "aggregateId": "account-uuid",
  "aggregateVersion": 5,
  "occurredAt": "2026-05-02T07:30:00.000Z",
  "correlationId": "txn-uuid",
  "causationId": "cmd-uuid",
  "issuedBy": "user-principal-id",
  "schemaVersion": 1,
  "payload": {
    "accountId": "account-uuid",
    "amount": 500000,
    "currency": "USD",
    "transactionId": "txn-uuid",
    "description": "Salary credit"
  }
}
```

### 4.4 Database Interactions

#### 4.4.1 Event Store (Write DB)

- **Database:** PostgreSQL 15+, dedicated instance/schema.
- **Access Pattern:** Append-only writes; reads for aggregate hydration only.
- **Connection Pooling:** HikariCP, max pool size 20 per service instance.
- **Isolation Level:** `SERIALIZABLE` for event append transactions; `READ COMMITTED` for reads.

#### 4.4.2 Read Model DB

- **Database:** PostgreSQL 15+, separate instance/schema from event store.
- **Access Pattern:** Write by projection handlers; read by Query API.
- **Connection Pooling:** HikariCP, max pool size 30 per service instance (read-heavy).
- **Isolation Level:** `READ COMMITTED`.
- **Indexing:** All foreign key columns and frequently-filtered columns (`account_id`, `occurred_at`, `transaction_id`) SHALL have B-tree indexes.

---

## 5. Non-Functional Requirements

### 5.1 Consistency

| ID | Requirement |
|---|---|
| **SRS-NFR-CON-01** | The write side SHALL enforce strong consistency within a single aggregate using optimistic concurrency control (OCC). The event store unique constraint on `(aggregate_id, aggregate_version)` is the enforcement mechanism. |
| **SRS-NFR-CON-02** | Read models are eventually consistent with the write side. The maximum acceptable propagation lag is **500ms** under steady-state load (measured as elapsed time from event store commit to read model update). |
| **SRS-NFR-CON-03** | Cross-account transfers SHALL use the Transfer Saga pattern. No distributed transactions (XA/2PC) are permitted. |
| **SRS-NFR-CON-04** | Time-critical balance checks (e.g., pre-withdrawal validation) SHALL always load the aggregate directly from the event store, never from the read model. |

### 5.2 Performance

| Metric | Target | Measurement Point |
|---|---|---|
| Deposit / Withdraw p99 latency | < 200ms | Command API response time |
| Fund Transfer p99 latency (end-to-end) | < 500ms | Transfer initiation to saga completion |
| Balance query p99 latency | < 50ms | Query API response time |
| Transaction history query p99 latency | < 100ms | Query API response time |
| Event store write throughput | ≥ 5,000 events/sec per partition | PostgreSQL insert rate |
| Kafka consumer lag (steady state) | < 1 second | Kafka consumer group lag metric |
| Projection rebuild throughput | ≥ 10,000 events/sec | Replay engine processing rate |

### 5.3 Scalability

| ID | Requirement |
|---|---|
| **SRS-NFR-SCL-01** | The Command API SHALL be stateless and horizontally scalable. Multiple instances SHALL run concurrently behind a load balancer without coordination. |
| **SRS-NFR-SCL-02** | Kafka topics SHALL be configured with a minimum of **12 partitions** for `account-events` to support parallel consumer throughput. Partition count is configurable without redeployment. |
| **SRS-NFR-SCL-03** | The read model database SHALL support read replicas. The Query API SHALL route read traffic to replicas; writes from projection handlers go to the primary. |
| **SRS-NFR-SCL-04** | The system SHALL operate correctly with **≥ 1,000,000 active accounts** and **≥ 50,000,000 events** without schema changes or architectural modifications. |
| **SRS-NFR-SCL-05** | The `domain_events` table SHALL be partitioned by `aggregate_id` hash (range or hash partitioning) to support large data volumes. |

### 5.4 Reliability & Fault Tolerance

| ID | Requirement |
|---|---|
| **SRS-NFR-REL-01** | The PostgreSQL event store SHALL be deployed with synchronous streaming replication to a minimum of **2 standby nodes** (total replication factor ≥ 3). |
| **SRS-NFR-REL-02** | Apache Kafka SHALL be configured with `replication.factor=3` and `min.insync.replicas=2` on all topics. |
| **SRS-NFR-REL-03** | A single node failure (application, DB replica, or Kafka broker) SHALL NOT result in data loss or service unavailability. |
| **SRS-NFR-REL-04** | The write API SHALL achieve **99.9% monthly uptime** (≤ 43.8 minutes downtime/month), excluding planned maintenance windows. |
| **SRS-NFR-REL-05** | Failed commands SHALL be retried with exponential backoff: initial delay 100ms, multiplier 2x, max delay 10s, max attempts 5. |
| **SRS-NFR-REL-06** | Commands exceeding retry limits SHALL be routed to the Dead Letter Queue (DLQ) Kafka topic `account-events-dlq` with full diagnostic context. |
| **SRS-NFR-REL-07** | The outbox relay process SHALL achieve exactly-once Kafka delivery by tracking published event IDs with a `published_at` timestamp in the outbox table. |
| **SRS-NFR-REL-08** | Saga state SHALL be persisted to PostgreSQL. On saga process restart, all in-flight sagas SHALL be resumed from their last persisted step. |

### 5.5 Security

| ID | Requirement |
|---|---|
| **SRS-NFR-SEC-01** | All Command and Query API endpoints SHALL require a valid JWT issued by the configured Identity Provider (IdP). Requests without a valid JWT SHALL return HTTP `401`. |
| **SRS-NFR-SEC-02** | JWT validation SHALL verify: signature, expiry (`exp`), issuer (`iss`), and audience (`aud`). Asymmetric signing (RS256 or ES256) is required. |
| **SRS-NFR-SEC-03** | An authenticated user SHALL only access or modify accounts where the JWT `sub` claim matches the account's `ownerId`, unless the JWT contains an `OPERATOR` or `ADMIN` role claim. |
| **SRS-NFR-SEC-04** | All data in transit SHALL use TLS 1.2 minimum; TLS 1.3 preferred. Plaintext HTTP connections SHALL be rejected. |
| **SRS-NFR-SEC-05** | All data at rest (PostgreSQL, Kafka) SHALL be encrypted using AES-256 or equivalent. |
| **SRS-NFR-SEC-06** | PII (owner names, contact details) SHALL NOT appear in event payloads, Kafka messages, or log entries. Only opaque `ownerId` references SHALL be stored. |
| **SRS-NFR-SEC-07** | Inter-service calls SHALL use mTLS with certificates rotated at minimum every 90 days. |
| **SRS-NFR-SEC-08** | Database credentials SHALL be managed via a secrets manager (e.g., HashiCorp Vault, AWS Secrets Manager) and SHALL NOT appear in environment variables or config files. |

### 5.6 Observability

| ID | Requirement |
|---|---|
| **SRS-NFR-OBS-01** | Every inbound command request SHALL be assigned a unique `traceId` (OpenTelemetry trace context) propagated across all service boundaries via HTTP headers and Kafka message headers. |
| **SRS-NFR-OBS-02** | All services SHALL emit structured JSON logs including: `timestamp`, `level`, `traceId`, `spanId`, `service`, `message`, and relevant domain context. |
| **SRS-NFR-OBS-03** | The following metrics SHALL be exposed via Micrometer/Prometheus: command processing latency (histogram), event store write latency (histogram), Kafka producer/consumer lag (gauge), error rates per error code (counter), active saga count (gauge). |
| **SRS-NFR-OBS-04** | Distributed traces SHALL be exported to a configured OpenTelemetry collector (e.g., Jaeger, Tempo). Trace sampling rate SHALL be configurable (default: 10% in production, 100% in staging). |
| **SRS-NFR-OBS-05** | Alerting rules SHALL be defined for: Kafka consumer lag > 5s, command error rate > 1% over 5 min, p99 latency > threshold, DLQ message count > 0. |

### 5.7 Auditability

| ID | Requirement |
|---|---|
| **SRS-NFR-AUD-01** | All domain events SHALL be retained in the event store for a minimum of **7 years** from the event's `occurred_at` timestamp. |
| **SRS-NFR-AUD-02** | The system SHALL provide an administrative endpoint to verify event store integrity by recomputing and comparing SHA-256 checksums for all events of a given aggregate. |
| **SRS-NFR-AUD-03** | Event store records SHALL be protected from modification by database role restrictions: the application write role is granted only `INSERT` and `SELECT` on `domain_events`. |


---

## 6. Data Requirements

### 6.1 Event Store Schema

#### Table: `domain_events`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `event_id` | `UUID` | PK, NOT NULL | Globally unique event identifier |
| `event_type` | `VARCHAR(100)` | NOT NULL | Event discriminator (e.g., `FundsDeposited`) |
| `aggregate_id` | `UUID` | NOT NULL, INDEXED | Account UUID this event belongs to |
| `aggregate_version` | `BIGINT` | NOT NULL | Monotonic sequence within aggregate |
| `occurred_at` | `TIMESTAMPTZ` | NOT NULL | UTC timestamp of occurrence |
| `correlation_id` | `UUID` | NOT NULL | Transaction / saga correlation ID |
| `causation_id` | `UUID` | NOT NULL | ID of the command that caused the event |
| `issued_by` | `VARCHAR(255)` | NOT NULL | Principal ID from JWT `sub` claim |
| `schema_version` | `SMALLINT` | NOT NULL, DEFAULT 1 | Event schema version for evolution |
| `payload` | `JSONB` | NOT NULL | Type-specific event data |
| `checksum` | `CHAR(64)` | NOT NULL | SHA-256 hash for tamper detection |

**Constraints:**
- `UNIQUE (aggregate_id, aggregate_version)` — enforces OCC
- `CHECK (aggregate_version > 0)`
- No foreign key to any mutable table (event store is self-contained)

**Partitioning:** Hash-partitioned on `aggregate_id` with 16 partitions (configurable).

---

#### Table: `event_outbox`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `outbox_id` | `UUID` | PK | Outbox record ID |
| `event_id` | `UUID` | NOT NULL, FK → `domain_events.event_id` | Referenced event |
| `aggregate_id` | `UUID` | NOT NULL | For Kafka partition key |
| `topic` | `VARCHAR(200)` | NOT NULL | Target Kafka topic |
| `payload` | `JSONB` | NOT NULL | Full Kafka message payload |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | When outbox record was created |
| `published_at` | `TIMESTAMPTZ` | NULLABLE | Null until successfully published |
| `status` | `VARCHAR(20)` | NOT NULL, DEFAULT `PENDING` | `PENDING`, `PUBLISHED`, `FAILED` |

---

#### Table: `account_snapshots`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `snapshot_id` | `UUID` | PK | Snapshot identifier |
| `aggregate_id` | `UUID` | NOT NULL, INDEXED | Account UUID |
| `snapshot_version` | `BIGINT` | NOT NULL | Aggregate version at snapshot time |
| `state` | `JSONB` | NOT NULL | Serialised aggregate state |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | When snapshot was taken |

**Constraint:** `UNIQUE (aggregate_id, snapshot_version)`

---

#### Table: `idempotency_keys`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `idempotency_key` | `UUID` | PK | Client-supplied idempotency key |
| `command_type` | `VARCHAR(100)` | NOT NULL | e.g., `DepositFunds` |
| `result_event_id` | `UUID` | NULLABLE | Event ID of the processed result |
| `response_payload` | `JSONB` | NULLABLE | Cached HTTP response body |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | When key was first seen |
| `expires_at` | `TIMESTAMPTZ` | NOT NULL | TTL for key retention (default: 24h) |

---

#### Table: `saga_state`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `saga_id` | `UUID` | PK | Transfer saga identifier |
| `saga_type` | `VARCHAR(100)` | NOT NULL | e.g., `TransferSaga` |
| `current_step` | `VARCHAR(100)` | NOT NULL | e.g., `DEBIT_INITIATED` |
| `status` | `VARCHAR(20)` | NOT NULL | `IN_PROGRESS`, `COMPLETED`, `FAILED`, `COMPENSATING` |
| `payload` | `JSONB` | NOT NULL | Full saga context |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | When saga started |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL | Last state update |
| `version` | `INT` | NOT NULL, DEFAULT 1 | Optimistic lock for saga state |

---

### 6.2 Read Model Schema

#### Table: `account_summary_view`

| Column | Type | Description |
|---|---|---|
| `account_id` | `UUID` PK | Account identifier |
| `owner_id` | `VARCHAR(255)` | Owner principal ID |
| `balance` | `BIGINT` | Current balance in minor units |
| `currency` | `CHAR(3)` | ISO 4217 currency code |
| `status` | `VARCHAR(20)` | `ACTIVE`, `CLOSED`, `SUSPENDED` |
| `aggregate_version` | `BIGINT` | Latest processed event version |
| `created_at` | `TIMESTAMPTZ` | Account creation time |
| `updated_at` | `TIMESTAMPTZ` | Last projection update time |

---

#### Table: `transaction_history_view`

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` PK | Read model row ID |
| `transaction_id` | `UUID` INDEXED | Correlation ID |
| `account_id` | `UUID` INDEXED | Associated account |
| `transaction_type` | `VARCHAR(20)` | `DEPOSIT`, `WITHDRAWAL`, `TRANSFER_DEBIT`, `TRANSFER_CREDIT`, `REFUND` |
| `amount` | `BIGINT` | Amount in minor units |
| `currency` | `CHAR(3)` | Currency code |
| `description` | `TEXT` | Optional human-readable note |
| `counterparty_account_id` | `UUID` NULLABLE | For transfers: the other account |
| `transfer_id` | `UUID` NULLABLE | Associated transfer saga ID |
| `occurred_at` | `TIMESTAMPTZ` INDEXED | Event timestamp |
| `aggregate_version` | `BIGINT` | Event version |

---

### 6.3 Event Schema Definitions

#### 6.3.1 Common Event Envelope Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `eventId` | UUID | Yes | Globally unique event ID |
| `eventType` | String | Yes | Event class discriminator |
| `aggregateId` | UUID | Yes | Account UUID |
| `aggregateVersion` | Long | Yes | Monotonic per-aggregate version |
| `occurredAt` | ISO-8601 UTC | Yes | Event occurrence timestamp |
| `correlationId` | UUID | Yes | Transaction/saga ID |
| `causationId` | UUID | Yes | ID of originating command |
| `issuedBy` | String | Yes | Principal ID |
| `schemaVersion` | Integer | Yes | Payload schema version (default: 1) |

#### 6.3.2 Type-Specific Payload Schemas

**`AccountCreated` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Newly created account ID |
| `ownerId` | String | Principal ID of owner (no PII) |
| `currency` | String | ISO 4217 code |
| `initialBalance` | Long | Always `0` |

---

**`FundsDeposited` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Target account |
| `amount` | Long | Positive minor-unit amount |
| `currency` | String | ISO 4217 code |
| `transactionId` | UUID | Unique transaction ID |
| `description` | String (nullable) | Optional note, max 500 chars |

---

**`FundsWithdrawn` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Source account |
| `amount` | Long | Positive minor-unit amount |
| `currency` | String | ISO 4217 code |
| `transactionId` | UUID | Unique transaction ID |
| `description` | String (nullable) | Optional note |

---

**`FundsDebited` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Source account |
| `transferId` | UUID | Saga transfer ID |
| `amount` | Long | Amount debited |
| `currency` | String | ISO 4217 code |

---

**`FundsCredited` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Destination account |
| `transferId` | UUID | Saga transfer ID |
| `amount` | Long | Amount credited |
| `currency` | String | ISO 4217 code |

---

**`FundsRefunded` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Account receiving refund |
| `transferId` | UUID | Original transfer saga ID |
| `amount` | Long | Amount refunded |
| `currency` | String | ISO 4217 code |

---

**`TransferCompleted` / `TransferFailed` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `transferId` | UUID | Saga transfer ID |
| `sourceAccountId` | UUID | Debited account |
| `destinationAccountId` | UUID | Credited account |
| `amount` | Long | Amount transferred |
| `reason` | String (nullable) | Failure reason (`TransferFailed` only) |
| `failedStep` | String (nullable) | Which saga step failed |

---

**`AccountClosed` (schemaVersion: 1)**

| Field | Type | Description |
|---|---|---|
| `accountId` | UUID | Closed account ID |
| `closedAt` | ISO-8601 UTC | Closure timestamp |
| `reason` | String (nullable) | Optional closure reason |

---

### 6.4 Event Versioning Strategy

| Rule | Specification |
|---|---|
| **Backward-compatible changes** | New optional fields may be added to an event payload without incrementing `schemaVersion`. Consumers must tolerate unknown fields (lenient deserialization). |
| **Breaking changes** | Removing, renaming, or changing the type of a field requires incrementing `schemaVersion`. The system supports multiple schema versions concurrently. |
| **Consumer upcasting** | A registered `EventUpcaster` component transforms events from older schema versions to the current schema at deserialization time. |
| **Original payload preservation** | The `domain_events.payload` column stores the original serialized JSON. Upcasting occurs in-memory only; stored events are never mutated. |
| **Schema registry** | Event schemas are published to a schema registry (e.g., Confluent Schema Registry) for documentation and consumer validation. |


---

## 7. System Behavior & Workflows

### 7.1 Deposit Workflow

```
Client
  │
  ├─1─► POST /accounts/{accountId}/deposit
  │      { amount, currency, description, idempotencyKey, issuedBy }
  │
  ▼
Command API
  │
  ├─2─► Authenticate JWT; validate request schema
  ├─3─► Check idempotency_keys table for idempotencyKey
  │       └─ If found → return cached HTTP 202 response (stop)
  │
  ▼
Command Handler
  │
  ├─4─► Load Account aggregate:
  │       a. Query account_snapshots for latest snapshot
  │       b. Query domain_events for events after snapshot_version
  │       c. Apply events to aggregate in order
  │
  ├─5─► Aggregate validates:
  │       - Account status == ACTIVE
  │       - amount > 0
  │       - amount <= MAX_AMOUNT
  │       └─ Failure → return HTTP 422 with error code
  │
  ├─6─► Aggregate produces FundsDeposited event:
  │       { eventId, aggregateVersion = current+1, correlationId=transactionId, ... }
  │
  ├─7─► BEGIN TRANSACTION (PostgreSQL):
  │       a. INSERT INTO domain_events (check unique aggregateVersion constraint)
  │       b. INSERT INTO event_outbox (status=PENDING)
  │       c. INSERT INTO idempotency_keys (key, result, response)
  │       └─ COMMIT — on unique constraint violation, retry from step 4 (max 3x)
  │
  ├─8─► Return HTTP 202 { transactionId, status: ACCEPTED, eventId }
  │
  ▼
Outbox Relay (async, separate process)
  │
  ├─9─► Poll event_outbox WHERE status=PENDING
  ├─10─► Publish to Kafka topic account-events (key=accountId)
  └─11─► UPDATE event_outbox SET status=PUBLISHED, published_at=now()
  │
  ▼
Projection Handler (Kafka Consumer)
  │
  ├─12─► Consume FundsDeposited from account-events
  ├─13─► UPSERT account_summary_view: balance += amount, version = aggregateVersion
  ├─14─► INSERT INTO transaction_history_view
  └─15─► Commit Kafka offset
```

---

### 7.2 Withdrawal Workflow

Steps 1–8 are identical to the Deposit Workflow with these differences:

- **Command:** `WithdrawFunds`
- **Step 5 validation (additional):** `account.balance - amount >= 0` → if false, return HTTP 422 `INSUFFICIENT_FUNDS`
- **Event produced:** `FundsWithdrawn`
- **Step 13:** `balance -= amount`

---

### 7.3 Transfer Funds Workflow

```
Client
  │
  ├─1─► POST /transfers
  │      { sourceAccountId, destinationAccountId, amount, currency, idempotencyKey, issuedBy }
  │
  ▼
Command API
  ├─2─► Authenticate; validate schema; check idempotency
  ├─3─► Generate transferId (UUID)
  │
  ▼
Transfer Saga (instantiated)
  │
  ├─4─► Persist saga state: { sagaId=transferId, step=DEBIT_INITIATED, status=IN_PROGRESS }
  │
  ├─5─► Issue DebitAccount command to source account Command Handler
  │       └─ Source Account Aggregate validates: ACTIVE, sufficient funds
  │           ├─ Failure → Saga transitions to FAILED; emit TransferFailed; return HTTP 422
  │           └─ Success → Append FundsDebited event; publish to Kafka
  │
  ├─6─► Saga consumes FundsDebited from Kafka
  │       └─ Update saga state: step=CREDIT_INITIATED
  │
  ├─7─► Issue CreditAccount command to destination account Command Handler
  │       └─ Destination Account Aggregate validates: ACTIVE
  │           ├─ Failure (COMPENSATE PATH):
  │           │     Saga issues RefundFunds compensating command to source account
  │           │     Source account appends FundsRefunded event
  │           │     Saga state → COMPENSATING → FAILED
  │           │     Emit TransferFailed event
  │           └─ Success → Append FundsCredited event; publish to Kafka
  │
  ├─8─► Saga consumes FundsCredited from Kafka
  │       └─ Update saga state: step=COMPLETED, status=COMPLETED
  │       └─ Emit TransferCompleted event to Kafka
  │
  └─9─► Projection Handler updates both account read models
```

**Saga State Transitions:**

```
DEBIT_INITIATED → CREDIT_INITIATED → COMPLETED
                                  ↘ COMPENSATING → FAILED
DEBIT_INITIATED → FAILED (if debit fails)
```

---

### 7.4 Event Replay Workflow

```
Trigger: Admin calls POST /admin/projections/account-summary/rebuild
  │
  ├─1─► Replay Engine creates a shadow table: account_summary_view_shadow
  │
  ├─2─► Query domain_events ORDER BY aggregate_id, aggregate_version ASC
  │       (batch size: 1000 records per query)
  │
  ├─3─► For each event batch:
  │       Apply event to in-memory aggregate state
  │       UPSERT shadow read model table
  │
  ├─4─► On completion, BEGIN TRANSACTION:
  │       ALTER TABLE account_summary_view RENAME TO account_summary_view_old
  │       ALTER TABLE account_summary_view_shadow RENAME TO account_summary_view
  │       COMMIT
  │
  ├─5─► DROP TABLE account_summary_view_old
  │
  └─6─► Return 200 OK with { eventsProcessed, duration, completedAt }
```

**Partial Replay (Historical State):**

```
GET /accounts/{accountId}/history?atVersion=10
  │
  ├─1─► Load latest snapshot WHERE snapshot_version <= 10
  ├─2─► Load domain_events WHERE aggregate_id=X AND aggregate_version BETWEEN (snapshot_version+1) AND 10
  ├─3─► Apply events to aggregate starting from snapshot state
  └─4─► Return computed state { balance, status, version=10 }
```


---

## 8. Concurrency & Consistency Handling

### 8.1 Optimistic Concurrency Control (OCC)

| ID | Specification |
|---|---|
| **SRS-CON-01** | Every command handler SHALL load the aggregate and record its current `aggregate_version` as the `expectedVersion`. |
| **SRS-CON-02** | When appending to `domain_events`, the handler SHALL set `aggregate_version = expectedVersion + 1`. The unique constraint on `(aggregate_id, aggregate_version)` ensures atomicity. |
| **SRS-CON-03** | On a unique constraint violation (`23505` PostgreSQL error code), the handler SHALL treat this as an optimistic concurrency conflict. |
| **SRS-CON-04** | On conflict, the handler SHALL reload the aggregate from the event store and re-evaluate the command up to **3 times** with exponential backoff (initial: 50ms, multiplier: 2x). |
| **SRS-CON-05** | If all retries are exhausted, the command SHALL fail with HTTP `409 Conflict` and error code `CONCURRENCY_CONFLICT`. |
| **SRS-CON-06** | Concurrent writes to **different** aggregates SHALL proceed fully in parallel with no cross-account locking or coordination. |

### 8.2 Idempotency

| ID | Specification |
|---|---|
| **SRS-IDP-01** | Every command submitted by a client MUST include an `idempotencyKey` (UUID v4). Commands without an `idempotencyKey` SHALL be rejected with HTTP `400` and error code `MISSING_IDEMPOTENCY_KEY`. |
| **SRS-IDP-02** | Before processing, the command handler SHALL query `idempotency_keys` for the submitted key. If found and `status=COMPLETED`, the handler SHALL return the cached response immediately (HTTP `202`) without reprocessing. |
| **SRS-IDP-03** | If the key is found with `status=IN_PROGRESS` (concurrent duplicate), return HTTP `409` with code `DUPLICATE_REQUEST_IN_FLIGHT`. |
| **SRS-IDP-04** | Idempotency keys SHALL be stored in the same database transaction as the domain event and outbox record. |
| **SRS-IDP-05** | Idempotency keys SHALL expire after **24 hours** from creation. Expired keys are pruned by a scheduled background job. |
| **SRS-IDP-06** | Kafka projection handlers SHALL implement upsert semantics: re-processing the same event SHALL produce the same read model state (update if `aggregateVersion` matches or is higher; discard if lower). |

### 8.3 Duplicate Event Handling (Kafka Consumer Side)

| ID | Specification |
|---|---|
| **SRS-DUP-01** | Projection handlers SHALL record the last processed `eventId` per `aggregateId` in a consumer checkpoint table. |
| **SRS-DUP-02** | Before applying an event, the projection handler SHALL verify that the incoming `aggregateVersion` is exactly `currentVersion + 1`. If it is less or equal, the event is a duplicate and SHALL be discarded (idempotent skip). |
| **SRS-DUP-03** | If the incoming `aggregateVersion` is more than `currentVersion + 1` (gap detected), the handler SHALL pause processing for that `aggregateId` and trigger a gap-fill query from the event store. |
| **SRS-DUP-04** | Unprocessable events (deserialization failure, schema mismatch) SHALL be published to `account-events-dlq` after 3 processing attempts. |

### 8.4 Double-Spend Prevention

| ID | Specification |
|---|---|
| **SRS-DS-01** | The Account aggregate SHALL maintain a `balance` field derived solely from replayed events. This balance is authoritative for all withdrawal and debit validations. |
| **SRS-DS-02** | The `WithdrawFunds` and `DebitAccount` (transfer) command handlers SHALL load the aggregate directly from the event store (not from the read model) before evaluating sufficiency. |
| **SRS-DS-03** | The balance check and event append occur within the same OCC cycle. A concurrent withdrawal that changes the balance will increment the `aggregate_version`, causing the conflicting write to fail its unique constraint and retry with the updated balance. |
| **SRS-DS-04** | The system SHALL NEVER allow the aggregate's computed balance to go below `0`. This invariant is enforced in the `Account` aggregate's domain logic and is not bypassable via any API path. |

---

## 9. Error Handling & Edge Cases

### 9.1 Network Failures

| Scenario | System Behaviour |
|---|---|
| Client disconnects after command accepted but before response | Event may still be committed. Client SHALL use same `idempotencyKey` on retry to receive the cached result. |
| Command API → Event Store connection failure | Return HTTP `503 Service Unavailable`. No event written. Client retries safely with same idempotency key. |
| Outbox Relay → Kafka connection failure | Relay retries with exponential backoff. Events remain in `PENDING` state in outbox. No data loss. |
| Projection Handler → Read Model DB failure | Consumer pauses; Kafka offset not committed; event reprocessed after reconnection. |

### 9.2 Partial Failures

| Scenario | System Behaviour |
|---|---|
| Event written to `domain_events` but outbox record fails | Impossible by design: both writes are in the same DB transaction. If the transaction fails, neither write persists. |
| Outbox published to Kafka but `published_at` update fails | Relay republishes the event to Kafka (at-least-once). Consumer idempotency handling (SRS-DUP-01 to SRS-DUP-03) ensures no duplicate read model update. |
| Read model is stale after write | Clients requiring up-to-date data use the `asOfVersion` polling pattern (SRS-QRY-08) or query the write side for aggregate state reconstruction. |
| Transfer debit committed; credit fails | Transfer Saga issues `RefundFunds` compensating command. Compensating command retried until `FundsRefunded` event is committed. Saga transitions to `FAILED` state. |

### 9.3 Kafka Failures

| Scenario | System Behaviour |
|---|---|
| Kafka broker unavailable | Outbox relay queues events in PostgreSQL outbox. Events published when Kafka recovers. Maximum outbox accumulation duration equals Kafka downtime. |
| Consumer group rebalance | In-flight event processing is abandoned; events reprocessed from last committed offset after rebalance. Consumer idempotency prevents double-application. |
| Message exceeds Kafka max message size | Command rejected at the outbox relay with alert. Event payload size SHALL be validated at write time (max 64KB per event payload). |
| DLQ message accumulation | Alert fires when DLQ record count > 0. Operations team investigates and manually replays or discards after root cause analysis. |

### 9.4 Data Corruption Scenarios

| Scenario | System Behaviour |
|---|---|
| Checksum mismatch detected on event read | Event is flagged as `CORRUPTED` in an alert log. Aggregate hydration fails with a `DATA_INTEGRITY_ERROR`. Operations team is paged. System does NOT apply corrupted events. |
| Snapshot state inconsistent with events | Snapshot is discarded; full event replay from version 0 is performed for that aggregate. Alert logged for investigation. |
| Read model out of sync with event store | Projection rebuild triggered via admin endpoint; shadow-table swap strategy ensures zero-downtime correction. |

### 9.5 Account-Specific Edge Cases

| Scenario | Behaviour |
|---|---|
| Deposit to closed account | Rejected: HTTP 422, `ACCOUNT_INACTIVE` |
| Transfer to non-existent destination | Saga fails at credit step; debit compensated via `FundsRefunded`; HTTP 404, `ACCOUNT_NOT_FOUND` |
| Concurrent deposits to same account | OCC serialises writes; all deposits succeed (no balance invariant violated) |
| Concurrent withdrawal + deposit (race) | OCC retries; the winner commits first; the loser reloads updated aggregate and re-evaluates |
| Closing account with pending saga | Close command rejected: HTTP 422, `PENDING_OPERATIONS_EXIST` (system checks for in-progress sagas) |

---

## 10. Assumptions & Dependencies

### 10.1 Technical Assumptions

| ID | Assumption |
|---|---|
| **A-01** | All monetary amounts are in a single currency (USD) for v1. The currency field is stored and validated but FX conversion is not performed. |
| **A-02** | Amounts are stored as `BIGINT` in minor units (cents). No floating-point arithmetic is performed anywhere in the system for monetary values. |
| **A-03** | User authentication and identity management are handled entirely by an external IdP. The system receives a pre-validated JWT and trusts its claims. JWT public keys are fetched from the IdP's JWKS endpoint and cached with a configurable TTL. |
| **A-04** | The PostgreSQL event store natively supports the unique constraint on `(aggregate_id, aggregate_version)` as the OCC mechanism. No application-level locking is required. |
| **A-05** | Kafka is deployed before system launch with adequate retention (`log.retention.ms` ≥ 90 days for `account-events`), replication factor 3, and `min.insync.replicas=2`. |
| **A-06** | Network partitions between the event store and Kafka are fully handled by the Outbox pattern. The relay process is the only component that bridges these two systems. |
| **A-07** | Account holders are pre-registered users. User registration, KYC, and onboarding are handled by an upstream service. This system receives only a valid `ownerId` reference. |
| **A-08** | Snapshots are a performance optimisation, not a correctness requirement. The system is fully correct without snapshots; they only affect replay latency. |
| **A-09** | Kafka topic retention may be shorter than event store retention. The event store is the authoritative long-term archive. Projection rebuilds always read from the event store, not Kafka. |
| **A-10** | All inter-service communication (Command API → Command Handler, Saga → Command Handler) occurs over a secure internal network with mTLS enforced. |
| **A-11** | The `description` field in transaction events is optional and non-searchable in v1. Full-text search on descriptions is a future enhancement. |
| **A-12** | The system is deployed in a single geographic region for v1. Multi-region active-active replication is deferred to a future release. |

### 10.2 External Dependencies

| Dependency | Purpose | Failure Impact | Mitigation |
|---|---|---|---|
| **PostgreSQL (Event Store)** | Authoritative event persistence | Write side unavailable | Synchronous replication; automatic failover (Patroni/pg_auto_failover) |
| **PostgreSQL (Read Model)** | Query serving | Read side degraded | Read replicas; circuit breaker returns stale data with warning header |
| **Apache Kafka** | Event distribution | Projection lag; no new events published | Outbox buffers events; catch-up on recovery |
| **External IdP (JWT Issuer)** | Authentication | All API requests fail (401) | JWKS public key caching (15-min TTL); cached keys allow continued validation during brief IdP downtime |
| **Secrets Manager** | Credential management | Service startup fails | Cache credentials in encrypted memory; restart triggers re-fetch |
| **Schema Registry** | Event schema validation | Deserialization warnings | System continues with local schema definitions; registry used for governance, not hard enforcement |
| **OpenTelemetry Collector** | Distributed tracing export | Trace loss | Non-blocking export; trace loss does not affect functional correctness |

---

*End of Document — SRS v1.0*

---

> **Document Control:** This SRS is derived from BRD v1.0. Any changes to the BRD must be reviewed for SRS impact. Version history is tracked in the project's version control system.
