# High-Level Design (HLD)
## Event-Sourced Financial Ledger using CQRS

---

| Field | Detail |
|---|---|
| **Document Version** | 1.0 |
| **Status** | Draft |
| **Date** | 2026-05-02 |
| **Derived From** | SRS v1.0 — Event-Sourced Financial Ledger |
| **Author** | Architecture Team |
| **Audience** | Backend Engineers, System Design Review, DevOps/SRE |

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Component Breakdown](#3-component-breakdown)
4. [Data Flow Architecture](#4-data-flow-architecture)
5. [CQRS Design](#5-cqrs-design)
6. [Event-Driven Architecture](#6-event-driven-architecture)
7. [Database Design (High-Level)](#7-database-design-high-level)
8. [Scalability & Performance Strategy](#8-scalability--performance-strategy)
9. [Fault Tolerance & Recovery](#9-fault-tolerance--recovery)
10. [Security Overview](#10-security-overview)
11. [Assumptions & Tradeoffs](#11-assumptions--tradeoffs)
12. [Production Improvements & Potential Weaknesses](#12-production-improvements--potential-weaknesses)

---

## 1. System Overview

### 1.1 Architecture Summary

This system is a **production-grade, event-sourced financial ledger** built on the **CQRS (Command Query Responsibility Segregation)** pattern. It provides a backend platform for managing financial accounts and processing monetary transactions — deposits, withdrawals, and cross-account fund transfers — with strict consistency, full auditability, and horizontal scalability.

The architecture is designed around three foundational principles:

- **Event Sourcing:** The system never stores current state directly. Instead, every state-changing operation produces an immutable domain event. The current state of any account is always derived by replaying its event history in order. This provides a complete, tamper-evident audit log as a first-class architectural concern — not an afterthought.

- **CQRS:** The write path (commands) and read path (queries) are fully separated. Commands are handled by an aggregate-based domain model backed by an append-only PostgreSQL event store. Queries are served from denormalised read models maintained asynchronously via Kafka-driven projection handlers. The two sides scale independently, are optimised for their respective workloads, and never share a database.

- **Immutability:** Once a domain event is written to the event store, it is never updated or deleted — by design, by database role restrictions, and by application enforcement. All state changes are expressed as new events. This guarantees that the full history of any account is always reconstructable.

### 1.2 Core Design Principles

| Principle | Application in This System |
|---|---|
| **Append-only Event Store** | `domain_events` table accepts only `INSERT`. No `UPDATE` or `DELETE` is permitted at the DB role level. |
| **Strong consistency on write** | Optimistic Concurrency Control (OCC) via `UNIQUE(aggregate_id, aggregate_version)` ensures no two concurrent writes produce conflicting state. |
| **Eventual consistency on read** | Read models are updated asynchronously via Kafka consumers. Target propagation lag is < 500ms under steady-state load. |
| **No distributed transactions** | The Outbox pattern bridges the event store and Kafka without XA/2PC. The Saga pattern manages multi-step cross-account workflows. |
| **Idempotency by design** | Every command carries a client-supplied `idempotencyKey`. The system guarantees exactly-once processing semantics regardless of client retries. |
| **PII-free event payloads** | Events reference users by opaque `ownerId` only. No names, emails, or other personally identifiable information appear in the event store, Kafka messages, or logs. |
| **Minor-unit monetary arithmetic** | All amounts are stored and computed as `BIGINT` (cents). Floating-point types are prohibited for any monetary value. |

---

## 2. Architecture Diagram

### 2.1 Full System Architecture

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                          CLIENT LAYER                                        ║
║           (Web App / Mobile App / Internal Services)                         ║
╚══════════════════════════╤═══════════════════════════════╤════════════════════╝
                           │  REST (HTTPS + JWT)           │  REST (HTTPS + JWT)
                           ▼                               ▼
╔══════════════════════════════════╗     ╔══════════════════════════════════════╗
║      COMMAND API                 ║     ║         QUERY API                    ║
║  (Spring Boot — Write Side)      ║     ║  (Spring Boot — Read Side)           ║
║                                  ║     ║                                      ║
║  POST /accounts                  ║     ║  GET /accounts/{id}/balance          ║
║  POST /accounts/{id}/deposit     ║     ║  GET /accounts/{id}/transactions     ║
║  POST /accounts/{id}/withdraw    ║     ║  GET /transactions/{id}              ║
║  POST /transfers                 ║     ║  GET /accounts/{id}/history          ║
║  DELETE /accounts/{id}           ║     ║  GET /admin/audit                    ║
║                                  ║     ║  POST /admin/projections/rebuild     ║
╚══════════════╤═══════════════════╝     ╚══════════════════╤═══════════════════╝
               │                                            │
               ▼                                            ▼
╔══════════════════════════════════╗     ╔══════════════════════════════════════╗
║      COMMAND HANDLER             ║     ║         READ MODEL DB                ║
║  (Domain / Application Layer)    ║     ║  (PostgreSQL — Denormalised Views)   ║
║                                  ║     ║                                      ║
║  - Validates JWT & schema        ║     ║  • account_summary_view              ║
║  - Checks idempotency keys       ║     ║  • transaction_history_view          ║
║  - Loads Account Aggregate       ║     ║  • audit_log_view                    ║
║  - Enforces business invariants  ║     ║                                      ║
║  - Produces domain events        ║     ║  (Served by read replicas)           ║
║  - OCC via aggregate versioning  ║     ║                                      ║
╚══════════════╤═══════════════════╝     ╚══════════════════▲═══════════════════╝
               │                                            │
               ▼                                            │ UPSERT
╔══════════════════════════════════╗     ╔══════════════════╧═══════════════════╗
║      EVENT STORE (Write DB)      ║     ║      PROJECTION HANDLER              ║
║  (PostgreSQL — Append-Only)      ║     ║  (Kafka Consumer — Spring Kafka)     ║
║                                  ║     ║                                      ║
║  • domain_events                 ║     ║  - Consumes domain events from Kafka ║
║  • event_outbox                  ║     ║  - Applies events to read models     ║
║  • account_snapshots             ║     ║  - Implements upsert + idempotency   ║
║  • idempotency_keys              ║     ║  - Gap detection + DLQ routing       ║
║  • saga_state                    ║     ║                                      ║
╚══════════════╤═══════════════════╝     ╚══════════════════▲═══════════════════╝
               │                                            │
               │ (same DB transaction)                      │ Kafka consume
               ▼                                            │
╔══════════════════════════════════╗                        │
║      OUTBOX RELAY                ║                        │
║  (Background Process)            ║     ╔══════════════════╧═══════════════════╗
║                                  ║     ║      APACHE KAFKA CLUSTER            ║
║  - Polls event_outbox (PENDING)  ║────►║                                      ║
║  - Publishes to Kafka topics     ║     ║  Topics:                             ║
║  - Marks records PUBLISHED       ║     ║  • account-events (12 partitions)    ║
║                                  ║     ║  • transfer-saga-events              ║
╚══════════════════════════════════╝     ║  • account-events-dlq                ║
                                         ║                                      ║
╔══════════════════════════════════╗     ║  Config:                             ║
║      TRANSFER SAGA               ║◄───►║  • replication.factor = 3            ║
║  (Saga / Process Manager)        ║     ║  • min.insync.replicas = 2           ║
║                                  ║     ║  • acks = all                        ║
║  - Orchestrates debit → credit   ║     ║  • enable.idempotence = true         ║
║  - Issues compensating commands  ║     ╚══════════════════════════════════════╝
║  - Persists state in saga_state  ║
║  - Resumes on restart            ║
╚══════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════════╗
║                      CROSS-CUTTING CONCERNS                                  ║
║                                                                              ║
║  • OpenTelemetry → Jaeger/Tempo (distributed tracing)                        ║
║  • Micrometer → Prometheus → Grafana (metrics + alerting)                    ║
║  • Structured JSON logging (ELK / Loki)                                      ║
║  • HashiCorp Vault / AWS Secrets Manager (credential management)             ║
║  • External IdP (JWT issuer + JWKS endpoint)                                 ║
║  • Schema Registry (Confluent) — event schema governance                     ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### 2.2 Write Path (Command Flow — Simplified)

```
Client
  └─► Command API (REST)
        └─► Command Handler
              ├─► Load Aggregate ◄── Event Store (PostgreSQL)
              ├─► Validate Business Invariants
              ├─► Produce Domain Event
              └─► BEGIN TRANSACTION
                    ├─► INSERT domain_events
                    ├─► INSERT event_outbox
                    ├─► INSERT idempotency_keys
                    └─► COMMIT
                          └─► Outbox Relay ──► Kafka (account-events)
```

### 2.3 Read Path (Query Flow — Simplified)

```
Kafka (account-events)
  └─► Projection Handler (Kafka Consumer)
        └─► UPSERT Read Model DB
              ├─► account_summary_view
              └─► transaction_history_view

Client
  └─► Query API (REST)
        └─► Read Model DB (PostgreSQL Read Replica)
              └─► HTTP 200 Response (balance / history / audit)
```

---

## 3. Component Breakdown

### 3.1 Command API

| Attribute | Detail |
|---|---|
| **Responsibility** | Entry point for all state-changing operations. Authenticates requests, validates input schema, enforces idempotency, and routes commands to the Command Handler. |
| **Input** | HTTP POST/DELETE requests with JSON bodies containing command data and a `idempotencyKey`. JWT Bearer token in Authorization header. |
| **Output** | HTTP 202 Accepted (async acknowledgement) or HTTP 4xx/5xx with structured error responses. |
| **Dependencies** | External IdP (JWT validation), Event Store DB (idempotency check), Command Handler. |
| **Technology** | Java 21, Spring Boot 3.x, Spring Web MVC, Spring Security OAuth2 Resource Server. |
| **Key Behaviours** | - Rejects all requests without a valid JWT (HTTP 401). - Validates `idempotencyKey` presence (HTTP 400 if missing). - Returns cached HTTP 202 for duplicate idempotency keys without re-processing. - Assigns `traceId` (OpenTelemetry) to every incoming request. |

---

### 3.2 Command Handler (Domain / Application Layer)

| Attribute | Detail |
|---|---|
| **Responsibility** | The core write-side orchestrator. Loads the Account aggregate, enforces business invariants, produces domain events, and persists them atomically. |
| **Input** | Validated command objects (e.g., `DepositFundsCommand`, `WithdrawFundsCommand`). |
| **Output** | Committed domain events in `domain_events` + outbox records in `event_outbox`. Returns event metadata to Command API. |
| **Dependencies** | Event Store DB (read for aggregate hydration, write for event append), Idempotency store. |
| **Technology** | Java 21, Spring Boot 3.x, Spring Data JPA / JOOQ, HikariCP. |
| **Key Behaviours** | - Loads aggregate by replaying all events from the event store (snapshot-assisted if available). - Enforces OCC by appending with `expectedVersion + 1`; retries up to 3 times on conflict. - Wraps `domain_events + event_outbox + idempotency_keys` write in a single PostgreSQL transaction. - Sets `SERIALIZABLE` isolation for event append transactions. |

---

### 3.3 Account Aggregate

| Attribute | Detail |
|---|---|
| **Responsibility** | The domain model for a financial account. Owns and enforces all business invariants: non-negative balance, valid status transitions, amount limits, currency validation. |
| **Input** | Sequence of domain events applied in `aggregate_version` order during hydration. Command validation inputs. |
| **Output** | New domain event(s) representing the state change (e.g., `FundsDeposited`). Or a domain exception if an invariant is violated. |
| **Dependencies** | None — the aggregate is a pure in-memory domain object with no external I/O. |
| **Technology** | Java 21 (pure domain objects, no framework coupling). |
| **Key Behaviours** | - Balance is always computed from replayed events, never from a mutable field updated in place. - Never allows balance to go below 0. - Rejects commands on CLOSED or SUSPENDED accounts. - Enforces max single-transaction amount (9,999,999,999 minor units). |

---

### 3.4 Event Store (PostgreSQL — Write DB)

| Attribute | Detail |
|---|---|
| **Responsibility** | The single source of truth for the entire system. An append-only, ordered, tamper-evident log of all domain events. |
| **Input** | `INSERT` statements for new domain events and outbox records from the Command Handler. |
| **Output** | Event sequences for aggregate hydration; outbox records for the Outbox Relay. |
| **Dependencies** | None (self-contained). |
| **Technology** | PostgreSQL 15+, hash-partitioned `domain_events` table, synchronous streaming replication (min 2 standbys). |
| **Key Behaviours** | - `UNIQUE(aggregate_id, aggregate_version)` enforces OCC at the DB level. - `INSERT`/`SELECT` only at the application write-role level (no `UPDATE`/`DELETE`). - SHA-256 checksum per event for tamper detection. - Event retention: minimum 7 years. - Hash-partitioned on `aggregate_id` (16 partitions) for large-scale volume. |

---

### 3.5 Outbox Relay

| Attribute | Detail |
|---|---|
| **Responsibility** | Bridges the PostgreSQL event store and Apache Kafka reliably. Ensures all committed events are eventually published to Kafka without data loss, even during Kafka outages. |
| **Input** | Polls `event_outbox` for records with `status = PENDING`. |
| **Output** | Kafka messages published to the `account-events` topic; outbox record updated to `status = PUBLISHED`. |
| **Dependencies** | Event Store DB (read outbox), Apache Kafka (produce). |
| **Technology** | Java 21, Spring Kafka, scheduled poller (Spring `@Scheduled` or Debezium CDC). |
| **Key Behaviours** | - Publishes events in order within each `aggregate_id` partition key. - Exactly-once delivery tracked via `published_at` timestamp. - Retries with exponential backoff on Kafka unavailability. - Never blocks Command API processing (fully asynchronous). |

---

### 3.6 Apache Kafka Cluster

| Attribute | Detail |
|---|---|
| **Responsibility** | Durable, ordered event bus that decouples the write side from all downstream consumers (projections, saga, audit). |
| **Input** | Domain events published by the Outbox Relay (keyed by `accountId`). |
| **Output** | Ordered event streams consumed by Projection Handlers, Transfer Saga, and Audit Handler. |
| **Dependencies** | None (infrastructure component). |
| **Technology** | Apache Kafka 3.x, Spring Kafka client. |
| **Key Behaviours** | - `account-events`: 12 partitions, partitioned by `accountId` (preserves per-account ordering). - `replication.factor=3`, `min.insync.replicas=2` on all topics. - `acks=all`, `enable.idempotence=true` on producer side. - `isolation.level=read_committed`, `enable.auto.commit=false` on consumer side. - DLQ topic (`account-events-dlq`) for unprocessable messages. |

---

### 3.7 Projection Handler

| Attribute | Detail |
|---|---|
| **Responsibility** | Consumes domain events from Kafka and maintains the read model (denormalised views) in the Read Model DB. |
| **Input** | Domain events from the `account-events` Kafka topic. |
| **Output** | UPSERT operations on `account_summary_view` and `transaction_history_view` in the Read Model DB. |
| **Dependencies** | Apache Kafka (consume), Read Model DB (write). |
| **Technology** | Java 21, Spring Kafka (`@KafkaListener`), Spring Data JPA, PostgreSQL (Read Model DB). |
| **Key Behaviours** | - Idempotent upserts: re-processing the same event produces identical state. - Discards events with `aggregateVersion ≤ currentVersion` (duplicate detection). - Detects version gaps and triggers gap-fill from the event store. - Routes unprocessable events to DLQ after 3 attempts. - Manual Kafka offset commit only after successful DB write. |

---

### 3.8 Query API

| Attribute | Detail |
|---|---|
| **Responsibility** | Serves all read operations. Provides account balance, transaction history, date-range queries, historical state, and audit log queries. Never touches the write-side event store for normal operations. |
| **Input** | HTTP GET requests with JWT authentication. |
| **Output** | JSON responses from the Read Model DB. |
| **Dependencies** | Read Model DB (read replicas), Event Store DB (only for `atVersion` history queries via event replay). |
| **Technology** | Java 21, Spring Boot 3.x, Spring Web MVC, Spring Data JPA. |
| **Key Behaviours** | - p99 latency target: < 50ms for balance queries, < 100ms for history queries. - Supports `asOfVersion` parameter: polls until read model catches up (max 5 seconds), then returns HTTP 202 with retry hint. - Routes all reads to read replicas; never to the event store primary. - Operator/Admin endpoints protected by role claims in JWT. |

---

### 3.9 Transfer Saga (Process Manager)

| Attribute | Detail |
|---|---|
| **Responsibility** | Orchestrates the two-phase, cross-account fund transfer: debit source → credit destination. Handles compensation (rollback of debit) if the credit step fails. |
| **Input** | `TransferFunds` command from the Command API. Domain events from Kafka (`FundsDebited`, `FundsCredited`). |
| **Output** | Saga state transitions persisted to `saga_state` table. Compensating `RefundFunds` commands on failure. `TransferCompleted` / `TransferFailed` events published to Kafka. |
| **Dependencies** | Event Store DB (`saga_state`), Command Handler (issue debit/credit sub-commands), Apache Kafka (consume and produce). |
| **Technology** | Java 21, Spring Kafka, Spring Data JPA. |
| **Key Behaviours** | - State machine: `DEBIT_INITIATED → CREDIT_INITIATED → COMPLETED` (happy path) or `→ COMPENSATING → FAILED`. - Saga state persisted to PostgreSQL before each step transition (durable). - On restart, resumes all in-progress sagas from their last persisted step. - OCC applied to `saga_state` via a `version` column. |

---

### 3.10 Event Replay Engine

| Attribute | Detail |
|---|---|
| **Responsibility** | Rebuilds read models from the event store, supports historical state queries, and manages snapshot creation/loading. |
| **Input** | Admin API trigger (`POST /admin/projections/{name}/rebuild`). Historical query parameters (`atVersion`, `atTimestamp`). |
| **Output** | Rebuilt read model tables (via shadow-table swap). Reconstructed historical aggregate state (returned via Query API). |
| **Dependencies** | Event Store DB (full read access), Read Model DB (shadow table writes). |
| **Technology** | Java 21, Spring Batch or custom streaming processor, PostgreSQL DDL (`RENAME TABLE`). |
| **Key Behaviours** | - Writes to a shadow table, then atomically renames it to the live table (zero downtime). - Processes events in batches of 1,000. - Snapshot-assisted replay: loads latest snapshot ≤ target version, then applies only subsequent events. - Snapshots created asynchronously (do not block command processing). - Snapshot auto-trigger: after 50 events since last snapshot (configurable). |

---

## 4. Data Flow Architecture

### 4.1 Account Creation Flow

```
1. Client sends: POST /accounts
   Body: { ownerName, currency, idempotencyKey, issuedBy }

2. Command API:
   ├─ Validates JWT (signature, expiry, issuer, audience)
   ├─ Validates request schema (ownerName non-null, currency=USD, idempotencyKey valid UUID)
   └─ Checks idempotency_keys table → not found → proceed

3. Command Handler:
   ├─ Generates accountId (UUID v4)
   ├─ Attempts to load aggregate → no existing events found → new aggregate
   └─ Account Aggregate:
       - Validates no duplicate accountId in event store
       - Produces AccountCreated { accountId, ownerId, currency, initialBalance=0 }

4. Atomic PostgreSQL Transaction:
   ├─ INSERT domain_events (event_id, event_type='AccountCreated', aggregate_id, aggregate_version=1, ...)
   ├─ INSERT event_outbox (status=PENDING, topic='account-events', payload=...)
   ├─ INSERT idempotency_keys (key, result_event_id, response_payload, expires_at=+24h)
   └─ COMMIT

5. Command API returns: HTTP 202 { accountId, status: CREATED, eventId }

6. [Async] Outbox Relay:
   ├─ Polls event_outbox WHERE status=PENDING
   ├─ Publishes AccountCreated to Kafka topic 'account-events' (key=accountId)
   └─ UPDATE event_outbox SET status=PUBLISHED

7. [Async] Projection Handler (Kafka Consumer):
   ├─ Consumes AccountCreated event
   ├─ INSERT INTO account_summary_view (account_id, owner_id, balance=0, status=ACTIVE, ...)
   └─ Commits Kafka offset
```

---

### 4.2 Deposit Transaction Flow

```
1. Client sends: POST /accounts/{accountId}/deposit
   Body: { amount, currency, description, idempotencyKey, issuedBy }

2. Command API:
   ├─ JWT validation + schema validation
   └─ Idempotency check → not found → proceed

3. Command Handler:
   ├─ Load Account Aggregate:
   │   a. Query account_snapshots for latest snapshot
   │   b. Query domain_events for events after snapshot_version
   │   c. Apply events in order to reconstruct current state
   └─ Account Aggregate validates:
       ├─ status == ACTIVE       → pass
       ├─ amount > 0             → pass
       └─ amount ≤ MAX_AMOUNT    → pass
       Produces: FundsDeposited { accountId, amount, currency, transactionId, description }

4. Atomic PostgreSQL Transaction:
   ├─ INSERT domain_events (aggregate_version = currentVersion + 1)
   ├─ INSERT event_outbox
   ├─ INSERT idempotency_keys
   └─ COMMIT (unique constraint violation → OCC retry up to 3x)

5. HTTP 202 returned to client

6. [Async] Outbox Relay → Kafka 'account-events'

7. [Async] Projection Handler:
   ├─ Consumes FundsDeposited
   ├─ UPDATE account_summary_view SET balance = balance + amount, aggregate_version = new_version
   ├─ INSERT INTO transaction_history_view (type=DEPOSIT, amount, occurred_at, ...)
   └─ Commit Kafka offset
```

---

### 4.3 Fund Transfer Flow (Saga)

```
1. Client sends: POST /transfers
   Body: { sourceAccountId, destinationAccountId, amount, currency, idempotencyKey }

2. Command API: JWT validation + idempotency check → proceed
   Generates transferId (UUID)

3. Transfer Saga instantiated:
   ├─ Persists saga state: { sagaId=transferId, step=DEBIT_INITIATED, status=IN_PROGRESS }

── STEP 1: DEBIT ──────────────────────────────────────────────────────────────

4. Saga issues DebitAccount command to source account Command Handler
   ├─ Load source Account Aggregate from event store
   ├─ Aggregate validates: ACTIVE, balance - amount >= 0
   │   └─ FAIL → Saga transitions to FAILED; TransferFailed event emitted; HTTP 422
   └─ SUCCESS → Append FundsDebited event; publish to Kafka

5. Outbox Relay → Kafka 'account-events' (FundsDebited)

── STEP 2: CREDIT ─────────────────────────────────────────────────────────────

6. Saga consumes FundsDebited from Kafka
   └─ Update saga state: step=CREDIT_INITIATED

7. Saga issues CreditAccount command to destination account Command Handler
   ├─ Load destination Account Aggregate from event store
   ├─ Aggregate validates: ACTIVE
   │   └─ FAIL (COMPENSATE PATH):
   │       ├─ Saga issues RefundFunds compensating command to source account
   │       ├─ Source account appends FundsRefunded event
   │       ├─ Saga state → COMPENSATING → FAILED
   │       └─ TransferFailed event emitted → Kafka
   └─ SUCCESS → Append FundsCredited event; publish to Kafka

── STEP 3: COMPLETE ───────────────────────────────────────────────────────────

8. Saga consumes FundsCredited from Kafka
   ├─ Update saga state: step=COMPLETED, status=COMPLETED
   └─ Emit TransferCompleted event to Kafka

9. Projection Handler updates both account read models:
   ├─ Source: balance -= amount, INSERT transaction (TRANSFER_DEBIT)
   └─ Destination: balance += amount, INSERT transaction (TRANSFER_CREDIT)
```

---

### 4.4 Event Replay / Projection Rebuild Flow

```
1. Admin sends: POST /admin/projections/account-summary/rebuild
   (JWT must carry OPERATOR or ADMIN role claim)

2. Replay Engine:
   └─ Creates shadow table: account_summary_view_shadow

3. Streams all domain_events:
   └─ SELECT * FROM domain_events ORDER BY aggregate_id, aggregate_version ASC
      (batches of 1,000 records)

4. For each event batch:
   ├─ Load latest snapshot for aggregate (if available)
   ├─ Apply events to in-memory aggregate starting from snapshot state
   └─ UPSERT into account_summary_view_shadow

5. On completion — Atomic swap:
   BEGIN TRANSACTION
     ALTER TABLE account_summary_view RENAME TO account_summary_view_old
     ALTER TABLE account_summary_view_shadow RENAME TO account_summary_view
   COMMIT

6. DROP TABLE account_summary_view_old

7. Return HTTP 200 { eventsProcessed, duration, completedAt }
   ─ Query API continues serving from live table throughout (zero downtime)

── HISTORICAL STATE QUERY ─────────────────────────────────────────────────────

GET /accounts/{id}/history?atVersion=10

1. Load latest snapshot WHERE snapshot_version <= 10
2. Load domain_events WHERE aggregate_id=X AND aggregate_version BETWEEN (snapshot_version+1) AND 10
3. Apply events sequentially to snapshot state (or from scratch if no snapshot)
4. Return: { balance, status, currency, version=10, computedAt }
```

---

## 5. CQRS Design

### 5.1 Separation of Write and Read Models

The system maintains two entirely separate models that never share a database:

| Dimension | Write Model (Command Side) | Read Model (Query Side) |
|---|---|---|
| **Data Store** | PostgreSQL Event Store (`domain_events`) | PostgreSQL Read Model DB (denormalised views) |
| **Consistency** | Strong (OCC within aggregate) | Eventual (< 500ms propagation lag) |
| **Optimised For** | Correctness, invariant enforcement, auditability | Query speed, pagination, filtering |
| **Updated By** | Command Handler (synchronous, within transaction) | Projection Handler (async, via Kafka) |
| **Schema Style** | Normalised, append-only event log | Denormalised, flat tables with indexes |
| **Can Be Rebuilt?** | No (authoritative source of truth) | Yes (drop and replay from event store) |
| **Isolation Level** | SERIALIZABLE (writes) | READ COMMITTED |

### 5.2 Why This Separation Exists

- **Different access patterns:** The write side needs transactional integrity with version-based conflict detection. The read side needs fast, indexed lookups with pagination across large datasets. A single model cannot be optimal for both.

- **Independent scaling:** Command throughput is bounded by write-side concurrency and event store performance. Read throughput can be scaled horizontally by adding Query API instances and read replicas, completely independent of the write path.

- **Read model flexibility:** The read model schema can be changed freely — add new projections, reformat data, add derived columns — without touching the event store. The event store remains immutable; only the read-side interpretation changes.

- **Temporal queries for free:** Because the write side stores events (not current state), reconstructing an account's state at any point in history is a natural capability of the architecture, not a bolt-on.

### 5.3 Eventual Consistency Implications

- Most queries (balance checks, transaction history) are served from the eventually-consistent read model and tolerate a < 500ms lag.
- **Critical balance checks** (pre-withdrawal validation, pre-debit validation for transfers) always load the aggregate directly from the event store — never from the read model. This prevents double-spend vulnerabilities.
- Clients requiring strong read-after-write consistency can use the `asOfVersion` polling parameter, which blocks until the read model catches up to a specific event version.

---

## 6. Event-Driven Architecture

### 6.1 Kafka Topics Structure

| Topic | Partitioned By | Partition Count | Retention | Purpose |
|---|---|---|---|---|
| `account-events` | `accountId` | 12 (min) | 90 days | All account domain events (primary event bus) |
| `transfer-saga-events` | `transferId` | 12 | 30 days | Saga coordination events between saga steps |
| `account-events-dlq` | N/A | 1 | 365 days | Unprocessable messages for manual investigation |

### 6.2 Producers and Consumers

| Component | Role | Topic |
|---|---|---|
| **Outbox Relay** | Producer | `account-events`, `transfer-saga-events` |
| **Transfer Saga** | Producer + Consumer | `account-events` (consume), `transfer-saga-events` (produce + consume) |
| **Projection Handler** | Consumer | `account-events` |
| **Audit Handler** | Consumer | `account-events` |
| **DLQ Handler** | Consumer | `account-events-dlq` |

### 6.3 Event Ordering Strategy

- **Per-account ordering:** All events for a given account are published to the same Kafka partition by using `accountId` as the Kafka message key. This guarantees ordered delivery within a single account's event stream to all consumers.
- **Cross-account ordering:** No global ordering guarantee is required or expected. Transfer saga events are keyed by `transferId` on the saga topic to keep coordination events co-located.
- **Version-based ordering enforcement:** Projection handlers additionally validate that incoming `aggregateVersion = currentVersion + 1`. Any gap triggers a gap-fill from the event store, making the consumer resilient to rare reordering at the Kafka layer.

### 6.4 Event Durability and Replayability

- **Durability:** `replication.factor=3` + `min.insync.replicas=2` ensures no event is lost if a single Kafka broker fails. Producer `acks=all` ensures acknowledgement only after all in-sync replicas have persisted the message.
- **Idempotent producer:** `enable.idempotence=true` prevents duplicate Kafka messages from producer retries.
- **Replayability from event store:** Kafka is a distribution mechanism, not the archive. The PostgreSQL event store retains all events for 7+ years. Any consumer can be rebuilt from scratch using the event store as the replay source, independent of Kafka's 90-day retention.
- **Manual offset management:** `enable.auto.commit=false`. Consumers commit offsets only after successful DB write, preventing acknowledged-but-unprocessed events.

---

## 7. Database Design (High-Level)

### 7.1 Event Store Database (Append-Only)

The event store is a dedicated PostgreSQL instance containing the following key tables:

**`domain_events` — Core event log:**
- Every state change in the system produces a row in this table.
- Hash-partitioned on `aggregate_id` (16 partitions) to distribute data and I/O across the system lifetime.
- The `UNIQUE(aggregate_id, aggregate_version)` constraint is the entire OCC mechanism — no advisory locks, no application-level version checks beyond this.
- `payload JSONB` stores the type-specific event data; `event_type` is the discriminator for deserialization.
- `checksum CHAR(64)` stores a SHA-256 hash of key fields for tamper detection during reads.
- No foreign keys to mutable tables — the event store is fully self-contained.

**`event_outbox` — Transactional messaging bridge:**
- Written in the same transaction as `domain_events` to guarantee no event is lost between the DB and Kafka.
- Polled by the Outbox Relay; records transition from `PENDING → PUBLISHED` after Kafka acknowledgement.

**`account_snapshots` — Replay performance optimiser:**
- Stores the serialised aggregate state at a specific version.
- Created asynchronously after every 50 events (configurable threshold).
- Replay loads the nearest snapshot ≤ target version, then applies only the subsequent delta.

**`idempotency_keys` — Exactly-once command processing:**
- Keyed by client-supplied UUID. TTL of 24 hours, pruned by a background scheduler.
- Cached response payload allows instant replay of the original HTTP 202 response for retried requests.

**`saga_state` — Durable transfer orchestration:**
- Persists the full state machine context for each in-flight transfer saga.
- OCC via a `version` column. Resumed on saga coordinator restart.

### 7.2 Read Model Database

**`account_summary_view` — Balance and status projection:**
- One row per account. `balance` is updated (UPSERT) on every deposit, withdrawal, debit, credit, or refund event.
- `aggregate_version` tracks the last applied event — projection handler discards any event with version ≤ this value.
- Served from read replicas to isolate read traffic from projection write load.

**`transaction_history_view` — Transaction log projection:**
- One row per transaction event (`DEPOSIT`, `WITHDRAWAL`, `TRANSFER_DEBIT`, `TRANSFER_CREDIT`, `REFUND`).
- Supports date-range filtering, pagination, and single-transaction lookups.
- B-tree indexes on: `account_id`, `occurred_at`, `transaction_id`.

### 7.3 Indexing Strategy (High-Level)

| Table | Index | Type | Purpose |
|---|---|---|---|
| `domain_events` | `(aggregate_id, aggregate_version)` | UNIQUE B-tree | OCC enforcement + aggregate hydration |
| `domain_events` | `occurred_at` | B-tree | Time-range replay queries |
| `account_snapshots` | `(aggregate_id, snapshot_version)` | UNIQUE B-tree | Latest snapshot lookup |
| `account_summary_view` | `account_id` | Primary Key | Balance lookups |
| `transaction_history_view` | `account_id` | B-tree | Transaction history queries |
| `transaction_history_view` | `occurred_at` | B-tree | Date-range filtering |
| `transaction_history_view` | `transaction_id` | B-tree | Single transaction lookup |
| `event_outbox` | `status, created_at` | B-tree (partial) | Outbox relay polling (`WHERE status='PENDING'`) |
| `idempotency_keys` | `idempotency_key` | Primary Key | O(1) duplicate detection |

---

## 8. Scalability & Performance Strategy

### 8.1 Horizontal Scaling

- **Command API:** Fully stateless Spring Boot instances behind a load balancer (Kubernetes Deployment). No session state, no in-memory caches that affect correctness. New instances added without any coordination or reconfiguration.
- **Query API:** Stateless, routes all reads to PostgreSQL read replicas. Adding read replicas and Query API instances independently expands read throughput without affecting the write path.
- **Projection Handlers:** Scale by adding Kafka consumer group members up to the partition count (12). Each partition is consumed by exactly one handler instance. Increasing partition count (requires rebalancing) expands maximum consumer parallelism.
- **Outbox Relay:** Can run as multiple competing consumers with leader election (e.g., via a DB advisory lock or Kubernetes leader election) to ensure exactly one active relay at a time while allowing fast failover.

### 8.2 Kafka Partitioning Strategy

- `account-events` is partitioned by `accountId` (12 partitions minimum). This guarantees:
  - **Ordering:** All events for a single account are processed in sequence by a single consumer.
  - **Parallelism:** Up to 12 accounts' events can be processed concurrently across the consumer group.
  - **Isolation:** A slow or stuck account's processing does not block other accounts on different partitions.
- Partition count is configurable without code changes (via Kafka topic configuration). Target throughput should be projected from `expected_events_per_second / events_per_partition_per_second`.

### 8.3 Read Model Optimisation

- **Denormalisation:** The `account_summary_view` and `transaction_history_view` are pre-computed and denormalised. Balance queries require no aggregation — a single indexed primary key lookup.
- **Read replicas:** PostgreSQL streaming replicas serve all Query API traffic. The primary handles only projection handler writes and event store operations.
- **Snapshot-assisted replay:** For the `atVersion` historical query, snapshots cut the event replay cost from O(n) to O(n - snapshot_version). For accounts with thousands of events, this is a significant latency reduction.
- **Pagination:** Transaction history uses cursor-based pagination (by `occurred_at` + `id`) rather than `OFFSET`, avoiding full-table scans at deep pages.

### 8.4 Bottleneck Analysis and Mitigation

| Bottleneck | Mitigation |
|---|---|
| **Event store write throughput** | Hash partitioning on `aggregate_id` distributes I/O. SERIALIZABLE isolation only on per-aggregate transactions (no cross-aggregate locking). Target: ≥ 5,000 events/sec per partition. |
| **Hot accounts (high-frequency writes to one account)** | OCC serialises writes per aggregate. High-volume accounts will see more OCC retries. Mitigated by exponential backoff and configurable retry count. Not avoidable without relaxing consistency guarantees. |
| **Projection lag under burst load** | Kafka acts as a buffer absorbing write bursts. Consumer lag is monitored (alert if > 5s). Projection handlers can be scaled horizontally up to partition count. |
| **Outbox relay as a chokepoint** | Relay processes in batches; multiple relay instances with leader election for failover. Alternatively, replace with Debezium CDC (change data capture) for sub-100ms outbox relay latency. |
| **Saga coordinator throughput** | Sagas run per-transfer. Each saga is independent. Throughput scales with Kafka partitions and saga consumer group size. |

---

## 9. Fault Tolerance & Recovery

### 9.1 Event Replay Mechanism

- **Read model corruption or drift:** Admin triggers `POST /admin/projections/{name}/rebuild`. Replay engine streams all events from the event store, writes to a shadow table, then atomically swaps it live. Query API remains available throughout — served from the current live table until the moment of swap.
- **New projection type:** Adding a new read model (e.g., a monthly summary view) requires no change to the event store. A new projection handler is deployed and a full replay populates the new read model from the complete event history.
- **Historical state reconstruction:** Any account's state at any point in time is reconstructable via `GET /accounts/{id}/history?atVersion={n}`. This is a first-class API concern, not a special operational procedure.

### 9.2 Consumer Failure Handling

| Failure Scenario | Recovery Behaviour |
|---|---|
| **Projection Handler crashes mid-processing** | Kafka offset was not committed (manual commit only after DB write). On restart, consumer re-reads from last committed offset and reprocesses. Idempotent upserts ensure no double-application. |
| **Kafka consumer group rebalance** | In-flight processing abandoned. Events reprocessed from last committed offset by newly assigned consumer. |
| **Projection Handler DB write failure** | Kafka offset not committed. Consumer retries processing after reconnection. Circuit breaker added around DB calls to prevent retry storms. |
| **Deserialization failure (unknown event schema)** | After 3 attempts, event is published to `account-events-dlq`. Consumer continues processing subsequent events. DLQ alert fires. |
| **Version gap detected in consumer** | Consumer pauses processing for that `aggregateId`, queries the event store directly to fill the gap, then resumes. |

### 9.3 Data Recovery Strategy

- **Event store is the authoritative archive.** Even total loss of the Read Model DB, Kafka, snapshot tables, and saga state tables can be fully recovered by replaying the event store from the beginning.
- **PostgreSQL event store:** Synchronous streaming replication to ≥ 2 standby nodes. Automatic failover via Patroni or pg_auto_failover. RPO (Recovery Point Objective) ≈ 0 (synchronous replication). RTO (Recovery Time Objective) < 30 seconds (automatic failover).
- **Kafka:** `replication.factor=3`, `min.insync.replicas=2`. Single broker failure does not cause data loss or consumer group disruption.
- **Outbox accumulation during Kafka downtime:** Events remain in `PENDING` state in the outbox table (PostgreSQL). On Kafka recovery, the relay publishes all accumulated events in order. Maximum data staleness = Kafka downtime duration.
- **Saga recovery on restart:** All in-flight sagas are persisted to PostgreSQL. On any restart, the saga coordinator queries `saga_state WHERE status='IN_PROGRESS'` and resumes each saga from its last persisted step.

---

## 10. Security Overview

### 10.1 API Security

- **Authentication:** Every Command API and Query API endpoint requires a Bearer JWT token issued by the configured external Identity Provider (IdP). Requests without a valid JWT receive HTTP 401.
- **JWT Validation:** The system validates: cryptographic signature (RS256 or ES256), expiry (`exp`), issuer (`iss`), and audience (`aud`). JWT public keys are fetched from the IdP's JWKS endpoint and cached with a 15-minute TTL, enabling continued validation during brief IdP downtime.
- **Authorisation:** Account holders can only access accounts where the JWT `sub` claim matches the account's `ownerId`. OPERATOR and ADMIN role claims in the JWT grant cross-account read access and admin endpoint access respectively.
- **TLS Enforcement:** All external traffic requires TLS 1.2 minimum (TLS 1.3 preferred). Plaintext HTTP connections are rejected at the ingress layer.

### 10.2 Data Integrity Protection

- **SHA-256 checksum per event:** Every domain event includes a checksum of `(aggregate_id + aggregate_version + event_type + payload)`. Computed at write time, verified at read time. Any tampered event is detected, flagged as CORRUPTED, and excluded from aggregate hydration.
- **Append-only enforcement:** The application write DB role has only `INSERT` and `SELECT` privileges on `domain_events`. No `UPDATE` or `DELETE` is executable, even in a compromised application. PostgreSQL row-level security or role-based access provides the enforcement layer.
- **Immutable audit trail:** The event store with 7-year retention and append-only enforcement provides a compliant, tamper-evident financial audit log by design.

### 10.3 Inter-Service Communication

- **mTLS everywhere:** All internal service-to-service communication (Command API → Command Handler, Saga → Command Handler, Outbox Relay → Kafka) uses mutual TLS. Both parties present and validate certificates.
- **Certificate rotation:** mTLS certificates rotated at minimum every 90 days, managed via a secrets manager or a service mesh (e.g., Istio).

### 10.4 Credential and Secret Management

- **Secrets manager:** All database credentials, Kafka credentials, and JWT signing keys are managed via HashiCorp Vault or AWS Secrets Manager. Credentials never appear in environment variables, config files, or container images.
- **PII isolation:** Event payloads, Kafka messages, and log entries contain only opaque `ownerId` references. Actual PII (names, contact details) is resolved via the external identity store only when needed by the calling application layer.
- **Audit log access control:** The audit query endpoint (`GET /admin/audit`) requires an OPERATOR or ADMIN role claim in the JWT.

---

## 11. Assumptions & Tradeoffs

### 11.1 Architectural Assumptions

| # | Assumption | Rationale |
|---|---|---|
| A1 | The Account is the only aggregate root in v1. | Simplifies the command model. Future extension (e.g., Portfolio aggregate) does not require schema changes — only new event types and handlers. |
| A2 | Eventual consistency (< 500ms lag) is acceptable for the read model. | This is standard for financial ledger read paths. Critical operations (withdrawal validation) always load from the event store. |
| A3 | The Outbox pattern is sufficient to bridge PostgreSQL and Kafka. | Eliminates XA/2PC complexity. The relay is a simple, restartable process. The only requirement is that the relay runs reliably with at-least-once delivery, and consumers handle idempotency. |
| A4 | Sagas are the right pattern for fund transfers. | Transfers span two aggregates. The Saga pattern provides compensating transactions and durable state without distributed locking or XA. |
| A5 | Snapshots are a performance optimisation only. | The system is correct without them. Their only impact is replay latency. This keeps the core correctness model simple: if a snapshot is absent or corrupt, full replay from version 1 is always the fallback. |
| A6 | A single-region deployment is acceptable for v1. | Deferred multi-region active-active to avoid the complexity of cross-region event ordering and conflict resolution in v1. |
| A7 | JWT validation is fully delegated to the external IdP. | Avoids duplicating auth logic. Cached JWKS keys handle brief IdP unavailability. |

### 11.2 Key Tradeoffs

| Tradeoff | Decision | Consequence |
|---|---|---|
| **Strong write consistency vs. write throughput** | OCC with up to 3 retries. | Concurrent writes to the same account are serialised. Hot accounts with extremely high concurrent write frequency will see retry latency. Acceptable for the financial domain where correctness outweighs raw write throughput. |
| **Eventual read consistency vs. read simplicity** | Read model is eventually consistent. | Balance in the read model may lag up to 500ms. Critical validations bypass the read model entirely. Clients that cannot tolerate this use the `asOfVersion` polling mechanism. |
| **Event-first over state-first** | Full event sourcing for all state changes. | Significantly increases operational complexity (event versioning, upcasting, snapshot management, projection rebuilds). The payoff is complete auditability, temporal queries, and read model flexibility — essential for a financial system. |
| **No distributed transactions (XA)** | Outbox pattern + Saga pattern. | Adds latency to cross-component operations (two-phase async vs. synchronous XA). Eliminates the notorious reliability and performance issues of distributed transactions entirely. |
| **BIGINT minor units vs. Decimal** | BIGINT (cents) for all monetary values. | No floating-point precision issues. Limits to currencies representable in two decimal places. Multi-currency support (3 decimal places, e.g., KWD) would require a minor schema adjustment. |
| **Kafka over direct DB pub/sub** | Apache Kafka as the event bus. | Operational complexity of running Kafka. The payoff: ordered, durable, replayable event streams with high throughput, fan-out to multiple consumer groups, and decoupled consumer scaling. |

---

## 12. Production Improvements & Potential Weaknesses

### 12.1 Recommended Production Improvements

**Infrastructure & Resilience:**
- **Replace the Outbox polling relay with Debezium CDC.** Polling the outbox table introduces latency proportional to the poll interval. Debezium reads PostgreSQL WAL (write-ahead log) directly, achieving near-zero latency relay with no polling overhead and no additional DB load.
- **Adopt a service mesh (Istio / Linkerd).** Centralises mTLS management, certificate rotation, retry policies, circuit breakers, and observability across all internal services without embedding these in application code.
- **Implement Kafka Schema Registry enforcement.** Currently schema registry is used for governance only. Enforcing schema compatibility checks at produce/consume time prevents schema violations from reaching consumers in production.

**Domain & Feature:**
- **Introduce domain event versioning tooling early.** As the system evolves, managing multiple `schemaVersion` values and `EventUpcaster` chains becomes complex quickly. A dedicated event schema migration framework reduces operational risk.
- **Add a query-side event-sourced aggregate (Aggregate Projection).** For heavy historical query workloads, pre-materialised aggregate state at every N-th version can serve `atVersion` queries in O(1) rather than requiring event replay.
- **Implement account-level rate limiting on commands.** A burst of deposits or withdrawals to a single account can cause excessive OCC retries, degrading throughput for that account. Token bucket rate limiting at the Command API level protects the write path.

**Observability:**
- **Define SLO-based alerting** (not just threshold-based). Alert on error budget burn rate rather than raw error counts for more actionable production signals.
- **Add consumer lag per-account tracking** for high-value accounts to detect projection staleness on specific critical accounts independently of aggregate lag metrics.

### 12.2 Potential Architectural Weaknesses

| Weakness | Impact | Mitigation |
|---|---|---|
| **Hot aggregate bottleneck** | A single account receiving extremely high concurrent write throughput will serialise through OCC retries, degrading its own write latency. | Accepted tradeoff for financial correctness. Can be partially mitigated with client-side rate limiting and backoff. True solution requires architectural changes (e.g., micro-batch command aggregation) that introduce complexity. |
| **Outbox relay as single point of failure** | If the outbox relay fails and its restart is delayed, Kafka consumers will not receive new events — read models become stale and sagas will not progress. | Multiple relay instances with leader election (only one active). Health checks and automated restarts via Kubernetes. Debezium CDC as a long-term replacement. |
| **Kafka partition rebalance disruption** | Consumer group rebalance during high load can cause processing pauses (seconds to minutes) as partitions are reassigned. | Use Kafka cooperative rebalancing (incremental rebalancing) to minimise partition movement. Size consumer groups conservatively. |
| **Long aggregate event chains** | An account with millions of events will have very high hydration cost without a snapshot, even with snapshot-assisted replay eventually. | Automatic snapshotting every 50 events (configurable) keeps this bounded. Alert when any aggregate exceeds a threshold event count without a recent snapshot. |
| **Saga non-deterministic recovery edge cases** | A saga that is in a COMPENSATING state when the system crashes may encounter a scenario where the compensating command is retried but the compensating event was already committed on the previous run. | Compensating commands must be idempotent. `FundsRefunded` events use the same `transferId` as the correlation; duplicate compensation events are detected and discarded by OCC. Careful testing of saga restart scenarios is required. |
| **Schema evolution complexity at scale** | Managing upcasters across many schema versions in a long-lived system becomes a maintenance burden. Old upcaster chains must be preserved indefinitely. | Periodic "snapshot compaction" (publishing a new event type that represents the current aggregate state, then retiring old events from active replay paths) can reduce the upcaster chain length over time. |
| **Single-region deployment risk (v1)** | A regional cloud outage takes down the entire system. | Accepted for v1. Multi-region active-passive (warm standby) is the recommended v2 enhancement before production at significant scale. |

---

*End of Document — HLD v1.0*

---

> **Document Control:** This HLD is derived from SRS v1.0. All architectural decisions herein are subject to review against updated SRS versions. Change history is tracked in the project version control system.