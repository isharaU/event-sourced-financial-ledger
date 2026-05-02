# Agile Delivery Plan — Event-Sourced Financial Ledger (CQRS)

**Document Version:** 1.0  
**Derived From:** HLD v1.0 — Event-Sourced Financial Ledger using CQRS  
**Target Stack:** Java 21 · Spring Boot 3.x · Apache Kafka · PostgreSQL 15+  
**Methodology:** Scrum · 2-week sprints  
**Audience:** Engineering Team, Product Owner, Delivery Lead  

---

## Table of Contents

1. [Epics Overview](#1-epics-overview)
2. [Product Backlog — Epics & User Stories](#2-product-backlog--epics--user-stories)
   - [Epic 1: Project Scaffolding & Infrastructure Foundation](#epic-1-project-scaffolding--infrastructure-foundation)
   - [Epic 2: Event Store & Domain Model](#epic-2-event-store--domain-model)
   - [Epic 3: Account Management](#epic-3-account-management)
   - [Epic 4: Transaction Processing](#epic-4-transaction-processing)
   - [Epic 5: Kafka Event Streaming & Outbox Relay](#epic-5-kafka-event-streaming--outbox-relay)
   - [Epic 6: Read Model Projections](#epic-6-read-model-projections)
   - [Epic 7: Fund Transfer Saga](#epic-7-fund-transfer-saga)
   - [Epic 8: Query API & Read Side](#epic-8-query-api--read-side)
   - [Epic 9: Event Replay Engine & Snapshots](#epic-9-event-replay-engine--snapshots)
   - [Epic 10: Security & Cross-Cutting Concerns](#epic-10-security--cross-cutting-concerns)
   - [Epic 11: Observability & Operational Readiness](#epic-11-observability--operational-readiness)
3. [Sprint Plan](#3-sprint-plan)
4. [MVP Definition](#4-mvp-definition)
5. [Release Strategy](#5-release-strategy)
6. [Risk & Dependency Register](#6-risk--dependency-register)

---

## 1. Epics Overview

| Epic # | Epic Name | Scope Summary | MVP? |
|--------|-----------|---------------|------|
| E1 | Project Scaffolding & Infrastructure Foundation | Repo setup, Docker, DB schemas, Kafka cluster | ✅ Yes |
| E2 | Event Store & Domain Model | Append-only event store, OCC, aggregate hydration | ✅ Yes |
| E3 | Account Management | Create, close, suspend accounts | ✅ Yes |
| E4 | Transaction Processing | Deposits, withdrawals, idempotency | ✅ Yes |
| E5 | Kafka Event Streaming & Outbox Relay | Outbox pattern, Kafka producer, topic config | ✅ Yes |
| E6 | Read Model Projections | Projection handler, account_summary_view, transaction_history_view | ✅ Yes |
| E7 | Fund Transfer Saga | Cross-account transfers, saga state machine, compensation | ✅ Yes |
| E8 | Query API & Read Side | Balance, history, audit endpoints | ✅ Yes |
| E9 | Event Replay Engine & Snapshots | Projection rebuild, snapshot creation, historical queries | ⚠️ Partial |
| E10 | Security & Cross-Cutting Concerns | JWT auth, TLS, mTLS, checksum, PII isolation | ✅ Yes |
| E11 | Observability & Operational Readiness | Tracing, metrics, alerting, SLOs | ⚠️ Partial |

---

## 2. Product Backlog — Epics & User Stories

---

### Epic 1: Project Scaffolding & Infrastructure Foundation

> Establish the development environment, project structure, database schemas, and Kafka cluster so all other epics can proceed.

---

#### US-1.1 — Multi-Module Project Setup

**User Story:**  
As a **backend engineer**, I want a multi-module Maven/Gradle project with separate modules for `command-api`, `query-api`, `domain`, `event-store`, and `projection-handler`, so that each bounded context compiles and deploys independently.

**Acceptance Criteria:**
- Project builds cleanly with `./gradlew build` (or `mvn package`) from root.
- Each module has its own `application.yml`, Dockerfile, and dependency declarations.
- `domain` module has zero Spring Boot dependencies (pure Java).
- Shared DTOs and event contracts live in a dedicated `shared-contracts` module.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** None  

---

#### US-1.2 — Docker Compose Local Environment

**User Story:**  
As a **backend engineer**, I want a `docker-compose.yml` that spins up PostgreSQL (write DB + read DB), Apache Kafka (3 brokers), Zookeeper/KRaft, and Schema Registry locally, so that I can develop and test the full stack without cloud infrastructure.

**Acceptance Criteria:**
- `docker-compose up` starts all services within 60 seconds.
- PostgreSQL write DB and read DB are separate named services with separate port mappings.
- Kafka cluster exposes all 3 broker ports; Schema Registry is reachable at `localhost:8081`.
- A `healthcheck` is defined for each service.
- A seed script creates the required Kafka topics (`account-events`, `transfer-saga-events`, `account-events-dlq`) on startup.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** None  

---

#### US-1.3 — Event Store Database Schema

**User Story:**  
As a **backend engineer**, I want the event store PostgreSQL schema (including `domain_events`, `event_outbox`, `account_snapshots`, `idempotency_keys`, and `saga_state` tables) created via versioned Flyway migrations, so that schema changes are traceable and repeatable across all environments.

**Acceptance Criteria:**
- All five tables are created by Flyway migration `V1__create_event_store_schema.sql`.
- `domain_events` is hash-partitioned on `aggregate_id` with 16 partitions.
- `UNIQUE(aggregate_id, aggregate_version)` constraint exists on `domain_events`.
- `domain_events.checksum` column is `CHAR(64)` NOT NULL.
- Application write DB role has only `INSERT` and `SELECT` on `domain_events` (no UPDATE/DELETE).
- `event_outbox.status` is restricted to `('PENDING', 'PUBLISHED')` via CHECK constraint.
- Migration runs successfully on a fresh PostgreSQL 15 instance.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.2  

---

#### US-1.4 — Read Model Database Schema

**User Story:**  
As a **backend engineer**, I want the read model PostgreSQL schema (including `account_summary_view` and `transaction_history_view` tables) created via versioned Flyway migrations, so that the query side has a stable, indexed read surface from day one.

**Acceptance Criteria:**
- Tables created by migration `V1__create_read_model_schema.sql` on the read DB instance.
- `account_summary_view` has columns: `account_id` (PK), `owner_id`, `balance BIGINT`, `currency`, `status`, `aggregate_version`, `created_at`, `updated_at`.
- `transaction_history_view` has columns: `transaction_id` (PK), `account_id`, `type`, `amount BIGINT`, `currency`, `occurred_at`, `description`, `aggregate_version`.
- B-tree indexes defined on: `transaction_history_view(account_id)`, `(occurred_at)`, `(transaction_id)`.
- Migration runs successfully on a fresh PostgreSQL 15 instance.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-1.2  

---

#### US-1.5 — Kafka Topic Provisioning

**User Story:**  
As a **platform engineer**, I want Kafka topics provisioned with correct partition counts, replication factors, and retention policies via infrastructure-as-code, so that the event bus is correctly configured before any service publishes events.

**Acceptance Criteria:**
- `account-events`: 12 partitions, `replication.factor=3`, `retention.ms=7776000000` (90 days).
- `transfer-saga-events`: 12 partitions, `replication.factor=3`, `retention.ms=2592000000` (30 days).
- `account-events-dlq`: 1 partition, `replication.factor=3`, `retention.ms=31536000000` (365 days).
- `min.insync.replicas=2` set on all topics.
- Provisioning is automated (Terraform, Helm, or startup script).
- Topics verified to exist after running the provisioning script.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-1.2  

---

### Epic 2: Event Store & Domain Model

> Implement the append-only event store, the Account aggregate, Optimistic Concurrency Control, and aggregate hydration from event history.

---

#### US-2.1 — Account Aggregate (Pure Domain Object)

**User Story:**  
As a **backend engineer**, I want the `Account` aggregate implemented as a pure Java domain object (no Spring dependencies), so that business invariants are enforced in isolation and the domain model is fully unit-testable.

**Acceptance Criteria:**
- `Account` class has fields: `accountId`, `ownerId`, `balance (long)`, `currency`, `status (enum: ACTIVE, SUSPENDED, CLOSED)`, `aggregateVersion`.
- `balance` is always a `long` representing minor units (cents). No `double` or `BigDecimal` for monetary arithmetic.
- `apply(DomainEvent event)` method replays each event type to reconstruct state.
- Business invariants enforced via dedicated methods: `deposit()`, `withdraw()`, `close()`, `suspend()`.
- `deposit()` throws `InvalidAmountException` if amount ≤ 0 or > 9,999,999,999.
- `withdraw()` throws `InsufficientFundsException` if resulting balance < 0.
- `withdraw()` and `deposit()` throw `AccountNotActiveException` if status ≠ ACTIVE.
- 100% unit test coverage on aggregate methods.
- Zero Spring Boot / JPA imports in the `domain` module.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.1  

---

#### US-2.2 — Domain Events Definition

**User Story:**  
As a **backend engineer**, I want all domain event classes (`AccountCreated`, `FundsDeposited`, `FundsWithdrawn`, `FundsDebited`, `FundsCredited`, `FundsRefunded`, `AccountClosed`, `AccountSuspended`) defined in the `shared-contracts` module, so that all services share a single, versioned event schema.

**Acceptance Criteria:**
- Each event class has fields: `eventId (UUID)`, `eventType (String)`, `aggregateId (UUID)`, `aggregateVersion (int)`, `occurredAt (Instant)`, `schemaVersion (int, default=1)`, plus type-specific payload fields.
- Events are immutable (all fields `final`; no setters).
- All monetary `amount` fields are `long` (minor units).
- No PII fields (no `ownerName`, `email`, etc.) — only `ownerId (UUID)`.
- Events are serialisable to/from JSON via Jackson (with `@JsonCreator` constructors).
- Shared unit test verifies round-trip JSON serialisation for each event type.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-1.1  

---

#### US-2.3 — Event Store Append (with OCC)

**User Story:**  
As a **backend engineer**, I want an `EventStoreRepository` that appends domain events to `domain_events` atomically, enforcing Optimistic Concurrency Control via the `UNIQUE(aggregate_id, aggregate_version)` constraint, so that concurrent writes to the same aggregate are detected and retried safely.

**Acceptance Criteria:**
- `appendEvents(aggregateId, expectedVersion, List<DomainEvent> events)` inserts all events in a single DB transaction at isolation level `SERIALIZABLE`.
- `aggregate_version` for each inserted event = `expectedVersion + n` (sequential).
- SHA-256 checksum computed from `(aggregate_id + aggregate_version + event_type + payload)` and stored in `checksum` column at write time.
- On `DataIntegrityViolationException` (unique constraint), method throws `OptimisticConcurrencyException`.
- Caller retries up to 3 times with exponential backoff (50ms, 100ms, 200ms) on `OptimisticConcurrencyException`.
- No `UPDATE` or `DELETE` statements are ever executed by this class.
- Integration test verifies: (a) successful append, (b) OCC conflict detection, (c) retry behaviour.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.3, US-2.2  

---

#### US-2.4 — Aggregate Hydration from Event Store

**User Story:**  
As a **backend engineer**, I want an `AggregateLoader` that reconstructs the current state of an `Account` aggregate by loading and replaying its complete event history from the event store, so that every command handler operates on a fully consistent, up-to-date aggregate state.

**Acceptance Criteria:**
- `load(accountId)` queries `domain_events WHERE aggregate_id = ? ORDER BY aggregate_version ASC`.
- Events applied to aggregate via `account.apply(event)` in exact version order.
- Returns `Optional.empty()` if no events exist for the given `accountId`.
- SHA-256 checksum verified on each event during load; throws `TamperedEventException` on mismatch.
- For accounts with ≥ 1 snapshot, loads latest snapshot first, then applies only events with `aggregate_version > snapshot_version` (snapshot-assisted replay — see US-9.2).
- Integration test verifies state consistency after replaying 100 events.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-2.1, US-2.2, US-2.3  

---

#### US-2.5 — Idempotency Key Store

**User Story:**  
As a **backend engineer**, I want an `IdempotencyRepository` that stores client-supplied idempotency keys and their associated response payloads, so that duplicate command submissions are detected and the original response is replayed without re-processing the command.

**Acceptance Criteria:**
- `checkAndStore(idempotencyKey, responsePayload)` inserts a record with `expires_at = now() + 24h` within the same transaction as the domain event append.
- `find(idempotencyKey)` returns the cached `responsePayload` if the key exists and `expires_at > now()`.
- Returns `Optional.empty()` if key is not found or expired.
- A scheduled job runs every 15 minutes to `DELETE FROM idempotency_keys WHERE expires_at < now()`.
- `idempotency_key` column has a `PRIMARY KEY` constraint for O(1) lookups.
- Integration test verifies: duplicate key returns cached payload without writing new events.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-1.3  

---

### Epic 3: Account Management

> Expose Command API endpoints for creating, suspending, and closing accounts, backed by the domain model and event store.

---

#### US-3.1 — Create Account Command

**User Story:**  
As a **financial platform client**, I want to create a new account by sending a `POST /accounts` request, so that I can begin managing funds for an account holder.

**Acceptance Criteria:**
- `POST /accounts` accepts: `{ ownerId, currency, idempotencyKey }`.
- Validates: `ownerId` is non-null UUID, `currency` is `"USD"`, `idempotencyKey` is a valid UUID.
- Returns HTTP 400 with structured error if any validation fails.
- Returns HTTP 401 if JWT is missing or invalid.
- On success: appends `AccountCreated` event; returns HTTP 202 `{ accountId, status: "CREATED", eventId }`.
- On duplicate `idempotencyKey`: returns the original HTTP 202 payload without creating a new event.
- New account always has `initialBalance = 0` and `status = ACTIVE`.
- End-to-end integration test covers: happy path, validation failure, duplicate idempotency key.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-2.3, US-2.4, US-2.5, US-10.1  

---

#### US-3.2 — Close Account Command

**User Story:**  
As a **financial platform client**, I want to close an account by sending a `DELETE /accounts/{accountId}` request, so that the account is deactivated and no further transactions are accepted.

**Acceptance Criteria:**
- `DELETE /accounts/{accountId}` requires valid JWT with `sub` matching account's `ownerId` (or ADMIN role).
- Aggregate validates: account must be `ACTIVE` or `SUSPENDED`; balance must be 0.
- Returns HTTP 422 with reason code if balance ≠ 0 or account is already `CLOSED`.
- On success: appends `AccountClosed` event; returns HTTP 202.
- Subsequent deposit/withdrawal commands on a `CLOSED` account return HTTP 422.
- Unit test on aggregate: closing with non-zero balance throws `AccountClosureNotAllowedException`.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-3.1  

---

#### US-3.3 — Suspend Account Command

**User Story:**  
As a **platform operator**, I want to suspend an account via `POST /accounts/{accountId}/suspend`, so that I can temporarily block all transactions on a flagged account without closing it.

**Acceptance Criteria:**
- Requires JWT with `OPERATOR` or `ADMIN` role claim.
- Aggregate validates: account must be `ACTIVE`.
- Returns HTTP 422 if account is already `SUSPENDED` or `CLOSED`.
- On success: appends `AccountSuspended` event; returns HTTP 202.
- Deposit and withdrawal commands on a `SUSPENDED` account return HTTP 422.
- Suspension is reversible (reactivation as a future story).

**Priority:** Medium  
**Complexity:** S (2 pts)  
**Dependencies:** US-3.1  

---

### Epic 4: Transaction Processing

> Implement deposit and withdrawal commands with full business invariant enforcement, idempotency, and OCC.

---

#### US-4.1 — Deposit Funds Command

**User Story:**  
As a **financial platform client**, I want to deposit funds into an account via `POST /accounts/{accountId}/deposit`, so that the account balance is increased by the specified amount.

**Acceptance Criteria:**
- Accepts: `{ amount (long, cents), currency, description, idempotencyKey }`.
- Validates: `amount > 0`, `amount ≤ 9,999,999,999`, `currency` matches account currency.
- Returns HTTP 400 on schema validation failure.
- Returns HTTP 422 if account is `SUSPENDED` or `CLOSED`.
- Returns HTTP 404 if account does not exist.
- On success: appends `FundsDeposited` event with `transactionId (UUID)`; returns HTTP 202 `{ transactionId, newVersion }`.
- OCC retry on concurrent deposit conflict (up to 3 retries).
- Duplicate `idempotencyKey` returns original HTTP 202 without re-processing.
- Integration test covers: happy path, invalid amount, closed account, duplicate idempotency key, OCC conflict simulation.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-2.3, US-2.4, US-2.5, US-3.1  

---

#### US-4.2 — Withdraw Funds Command

**User Story:**  
As a **financial platform client**, I want to withdraw funds from an account via `POST /accounts/{accountId}/withdraw`, so that funds can be removed from the account balance.

**Acceptance Criteria:**
- Accepts: `{ amount (long), currency, description, idempotencyKey }`.
- Validates: `amount > 0`, `amount ≤ 9,999,999,999`, `currency` matches account currency.
- Returns HTTP 422 with error code `INSUFFICIENT_FUNDS` if `balance - amount < 0`.
- Returns HTTP 422 if account is not `ACTIVE`.
- Balance validation is performed against the event-sourced aggregate (never the read model).
- On success: appends `FundsWithdrawn` event; returns HTTP 202 `{ transactionId, newVersion }`.
- OCC retry on concurrent conflict (up to 3 retries); returns HTTP 409 if all retries exhausted.
- Integration test: concurrent withdrawal race condition resolved correctly by OCC.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-4.1  

---

#### US-4.3 — Monetary Arithmetic Guard

**User Story:**  
As a **backend engineer**, I want a compile-time and test-time enforcement layer that prevents any `double` or `float` or `BigDecimal` type from being used for monetary amounts, so that floating-point precision bugs are structurally impossible.

**Acceptance Criteria:**
- A custom ArchUnit test rule scans all classes in `domain`, `command-api`, and `event-store` modules and fails the build if `double`, `float`, or `Double`, `Float` appear in any field or method signature related to monetary values.
- All event payload `amount` fields are `long`.
- All DB columns storing monetary values are `BIGINT`.
- Test rule is part of the CI pipeline and blocks merging on violation.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-1.1  

---

### Epic 5: Kafka Event Streaming & Outbox Relay

> Implement the Transactional Outbox pattern to bridge the event store and Kafka, ensuring no event is lost between the database commit and Kafka publish.

---

#### US-5.1 — Transactional Outbox Write

**User Story:**  
As a **backend engineer**, I want every domain event append to also write a corresponding record into the `event_outbox` table within the same PostgreSQL transaction, so that no committed event is ever silently dropped before reaching Kafka.

**Acceptance Criteria:**
- `event_outbox` insert occurs in the same `@Transactional` block as `domain_events` insert.
- Outbox record contains: `event_id`, `aggregate_id`, `topic`, `partition_key` (= `aggregate_id`), `payload (JSONB)`, `status = 'PENDING'`, `created_at`.
- If either insert fails, the entire transaction rolls back (both `domain_events` and `event_outbox` are absent).
- Integration test: simulate `domain_events` insert success + `event_outbox` insert failure → verify rollback.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-2.3  

---

#### US-5.2 — Outbox Relay Poller

**User Story:**  
As a **backend engineer**, I want an Outbox Relay background process that polls `event_outbox` for `PENDING` records and publishes them to the correct Kafka topic, so that committed domain events are reliably propagated to all downstream consumers.

**Acceptance Criteria:**
- Relay polls every 500ms using `@Scheduled`.
- Selects up to 100 `PENDING` records per poll, ordered by `created_at ASC`.
- Records are published to Kafka with `aggregate_id` as the message key (ensuring per-account ordering).
- After successful Kafka `send().get()` (synchronous ack), record is updated to `status = 'PUBLISHED'` with `published_at = now()`.
- On Kafka publish failure: record remains `PENDING`; relay retries on next poll cycle with exponential backoff capped at 30 seconds.
- Producer configured: `acks=all`, `enable.idempotence=true`, `retries=Integer.MAX_VALUE`.
- Integration test: relay publishes pending events; Kafka consumer receives them in correct order.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-5.1, US-1.5  

---

#### US-5.3 — Outbox Relay Leader Election

**User Story:**  
As a **platform engineer**, I want the Outbox Relay to use leader election (PostgreSQL advisory lock) so that exactly one relay instance is active at a time across multiple pod replicas, preventing duplicate Kafka publishes during scale-out.

**Acceptance Criteria:**
- On startup, relay attempts to acquire PostgreSQL advisory lock `pg_try_advisory_lock(relay_lock_id)`.
- Only the lock-holder polls and publishes. Other instances poll the lock and stand by.
- On leader pod crash/restart, advisory lock is released (session ends), and a standby acquires it within the next poll cycle (max 500ms).
- Lock acquisition attempt is logged at INFO level.
- Integration test: two relay instances started; verify only one publishes (no duplicates in Kafka).

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-5.2  

---

#### US-5.4 — Kafka Producer Configuration (Exactly-Once Semantics)

**User Story:**  
As a **backend engineer**, I want the Kafka producer configured for idempotent delivery with `acks=all`, so that producer retries never result in duplicate messages on the `account-events` topic.

**Acceptance Criteria:**
- Spring Kafka producer bean configured: `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`, `retries=Integer.MAX_VALUE`.
- Producer uses a `transactional.id` for transactional producers (if transactional Kafka is adopted).
- Configuration lives in `application.yml` under `spring.kafka.producer` and is environment-overridable.
- A unit test verifies the producer bean is initialised with the correct config properties.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-1.5  

---

### Epic 6: Read Model Projections

> Build Kafka consumers that maintain denormalised read model tables from the event stream.

---

#### US-6.1 — Projection Handler: `AccountCreated`

**User Story:**  
As a **backend engineer**, I want a Kafka consumer that processes `AccountCreated` events and inserts a row into `account_summary_view`, so that newly created accounts are immediately queryable.

**Acceptance Criteria:**
- `@KafkaListener` on `account-events` topic, consumer group `projection-handler`.
- On `AccountCreated`: `INSERT INTO account_summary_view (account_id, owner_id, balance=0, currency, status='ACTIVE', aggregate_version=1, created_at)`.
- Insert is idempotent: `ON CONFLICT (account_id) DO NOTHING`.
- Kafka offset committed manually only after successful DB insert.
- On DB failure: no offset commit; consumer retries on next poll.
- After 3 failures: event published to `account-events-dlq`; offset committed; processing continues.
- Integration test: publish `AccountCreated` to Kafka; verify `account_summary_view` row within 2 seconds.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.4, US-5.2, US-2.2  

---

#### US-6.2 — Projection Handler: `FundsDeposited` & `FundsWithdrawn`

**User Story:**  
As a **backend engineer**, I want the Projection Handler to process `FundsDeposited` and `FundsWithdrawn` events and update `account_summary_view` balance and insert rows into `transaction_history_view`, so that the read model reflects the current balance and transaction history.

**Acceptance Criteria:**
- On `FundsDeposited`: `UPDATE account_summary_view SET balance = balance + amount, aggregate_version = new_version WHERE account_id = ? AND aggregate_version < new_version`.
- On `FundsWithdrawn`: `UPDATE account_summary_view SET balance = balance - amount, aggregate_version = new_version WHERE account_id = ? AND aggregate_version < new_version`.
- Both events trigger: `INSERT INTO transaction_history_view (transaction_id, account_id, type, amount, currency, occurred_at, description, aggregate_version)`.
- Duplicate event detection: if incoming `aggregateVersion ≤ currentVersion` in `account_summary_view`, event is discarded (idempotent).
- Integration test: deposit → verify balance updated in read model; send same event twice → verify balance not double-counted.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-6.1  

---

#### US-6.3 — Projection Handler: `AccountClosed` & `AccountSuspended`

**User Story:**  
As a **backend engineer**, I want the Projection Handler to process `AccountClosed` and `AccountSuspended` events and update the `status` field in `account_summary_view`, so that the account's lifecycle state is accurately reflected in the read model.

**Acceptance Criteria:**
- On `AccountClosed`: `UPDATE account_summary_view SET status = 'CLOSED', aggregate_version = new_version WHERE account_id = ?`.
- On `AccountSuspended`: `UPDATE account_summary_view SET status = 'SUSPENDED', aggregate_version = new_version WHERE account_id = ?`.
- Duplicate detection via `aggregate_version` guard (same as US-6.2).
- Integration test: close an account → verify `status = 'CLOSED'` in read model.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-6.2  

---

#### US-6.4 — Version Gap Detection & Gap-Fill

**User Story:**  
As a **backend engineer**, I want the Projection Handler to detect version gaps in consumed events (e.g., version 5 arrives after version 3, skipping 4) and back-fill the missing events directly from the event store, so that the read model is never silently inconsistent due to Kafka delivery anomalies.

**Acceptance Criteria:**
- Handler compares incoming `aggregateVersion` to `account_summary_view.aggregate_version` for the same account.
- If `incoming_version > current_version + 1`: gap detected; handler queries event store for events between `current_version + 1` and `incoming_version - 1` and applies them first.
- Gap-fill applies events from event store in version order before applying the current event.
- Gap detection logged at WARN level with `aggregateId`, `expected`, and `received` versions.
- A gap-fill counter metric is exposed via Micrometer.
- Integration test: manually publish event at version 5 when read model is at version 3; verify gap-fill fetches version 4 and both are applied correctly.

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-6.2, US-2.4  

---

#### US-6.5 — Dead Letter Queue (DLQ) Handler

**User Story:**  
As a **platform operator**, I want unprocessable events routed to the `account-events-dlq` topic after 3 failed processing attempts, so that projection handler failures on bad messages do not block the entire consumer group.

**Acceptance Criteria:**
- Projection handler tracks per-message failure count in memory (reset on new poll).
- After 3 consecutive failures on the same event: publish to `account-events-dlq` with headers: `original-topic`, `failure-reason`, `failed-at`.
- Commit Kafka offset after DLQ publish; continue processing next message.
- A separate `DlqHandler` consumer group reads from `account-events-dlq` and logs structured JSON alerts.
- DLQ alert fires on Prometheus metric `dlq_events_total > 0`.
- Integration test: publish an event that triggers a DB constraint violation 3 times; verify it lands in DLQ.

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-6.1, US-1.5  

---

### Epic 7: Fund Transfer Saga

> Implement the two-phase Transfer Saga with debit, credit, and compensating transaction handling.

---

#### US-7.1 — Transfer Saga State Machine Definition

**User Story:**  
As a **backend engineer**, I want the Transfer Saga state machine defined with all valid states and transitions, so that the saga's lifecycle is explicit, testable, and durable.

**Acceptance Criteria:**
- States: `DEBIT_INITIATED`, `CREDIT_INITIATED`, `COMPLETED`, `COMPENSATING`, `FAILED`.
- Valid transitions: `DEBIT_INITIATED → CREDIT_INITIATED → COMPLETED` (happy path); `DEBIT_INITIATED → FAILED` (debit fails); `CREDIT_INITIATED → COMPENSATING → FAILED` (credit fails).
- Invalid state transitions throw `IllegalSagaTransitionException`.
- State machine is a pure Java class with no framework coupling.
- 100% unit test coverage on all valid and invalid transitions.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-1.1  

---

#### US-7.2 — `saga_state` Persistence

**User Story:**  
As a **backend engineer**, I want each saga step transition persisted to the `saga_state` table before executing the next step, so that a crashed saga coordinator can resume all in-flight sagas from the last committed state.

**Acceptance Criteria:**
- `saga_state` row contains: `saga_id (UUID)`, `transfer_id`, `source_account_id`, `destination_account_id`, `amount`, `currency`, `step (enum)`, `status`, `version (int)`, `created_at`, `updated_at`.
- OCC via `version` column: `UPDATE saga_state SET step=?, version=version+1 WHERE saga_id=? AND version=?`; throws `SagaConcurrencyException` on 0 rows updated.
- Saga state write precedes any command dispatch (write-before-act pattern).
- On application restart: `SELECT * FROM saga_state WHERE status='IN_PROGRESS'` resumes all incomplete sagas.
- Integration test: simulate crash after `DEBIT_INITIATED` persisted; verify saga resumes and completes correctly.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.3, US-7.1  

---

#### US-7.3 — Initiate Fund Transfer Command

**User Story:**  
As a **financial platform client**, I want to initiate a fund transfer via `POST /transfers`, so that funds are atomically moved from a source account to a destination account.

**Acceptance Criteria:**
- Accepts: `{ sourceAccountId, destinationAccountId, amount, currency, idempotencyKey }`.
- Validates: source ≠ destination, amount > 0, both accounts exist.
- Returns HTTP 400 on validation failure.
- Creates saga instance; persists `saga_state` with `step=DEBIT_INITIATED`.
- Dispatches `DebitAccount` sub-command to source account Command Handler.
- Returns HTTP 202 `{ transferId, status: "IN_PROGRESS" }` immediately (non-blocking).
- Duplicate `idempotencyKey` returns original HTTP 202 without creating a new saga.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-7.2, US-4.2, US-2.5  

---

#### US-7.4 — Saga Credit Step

**User Story:**  
As a **backend engineer**, I want the Transfer Saga to automatically dispatch a `CreditAccount` command to the destination account upon consuming a `FundsDebited` event from Kafka, so that the second phase of the transfer completes without manual intervention.

**Acceptance Criteria:**
- Saga consumer listens on `account-events` for `FundsDebited` events correlated by `transferId`.
- On receipt: updates saga state to `CREDIT_INITIATED`; dispatches `CreditAccount` sub-command.
- `CreditAccount` appends `FundsCredited` event to destination account; publishes to Kafka.
- On receipt of `FundsCredited`: saga state updated to `COMPLETED`; `TransferCompleted` event emitted.
- Saga consumer uses manual Kafka offset commit; only commits after saga state is persisted.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-7.3  

---

#### US-7.5 — Saga Compensation (Rollback)

**User Story:**  
As a **backend engineer**, I want the Transfer Saga to automatically issue a `RefundFunds` compensating command if the credit step fails, so that the source account balance is restored and the transfer is marked as failed.

**Acceptance Criteria:**
- Credit step failure (destination `CLOSED`, `SUSPENDED`, or other error): saga transitions to `COMPENSATING`.
- Dispatches `RefundFunds` command to source account (same `transferId` as correlation key).
- Source account appends `FundsRefunded` event; `account_summary_view` balance restored.
- After compensation: saga transitions to `FAILED`; `TransferFailed` event emitted to Kafka.
- `FundsRefunded` uses `transferId` as correlation; idempotent — duplicate `RefundFunds` on retry produces no second `FundsRefunded` event (OCC guard).
- Projection Handler processes `FundsRefunded` → updates `account_summary_view` balance and inserts `REFUND` row in `transaction_history_view`.
- Integration test: simulate closed destination account; verify source balance is fully restored.

**Priority:** High  
**Complexity:** L (8 pts)  
**Dependencies:** US-7.4  

---

#### US-7.6 — Projection Handler: Transfer Events

**User Story:**  
As a **backend engineer**, I want the Projection Handler to process `FundsDebited`, `FundsCredited`, and `FundsRefunded` events and update both accounts' read models, so that transfer activity is accurately reflected in the transaction history of both accounts.

**Acceptance Criteria:**
- `FundsDebited`: update `account_summary_view` balance (decrease source), insert `TRANSFER_DEBIT` row in `transaction_history_view`.
- `FundsCredited`: update `account_summary_view` balance (increase destination), insert `TRANSFER_CREDIT` row.
- `FundsRefunded`: update `account_summary_view` balance (increase source), insert `REFUND` row.
- All three are idempotent (duplicate-event guard via `aggregate_version`).
- Each transaction history row includes `transferId` as a correlation reference.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-6.2, US-7.4, US-7.5  

---

### Epic 8: Query API & Read Side

> Implement the read-only Query API endpoints served from the denormalised read model.

---

#### US-8.1 — Get Account Balance

**User Story:**  
As a **financial platform client**, I want to retrieve the current balance of my account via `GET /accounts/{accountId}/balance`, so that I can see my up-to-date available funds.

**Acceptance Criteria:**
- Returns HTTP 200 `{ accountId, balance (long), currency, status, asOfVersion }` from `account_summary_view`.
- Returns HTTP 404 if account does not exist in read model.
- Returns HTTP 403 if JWT `sub` ≠ `ownerId` (and caller lacks OPERATOR/ADMIN role).
- Query routed to read replica (never event store primary).
- p99 latency target: < 50ms under 500 concurrent requests.
- Integration test covers: happy path, unauthorised access, non-existent account.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-6.1, US-10.1  

---

#### US-8.2 — Get Transaction History

**User Story:**  
As a **financial platform client**, I want to retrieve a paginated list of my transactions via `GET /accounts/{accountId}/transactions`, so that I can review my account activity.

**Acceptance Criteria:**
- Supports query parameters: `from (ISO date)`, `to (ISO date)`, `page (cursor-based)`, `limit (max 100, default 20)`.
- Returns: `{ transactions: [...], nextCursor, totalCount }`.
- Each transaction includes: `transactionId`, `type`, `amount`, `currency`, `description`, `occurredAt`, `transferId (nullable)`.
- Cursor-based pagination uses `(occurred_at, transaction_id)` as cursor — no `OFFSET`.
- Returns HTTP 400 if `from > to` or `limit > 100`.
- p99 latency: < 100ms for up to 1,000 results.
- Integration test: create 50 transactions; verify cursor pagination returns all pages correctly.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-6.2  

---

#### US-8.3 — Get Single Transaction

**User Story:**  
As a **financial platform client**, I want to retrieve the details of a specific transaction via `GET /transactions/{transactionId}`, so that I can verify the status of a particular transaction.

**Acceptance Criteria:**
- Returns HTTP 200 with full transaction details from `transaction_history_view`.
- Returns HTTP 404 if `transactionId` not found.
- Returns HTTP 403 if caller's JWT `sub` does not match the account's `ownerId`.
- Response includes: all fields from US-8.2 plus `accountId`.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-8.2  

---

#### US-8.4 — Read-After-Write Consistency (`asOfVersion` Polling)

**User Story:**  
As a **financial platform client**, I want to retrieve the account state at a specific event version via `GET /accounts/{accountId}/balance?asOfVersion={n}`, so that I can confirm my recent command has been reflected in the read model.

**Acceptance Criteria:**
- If `account_summary_view.aggregate_version >= requested asOfVersion`: return immediately with HTTP 200.
- If read model is behind: poll every 200ms for up to 5 seconds until caught up.
- If still behind after 5 seconds: return HTTP 202 `{ message: "read model catching up", retryAfterMs: 500, currentVersion }`.
- Polling is implemented within the Query API (no client-side polling required).
- Integration test: insert event; immediately query with `asOfVersion`; verify eventual HTTP 200.

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-8.1, US-6.2  

---

#### US-8.5 — Admin Audit Log Query

**User Story:**  
As a **platform operator**, I want to query the full audit log of events for any account via `GET /admin/audit?accountId={id}&from={date}&to={date}`, so that I can investigate suspicious activity or resolve disputes.

**Acceptance Criteria:**
- Requires JWT with `OPERATOR` or `ADMIN` role claim; returns HTTP 403 otherwise.
- Returns paginated list of `transaction_history_view` rows for the specified account and date range.
- Supports `limit` (max 500) and cursor-based pagination.
- Response includes raw event metadata: `eventId`, `eventType`, `aggregateVersion`, `occurredAt`.
- p99 latency: < 200ms for up to 500 results.

**Priority:** Medium  
**Complexity:** S (3 pts)  
**Dependencies:** US-8.2, US-10.1  

---

### Epic 9: Event Replay Engine & Snapshots

> Implement snapshot creation for replay performance, and the projection rebuild mechanism for operational recovery.

---

#### US-9.1 — Projection Rebuild (Shadow Table Swap)

**User Story:**  
As a **platform operator**, I want to trigger a full read model rebuild via `POST /admin/projections/{name}/rebuild`, so that I can recover from read model corruption or populate a newly added projection without downtime.

**Acceptance Criteria:**
- Requires JWT with `OPERATOR` or `ADMIN` role.
- Rebuild streams all `domain_events` from the event store in batches of 1,000.
- Writes to shadow table (`account_summary_view_shadow`, `transaction_history_view_shadow`).
- On completion: atomically renames shadow table to live table within a single `BEGIN/COMMIT` block.
- Query API continues serving from the current live table throughout the rebuild.
- Response: HTTP 200 `{ eventsProcessed, durationMs, completedAt }`.
- If rebuild is already in progress: returns HTTP 409.
- Integration test: corrupt the read model; trigger rebuild; verify read model is consistent post-swap.

**Priority:** High  
**Complexity:** L (8 pts)  
**Dependencies:** US-6.1, US-2.4  

---

#### US-9.2 — Snapshot Creation

**User Story:**  
As a **backend engineer**, I want the system to automatically create account aggregate snapshots every 50 events, so that aggregate hydration is bounded in time regardless of account age.

**Acceptance Criteria:**
- After each successful event append, check if `(aggregateVersion % 50 == 0)`.
- If true: asynchronously serialise current aggregate state to JSON and `INSERT INTO account_snapshots (aggregate_id, snapshot_version, state JSONB, created_at)`.
- Snapshot creation is asynchronous (does not block the command path; uses `@Async` or a background queue).
- `AggregateLoader` uses the nearest snapshot ≤ target version, then replays only subsequent events.
- Snapshot threshold is configurable via `app.snapshot.threshold` in `application.yml`.
- Integration test: create 55 events for one account; verify snapshot exists at version 50; verify reload uses snapshot and applies only 5 additional events.

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-2.4, US-2.3  

---

#### US-9.3 — Historical State Query (`atVersion`)

**User Story:**  
As a **financial platform client**, I want to query an account's state at a specific historical version via `GET /accounts/{accountId}/history?atVersion={n}`, so that I can reconstruct the account balance at any point in its history.

**Acceptance Criteria:**
- Loads nearest snapshot at version ≤ `atVersion` (or starts from scratch if none).
- Replays events from `snapshot_version + 1` to `atVersion` on the snapshot state.
- Returns: `{ balance, status, currency, version, computedAt }`.
- Returns HTTP 400 if `atVersion < 1` or `atVersion > current aggregate version`.
- Response computed in < 500ms for accounts with ≤ 500 events (snapshot coverage ensured by US-9.2).
- Integration test: create 100 events; query at version 42; verify correct balance returned.

**Priority:** Medium  
**Complexity:** M (5 pts)  
**Dependencies:** US-9.2, US-8.1  

---

### Epic 10: Security & Cross-Cutting Concerns

> Implement JWT authentication, authorisation, TLS enforcement, event checksum validation, and PII isolation.

---

#### US-10.1 — JWT Authentication & Validation

**User Story:**  
As a **platform security engineer**, I want every Command API and Query API endpoint to require a valid JWT Bearer token, so that only authenticated callers can interact with the financial ledger.

**Acceptance Criteria:**
- Spring Security OAuth2 Resource Server configured on both Command API and Query API.
- Validates: RS256/ES256 signature, `exp` claim, `iss` claim, `aud` claim.
- JWKS keys fetched from IdP endpoint; cached with a 15-minute TTL.
- Requests without a valid JWT return HTTP 401 with a structured JSON error.
- Expired tokens return HTTP 401; wrong audience returns HTTP 401.
- Configuration: IdP JWKS URL and expected `iss`/`aud` values are environment-injectable.
- Unit test: mock JWT with expired `exp`; verify 401 returned.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-1.1  

---

#### US-10.2 — Role-Based Authorisation

**User Story:**  
As a **platform security engineer**, I want account access restricted so that account holders can only access their own accounts, while `OPERATOR` and `ADMIN` roles have broader cross-account read access, so that data is isolated between users.

**Acceptance Criteria:**
- Account holder access: JWT `sub` must match `account.ownerId`; otherwise HTTP 403.
- `OPERATOR` role: read access to all accounts; access to audit endpoints; no write access.
- `ADMIN` role: full access including rebuild and suspension endpoints.
- Role claims extracted from JWT `roles` array claim.
- Authorisation logic implemented as Spring Security method-level annotations (`@PreAuthorize`).
- Integration test: account holder A cannot read account holder B's balance.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-10.1  

---

#### US-10.3 — Event Checksum Verification

**User Story:**  
As a **backend engineer**, I want every domain event's integrity verified by SHA-256 checksum on read, so that tampered or corrupted events are detected before they affect aggregate state.

**Acceptance Criteria:**
- At write time: compute SHA-256 of `(aggregate_id + aggregate_version + event_type + payload_json)`; store in `domain_events.checksum`.
- At read time (during aggregate hydration): recompute checksum; compare to stored value.
- Mismatch: log ERROR with `eventId` and `aggregateId`; throw `TamperedEventException`; exclude event from hydration.
- Tampered event detection metric incremented via Micrometer: `events_checksum_failures_total`.
- Unit test: modify event payload after writing; verify `TamperedEventException` is thrown on reload.

**Priority:** High  
**Complexity:** S (3 pts)  
**Dependencies:** US-2.3, US-2.4  

---

#### US-10.4 — TLS Enforcement

**User Story:**  
As a **platform security engineer**, I want all external HTTP traffic to require TLS 1.2 minimum, so that data in transit cannot be intercepted.

**Acceptance Criteria:**
- Ingress controller / load balancer configured to reject all plaintext HTTP connections.
- TLS 1.2 minimum; TLS 1.0 and 1.1 disabled.
- TLS 1.3 preferred (configured as the default where supported).
- Spring Boot `server.ssl.*` properties or Kubernetes Ingress TLS block configured.
- Verified by attempting a plaintext HTTP request → 301 redirect or connection refused.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-1.2  

---

#### US-10.5 — PII Isolation in Events

**User Story:**  
As a **platform security engineer**, I want all domain events, Kafka messages, and structured logs to contain only opaque `ownerId (UUID)` references and never include names, emails, or other PII, so that the event store and audit log are compliant with data minimisation principles.

**Acceptance Criteria:**
- All domain event classes are reviewed; no `ownerName`, `email`, `phoneNumber`, or similar PII fields exist.
- Structured log output (JSON) for command processing does not include PII.
- An ArchUnit test scans all event classes for field names matching PII patterns (`name`, `email`, `phone`, `address`); fails build on detection.
- Code review checklist updated to include PII field prohibition for events.

**Priority:** High  
**Complexity:** S (2 pts)  
**Dependencies:** US-2.2  

---

### Epic 11: Observability & Operational Readiness

> Implement distributed tracing, metrics, structured logging, and alerting.

---

#### US-11.1 — Distributed Tracing (OpenTelemetry)

**User Story:**  
As a **platform engineer**, I want every request traced end-to-end using OpenTelemetry with trace context propagated across Command API, Command Handler, Kafka, and Projection Handler, so that I can diagnose latency issues and failures across service boundaries.

**Acceptance Criteria:**
- OpenTelemetry Java agent configured on all Spring Boot services.
- `traceId` and `spanId` injected into all structured log lines (MDC).
- Trace context propagated in Kafka message headers (`traceparent`).
- Traces exported to Jaeger or Tempo.
- End-to-end trace visible for a deposit flow: Command API span → Command Handler span → Outbox Relay span → Projection Handler span.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-4.1, US-5.2, US-6.2  

---

#### US-11.2 — Metrics & Alerting (Prometheus/Grafana)

**User Story:**  
As a **platform engineer**, I want key business and system metrics exposed via Micrometer and scraped by Prometheus, so that I can monitor system health and set up alerts on critical thresholds.

**Acceptance Criteria:**
- Metrics exposed: `commands_processed_total` (by type, status), `events_appended_total`, `occ_retries_total`, `outbox_relay_lag_ms`, `kafka_consumer_lag` (by topic/partition), `projection_processing_latency_ms`, `dlq_events_total`, `saga_completions_total` (by status), `balance_query_latency_p99`.
- All metrics have `service`, `environment`, and `version` labels.
- Grafana dashboard provisioned with: command throughput, OCC retry rate, consumer lag, DLQ rate, p99 query latency.
- Alerts defined: consumer lag > 5s, DLQ events > 0, OCC retry rate > 10/min, p99 balance query > 100ms.

**Priority:** High  
**Complexity:** M (5 pts)  
**Dependencies:** US-11.1  

---

#### US-11.3 — Structured JSON Logging

**User Story:**  
As a **platform engineer**, I want all services to emit structured JSON logs with consistent fields, so that logs are machine-parseable and can be ingested by ELK or Loki for centralised log analysis.

**Acceptance Criteria:**
- Logback configured with JSON encoder (Logstash JSON encoder or equivalent).
- Every log line includes: `timestamp`, `level`, `service`, `traceId`, `spanId`, `accountId (if applicable)`, `message`.
- No PII in any log line.
- Log level configurable per package via environment variable without restart.
- Verified by running a deposit command and inspecting log output format in Docker Compose.

**Priority:** Medium  
**Complexity:** S (2 pts)  
**Dependencies:** US-11.1  

---

#### US-11.4 — Health & Readiness Endpoints

**User Story:**  
As a **platform engineer**, I want all services to expose Spring Boot Actuator `/health/liveness` and `/health/readiness` endpoints, so that Kubernetes can correctly manage pod lifecycle and traffic routing.

**Acceptance Criteria:**
- `liveness` probe: returns 200 if JVM is alive; 503 if in a fatal error state.
- `readiness` probe: returns 200 only if DB connection pool is healthy and Kafka consumer group is assigned; 503 otherwise.
- Probes configured in Kubernetes `Deployment` manifests with appropriate `initialDelaySeconds` and `periodSeconds`.
- Actuator endpoints secured: `/health/**` accessible without auth; all other actuator endpoints require ADMIN role.

**Priority:** Medium  
**Complexity:** S (2 pts)  
**Dependencies:** US-1.1  

---

## 3. Sprint Plan

> **Assumptions:** 2-week sprints · Team of 4 backend engineers · 1 sprint = ~50 story points capacity · 7 sprints total

---

### Sprint 1 — Foundation & Event Store Core

**Sprint Goal:** Establish the full development environment and implement the immutable event store with OCC and aggregate hydration. The team can append and replay domain events by end of sprint.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points |
|----------|-------|--------|
| US-1.1 | Multi-Module Project Setup | 5 |
| US-1.2 | Docker Compose Local Environment | 3 |
| US-1.3 | Event Store Database Schema | 5 |
| US-1.4 | Read Model Database Schema | 3 |
| US-1.5 | Kafka Topic Provisioning | 3 |
| US-2.1 | Account Aggregate (Pure Domain Object) | 5 |
| US-2.2 | Domain Events Definition | 3 |
| US-2.3 | Event Store Append (with OCC) | 5 |
| US-2.4 | Aggregate Hydration from Event Store | 5 |
| US-4.3 | Monetary Arithmetic Guard (ArchUnit) | 2 |
| US-10.5 | PII Isolation in Events | 2 |
| US-11.4 | Health & Readiness Endpoints | 2 |
| **Total** | | **43 pts** |

**Deliverables:**
- Fully functional local environment via `docker-compose up`.
- Event store schema deployed via Flyway.
- Account aggregate with all business invariants, 100% unit-tested.
- `EventStoreRepository.appendEvents()` with OCC retries, integration-tested.
- `AggregateLoader.load()` with checksum verification, integration-tested.
- All domain event classes defined and round-trip JSON tested.
- CI pipeline running with ArchUnit and unit tests.

**Dependencies Resolved:** None (sprint 0 activities)

---

### Sprint 2 — Idempotency, Account Commands & Kafka Outbox

**Sprint Goal:** Deliver the Create Account, Deposit, and Withdraw commands end-to-end (command → event store → outbox). Clients can submit commands with idempotency guarantees.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points |
|----------|-------|--------|
| US-2.5 | Idempotency Key Store | 3 |
| US-10.1 | JWT Authentication & Validation | 5 |
| US-10.2 | Role-Based Authorisation | 3 |
| US-10.3 | Event Checksum Verification | 3 |
| US-10.4 | TLS Enforcement | 2 |
| US-3.1 | Create Account Command | 5 |
| US-3.2 | Close Account Command | 3 |
| US-3.3 | Suspend Account Command | 2 |
| US-4.1 | Deposit Funds Command | 5 |
| US-4.2 | Withdraw Funds Command | 5 |
| US-5.1 | Transactional Outbox Write | 3 |
| US-5.4 | Kafka Producer Configuration | 2 |
| **Total** | | **41 pts** |

**Deliverables:**
- JWT-secured Command API live.
- `POST /accounts`, deposit, withdraw, close, suspend all functional with idempotency.
- Outbox records written atomically with domain events — verified by integration test.
- Kafka producer configured for exactly-once semantics.
- Full command-side integration test suite passing.

**Dependencies Resolved:** US-1.x (Sprint 1), US-2.x (Sprint 1)

---

### Sprint 3 — Kafka Relay & Read Model Projections

**Sprint Goal:** Close the event loop — events flow from the event store to Kafka to the read model. Account balance and transaction history are queryable via the read model by end of sprint.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points |
|----------|-------|--------|
| US-5.2 | Outbox Relay Poller | 5 |
| US-5.3 | Outbox Relay Leader Election | 5 |
| US-6.1 | Projection Handler: `AccountCreated` | 5 |
| US-6.2 | Projection Handler: Deposit & Withdrawal | 5 |
| US-6.3 | Projection Handler: Close & Suspend | 2 |
| US-6.5 | DLQ Handler | 5 |
| US-8.1 | Get Account Balance | 3 |
| US-8.2 | Get Transaction History | 5 |
| US-8.3 | Get Single Transaction | 2 |
| US-11.3 | Structured JSON Logging | 2 |
| **Total** | | **39 pts** |

**Deliverables:**
- Full event pipeline live: Command → Event Store → Outbox → Kafka → Projection Handler → Read Model.
- `GET /accounts/{id}/balance` and `GET /accounts/{id}/transactions` returning correct data.
- DLQ routing verified for bad messages.
- Structured JSON logs in all services.
- End-to-end integration test: create account → deposit → verify balance in read model.

**Dependencies Resolved:** US-5.1, US-5.4 (Sprint 2)

---

### Sprint 4 — Fund Transfer Saga

**Sprint Goal:** Implement end-to-end fund transfers including the saga state machine, happy path, and compensation. Transfers between accounts are fully functional with rollback on failure.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points |
|----------|-------|--------|
| US-7.1 | Transfer Saga State Machine Definition | 3 |
| US-7.2 | `saga_state` Persistence | 5 |
| US-7.3 | Initiate Fund Transfer Command | 5 |
| US-7.4 | Saga Credit Step | 5 |
| US-7.5 | Saga Compensation (Rollback) | 8 |
| US-7.6 | Projection Handler: Transfer Events | 5 |
| US-6.4 | Version Gap Detection & Gap-Fill | 5 |
| US-8.5 | Admin Audit Log Query | 3 |
| **Total** | | **39 pts** |

**Deliverables:**
- `POST /transfers` end-to-end: debit source → credit destination → read models updated.
- Compensation path: closed destination → source balance restored → `TransferFailed` event.
- Saga resumes correctly after simulated coordinator restart.
- Both accounts' transaction histories reflect transfer events.
- Version gap detection and gap-fill verified by integration test.

**Dependencies Resolved:** US-6.x (Sprint 3), US-4.x (Sprint 2)

---

### Sprint 5 — Replay Engine, Snapshots & Read-After-Write

**Sprint Goal:** Add operational resilience capabilities — projection rebuild for recovery, snapshots for replay performance, and `asOfVersion` for strong read-after-write consistency.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points |
|----------|-------|--------|
| US-9.1 | Projection Rebuild (Shadow Table Swap) | 8 |
| US-9.2 | Snapshot Creation | 5 |
| US-9.3 | Historical State Query (`atVersion`) | 5 |
| US-8.4 | Read-After-Write (`asOfVersion` Polling) | 5 |
| US-11.1 | Distributed Tracing (OpenTelemetry) | 5 |
| US-11.2 | Metrics & Alerting (Prometheus/Grafana) | 5 |
| **Total** | | **33 pts** |

**Deliverables:**
- Admin-triggered projection rebuild with zero downtime (shadow table swap verified).
- Snapshots created automatically every 50 events; hydration uses snapshot.
- `GET /accounts/{id}/history?atVersion=N` returns correct historical state.
- `asOfVersion` polling prevents stale reads after write.
- Distributed traces visible in Jaeger for full deposit flow.
- Grafana dashboard live with key metrics and alerts configured.

**Dependencies Resolved:** US-6.x (Sprint 3), US-9.2 (Sprint 5 internal)

---

### Sprint 6 — Hardening, Performance & Production Readiness

**Sprint Goal:** Harden the system for production — load testing, OCC tuning, security review, documentation, and deployment manifests. System is production-ready at sprint end.

**Duration:** 2 weeks  
**Capacity:** 50 pts

| Story ID | Title | Points | Notes |
|----------|-------|--------|-------|
| **Performance Testing** | Load test deposit/withdraw at 5,000 events/sec | 8 | k6 or Gatling |
| **OCC Retry Tuning** | Validate 3-retry + backoff under high concurrency | 5 | Chaos test |
| **Kafka Consumer Lag SLO** | Alert if consumer lag > 5s; verify under burst load | 5 | Load test + alert tuning |
| **Security Penetration Review** | JWT edge cases, role bypass, checksum tampering | 5 | Security test suite |
| **Kubernetes Deployment Manifests** | All service `Deployment`, `Service`, `ConfigMap`, `HPA` | 5 | K8s YAML |
| **API Documentation** | OpenAPI 3.0 spec for Command API and Query API | 5 | Springdoc |
| **Runbook: Incident Playbook** | Projection lag, DLQ spike, OCC storm, saga stuck | 5 | Markdown runbook |
| **Saga Restart Chaos Test** | Kill coordinator mid-transfer; verify resume | 5 | Integration test |
| **Total** | | **43 pts** | |

**Deliverables:**
- System passes load test at ≥ 5,000 events/sec sustained.
- p99 balance query latency < 50ms verified under load.
- Kubernetes manifests with HPA for Command API and Query API.
- OpenAPI spec published.
- Incident runbook covering all known failure modes.
- Full production readiness checklist signed off.

**Dependencies Resolved:** All previous sprints

---

### Sprint Summary Table

| Sprint | Goal Summary | Key Deliverables | Story Points |
|--------|-------------|-----------------|--------------|
| Sprint 1 | Foundation & Event Store | Env, schema, aggregate, OCC, hydration | 43 |
| Sprint 2 | Commands & Outbox | JWT, CRUD commands, idempotency, outbox | 41 |
| Sprint 3 | Kafka & Projections | Relay, projection handlers, Query API | 39 |
| Sprint 4 | Transfer Saga | Full saga: happy path + compensation | 39 |
| Sprint 5 | Replay & Observability | Rebuild, snapshots, tracing, metrics | 33 |
| Sprint 6 | Hardening & Production | Load test, K8s, docs, runbooks | 43 |
| **Total** | | | **238 pts** |

---

## 4. MVP Definition

### Included in MVP (Sprints 1–4)

The MVP delivers a production-deployable financial ledger capable of end-to-end account management, transaction processing, and fund transfers with full auditability.

| # | Capability | Justification |
|---|-----------|---------------|
| 1 | Account creation, closure, suspension | Core lifecycle management |
| 2 | Deposit and withdrawal with idempotency | Primary transaction types |
| 3 | Fund transfers (happy path + compensation) | Core financial feature |
| 4 | Event store with OCC | Non-negotiable correctness guarantee |
| 5 | Kafka event pipeline + Outbox pattern | Required for eventual consistency |
| 6 | Read model projections (balance + history) | Required for any usable Query API |
| 7 | Query API: balance + transaction history | Minimum read surface |
| 8 | JWT authentication + role-based authorisation | Security baseline |
| 9 | Event checksum integrity verification | Financial audit requirement |
| 10 | PII isolation in events | Compliance requirement |

### Excluded from MVP (Deferred to Post-MVP)

| # | Feature | Why Deferred |
|---|---------|-------------|
| 1 | Projection rebuild (shadow table swap) | Operational feature; MVP can use downtime rebuild |
| 2 | Snapshot creation | Performance optimisation only; system correct without it |
| 3 | `atVersion` historical queries | Advanced client feature; not required for basic operation |
| 4 | `asOfVersion` read-after-write polling | Nice-to-have; most clients tolerate < 500ms lag |
| 5 | Full Prometheus/Grafana dashboards | Infrastructure; alerts sufficient for MVP |
| 6 | Distributed tracing (OpenTelemetry) | Observability enhancement; structured logs sufficient for MVP |
| 7 | Kubernetes HPA & manifests | DevOps concern; manual scaling acceptable for early launch |
| 8 | Admin audit log query | Operator feature; basic transaction history covers compliance |

---

## 5. Release Strategy

### v1.0 Release (MVP + Sprint 5 + Sprint 6)

**Scope:** All 11 epics, all 6 sprints.

**Included Capabilities:**
- Full account lifecycle management.
- Deposits, withdrawals, transfers with saga compensation.
- Immutable, checksummed event store with OCC.
- Kafka event pipeline with exactly-once delivery.
- Read model projections with projection rebuild (zero downtime).
- Snapshot-assisted aggregate hydration.
- JWT authentication with RBAC.
- Historical state queries (`atVersion`).
- OpenTelemetry tracing + Prometheus/Grafana monitoring.
- Full API documentation (OpenAPI 3.0).
- Kubernetes deployment manifests with HPA.

**Single-region deployment only (v1 constraint — per HLD A6).**

---

### v1.1 — Near-Term Iterations (4–8 weeks post-launch)

| Feature | Rationale |
|---------|-----------|
| Debezium CDC replacing Outbox Relay poller | Reduce relay latency from ~500ms to <50ms; eliminate polling load |
| Account reactivation command | Complete account lifecycle (suspend → active) |
| Multi-currency support (3 decimal currencies e.g. KWD) | Minor schema adjustment to monetary column scaling |
| Account-level command rate limiting (token bucket) | Protect write path from OCC storms on hot accounts |
| SLO-based alerting (error budget burn rate) | More actionable than threshold-based alerts |

---

### v2.0 — Strategic Roadmap (3–6 months post-launch)

| Feature | Rationale |
|---------|-----------|
| Multi-region active-passive deployment | Eliminate single-region outage risk; warm standby for RTO < 5min |
| Service mesh adoption (Istio/Linkerd) | Centralise mTLS management, circuit breakers, retry policies |
| Kafka Schema Registry enforcement (hard validation) | Prevent schema violations at produce/consume time in production |
| Event schema versioning framework (upcasters) | Manage schema evolution as domain grows |
| Portfolio aggregate (new aggregate type) | Extend domain model without schema changes |
| Consumer lag per-account tracking for high-value accounts | Fine-grained SLO monitoring |

---

## 6. Risk & Dependency Register

### 6.1 Technical Risks

| Risk ID | Risk Description | Probability | Impact | Mitigation |
|---------|-----------------|------------|--------|------------|
| R1 | **Hot aggregate OCC storms** — A single high-frequency account causes cascading retries, degrading write latency for that account. | Medium | High | Client-side rate limiting at Command API (Sprint 6). Exponential backoff with jitter on OCC retries. Alert on `occ_retries_total > 10/min` per account. |
| R2 | **Outbox Relay SPOF** — Relay crash causes read models to stagnate; sagas stall. | Low | High | Leader election (US-5.3) ensures fast failover. Kubernetes liveness probe + auto-restart. Long-term: replace with Debezium CDC (v1.1). |
| R3 | **Kafka partition rebalance disruption** — Consumer group rebalance under load causes processing pauses (seconds to minutes). | Medium | Medium | Enable Kafka cooperative incremental rebalancing. Size consumer groups conservatively (< 80% partition count). Alert on rebalance events. |
| R4 | **Saga compensation idempotency edge case** — Compensating command retried after partial commit causes double-refund. | Low | Critical | `FundsRefunded` uses `transferId` as idempotency correlation. OCC on saga state prevents duplicate compensation. Dedicated chaos test for crash-during-compensation in Sprint 6. |
| R5 | **Long event chains without snapshots** — Account with 10,000+ events causes hydration timeout. | Low | Medium | Snapshot every 50 events (US-9.2). Alert when any aggregate exceeds 200 events without a recent snapshot. |
| R6 | **Schema evolution complexity** — Adding new event fields breaks existing consumers or upcaster chains accumulate. | Medium | Medium | Define `schemaVersion` field on all events from Sprint 1. Establish upcaster framework in v2.0. |
| R7 | **Kafka topic under-partitioned at launch** — 12 partitions insufficient for actual throughput at scale. | Low | Medium | Partition count is configurable without code changes. Monitor `events_per_partition_per_second`; rebalance before saturation. |

---

### 6.2 Critical Sprint Dependencies

```
Sprint 1 (Foundation)
    └─► Sprint 2 (Commands & Auth) ─────────────────────────────────────┐
              └─► Sprint 3 (Kafka & Projections) ──────────────────────┤
                         └─► Sprint 4 (Transfer Saga) ─────────────────┤
                                    └─► Sprint 5 (Replay & Observability)
                                               └─► Sprint 6 (Hardening)
```

- **Sprint 2 is blocked** until Sprint 1 (event store schema, domain model, OCC) is complete.
- **Sprint 3 is blocked** until Outbox write (Sprint 2) and Kafka topic provisioning (Sprint 1) are done.
- **Sprint 4 is blocked** until projection handlers (Sprint 3) are live — saga completion relies on read model updates.
- **Sprint 5 is non-blocking** for MVP delivery but must complete before v1.0 release.

---

### 6.3 External Dependencies

| Dependency | Owner | Required By | Risk |
|-----------|-------|-------------|------|
| External Identity Provider (JWT issuer + JWKS endpoint) | Infrastructure / External | Sprint 2 | Medium — dev environment needs a mock IdP (Keycloak in Docker Compose) from Sprint 1 |
| HashiCorp Vault / AWS Secrets Manager | Infrastructure / DevOps | Sprint 6 | Low — local env uses `application.yml`; production requires Vault before v1.0 |
| PostgreSQL 15+ production cluster (with synchronous replication) | DBA / Infrastructure | Sprint 6 | Low — Docker Compose covers development; production infra must be provisioned before Sprint 6 |
| Apache Kafka production cluster (3 brokers) | Infrastructure / DevOps | Sprint 6 | Low — Confluent Cloud or self-managed; must be provisioned before Sprint 6 load test |
| Schema Registry (Confluent) | Infrastructure | Sprint 6 | Low — required for Schema Registry enforcement (v1.1 hard enforcement; v1.0 governance only) |

---

### 6.4 Definition of Done (Team Agreement)

A story is **Done** when all of the following are true:

- [ ] All acceptance criteria pass.
- [ ] Unit tests written and passing (≥ 80% coverage for new code).
- [ ] Integration tests written and passing.
- [ ] No `double`, `float`, or PII in event payloads (verified by ArchUnit).
- [ ] Code reviewed and approved by at least one other engineer.
- [ ] Structured logs emit correctly for the new feature.
- [ ] Merged to `main` with a passing CI build.
- [ ] Feature tested in the local Docker Compose environment end-to-end.

---

*End of Agile Delivery Plan — v1.0*

> **Document Control:** This plan is derived from HLD v1.0. Sprint allocations should be reviewed after Sprint 2 retrospective and adjusted based on actual team velocity.