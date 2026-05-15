-- ============================================================================
-- V1__create_event_store_schema.sql
-- Flyway migration: Event Store schema for CQRS / Event-Sourced Financial Ledger
-- Target: PostgreSQL 15+
-- ============================================================================

-- ============================================================================
-- 1. APPLICATION ROLE
--    Restricted write-side role: INSERT + SELECT only on domain_events.
--    No UPDATE or DELETE — enforces the append-only event store invariant.
-- ============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'esfl_app_write') THEN
        CREATE ROLE esfl_app_write NOLOGIN;
    END IF;
END
$$;

-- ============================================================================
-- 2. domain_events  (HASH-PARTITIONED on aggregate_id, 16 partitions)
--    Append-only event log. Each row is an immutable fact.
-- ============================================================================
CREATE TABLE IF NOT EXISTS domain_events (
    event_id           UUID            NOT NULL,
    aggregate_id       UUID            NOT NULL,
    aggregate_version  INTEGER         NOT NULL,
    event_type         VARCHAR(255)    NOT NULL,
    payload            JSONB           NOT NULL,
    checksum           CHAR(64)        NOT NULL,
    schema_version     INTEGER         NOT NULL DEFAULT 1,
    occurred_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (aggregate_id, event_id),
    UNIQUE      (aggregate_id, aggregate_version)
) PARTITION BY HASH (aggregate_id);

-- Create 16 hash partitions
CREATE TABLE IF NOT EXISTS domain_events_p00 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 0);
CREATE TABLE IF NOT EXISTS domain_events_p01 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 1);
CREATE TABLE IF NOT EXISTS domain_events_p02 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 2);
CREATE TABLE IF NOT EXISTS domain_events_p03 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 3);
CREATE TABLE IF NOT EXISTS domain_events_p04 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 4);
CREATE TABLE IF NOT EXISTS domain_events_p05 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 5);
CREATE TABLE IF NOT EXISTS domain_events_p06 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 6);
CREATE TABLE IF NOT EXISTS domain_events_p07 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 7);
CREATE TABLE IF NOT EXISTS domain_events_p08 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 8);
CREATE TABLE IF NOT EXISTS domain_events_p09 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 9);
CREATE TABLE IF NOT EXISTS domain_events_p10 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 10);
CREATE TABLE IF NOT EXISTS domain_events_p11 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 11);
CREATE TABLE IF NOT EXISTS domain_events_p12 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 12);
CREATE TABLE IF NOT EXISTS domain_events_p13 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 13);
CREATE TABLE IF NOT EXISTS domain_events_p14 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 14);
CREATE TABLE IF NOT EXISTS domain_events_p15 PARTITION OF domain_events FOR VALUES WITH (MODULUS 16, REMAINDER 15);

-- Index for aggregate hydration: load events in version order
CREATE INDEX IF NOT EXISTS idx_domain_events_aggregate_version
    ON domain_events (aggregate_id, aggregate_version ASC);

-- Index for outbox relay / temporal queries
CREATE INDEX IF NOT EXISTS idx_domain_events_occurred_at
    ON domain_events (occurred_at);

-- ============================================================================
-- 3. event_outbox
--    Transactional Outbox pattern — bridging event store and Kafka.
-- ============================================================================
CREATE TABLE IF NOT EXISTS event_outbox (
    outbox_id      UUID            NOT NULL DEFAULT gen_random_uuid(),
    event_id       UUID            NOT NULL,
    aggregate_id   UUID            NOT NULL,
    topic          VARCHAR(255)    NOT NULL,
    partition_key  VARCHAR(255)    NOT NULL,
    payload        JSONB           NOT NULL,
    status         VARCHAR(10)     NOT NULL DEFAULT 'PENDING',
    created_at     TIMESTAMPTZ     NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,

    PRIMARY KEY (outbox_id),

    CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PUBLISHED'))
);

-- Relay poller picks up pending records ordered by creation time
CREATE INDEX IF NOT EXISTS idx_event_outbox_status_created
    ON event_outbox (status, created_at ASC)
    WHERE status = 'PENDING';

-- ============================================================================
-- 4. account_snapshots
--    Periodic snapshots of aggregate state for replay performance.
-- ============================================================================
CREATE TABLE IF NOT EXISTS account_snapshots (
    snapshot_id        UUID            NOT NULL DEFAULT gen_random_uuid(),
    aggregate_id       UUID            NOT NULL,
    aggregate_version  INTEGER         NOT NULL,
    snapshot_data      JSONB           NOT NULL,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (snapshot_id),

    CONSTRAINT uq_snapshots_aggregate_version
        UNIQUE (aggregate_id, aggregate_version)
);

-- Fast lookup: latest snapshot for a given aggregate
CREATE INDEX IF NOT EXISTS idx_account_snapshots_aggregate
    ON account_snapshots (aggregate_id, aggregate_version DESC);

-- ============================================================================
-- 5. idempotency_keys
--    Client-supplied idempotency keys with cached response payloads.
-- ============================================================================
CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key    UUID            NOT NULL,
    response_payload   JSONB           NOT NULL,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    expires_at         TIMESTAMPTZ     NOT NULL,

    PRIMARY KEY (idempotency_key)
);

-- Scheduled cleanup of expired keys
CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires
    ON idempotency_keys (expires_at);

-- ============================================================================
-- 6. saga_state
--    Durable state for the Fund Transfer Saga coordinator.
-- ============================================================================
CREATE TABLE IF NOT EXISTS saga_state (
    saga_id                UUID            NOT NULL,
    transfer_id            UUID            NOT NULL,
    source_account_id      UUID            NOT NULL,
    destination_account_id UUID            NOT NULL,
    amount                 BIGINT          NOT NULL,
    currency               VARCHAR(3)      NOT NULL DEFAULT 'USD',
    step                   VARCHAR(30)     NOT NULL,
    status                 VARCHAR(20)     NOT NULL,
    version                INTEGER         NOT NULL DEFAULT 0,
    created_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (saga_id),

    CONSTRAINT chk_saga_step
        CHECK (step IN ('DEBIT_INITIATED', 'CREDIT_INITIATED', 'COMPLETED', 'COMPENSATING', 'FAILED')),

    CONSTRAINT chk_saga_status
        CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED'))
);

-- Saga recovery on restart: find all in-progress sagas
CREATE INDEX IF NOT EXISTS idx_saga_state_status
    ON saga_state (status)
    WHERE status = 'IN_PROGRESS';

-- Correlation lookup by transfer ID
CREATE INDEX IF NOT EXISTS idx_saga_state_transfer
    ON saga_state (transfer_id);

-- ============================================================================
-- 7. ROLE GRANTS
--    esfl_app_write gets INSERT + SELECT only on domain_events.
--    Full access on the remaining operational tables.
-- ============================================================================

-- domain_events: append-only (INSERT + SELECT, no UPDATE/DELETE)
GRANT INSERT, SELECT ON domain_events TO esfl_app_write;

-- The role also needs access to the partitions
GRANT INSERT, SELECT ON domain_events_p00 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p01 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p02 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p03 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p04 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p05 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p06 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p07 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p08 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p09 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p10 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p11 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p12 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p13 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p14 TO esfl_app_write;
GRANT INSERT, SELECT ON domain_events_p15 TO esfl_app_write;

-- Operational tables: full DML for the write role
GRANT SELECT, INSERT, UPDATE, DELETE ON event_outbox       TO esfl_app_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON account_snapshots  TO esfl_app_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON idempotency_keys   TO esfl_app_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON saga_state         TO esfl_app_write;
