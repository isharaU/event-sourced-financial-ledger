-- ============================================================================
-- V1__create_read_model_schema.sql
-- Flyway migration: Read Model schema for CQRS / Event-Sourced Financial Ledger
-- Target: PostgreSQL 15+
-- Read DB instance (port 5433 in local dev)
-- ============================================================================

-- ============================================================================
-- 1. APPLICATION ROLE
--    Read-side role: full DML on read model tables.
--    Projection handler needs INSERT/UPDATE; query-api needs SELECT.
-- ============================================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'esfl_app_read') THEN
        CREATE ROLE esfl_app_read NOLOGIN;
    END IF;
END
$$;

-- ============================================================================
-- 2. account_summary_view
--    Denormalised read model of account state, maintained by projections.
-- ============================================================================
CREATE TABLE IF NOT EXISTS account_summary_view (
    account_id         UUID            NOT NULL,
    owner_id           UUID            NOT NULL,
    balance            BIGINT          NOT NULL DEFAULT 0,
    currency           VARCHAR(3)      NOT NULL DEFAULT 'USD',
    status             VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
    aggregate_version  INTEGER         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT now(),

    PRIMARY KEY (account_id)
);

-- ============================================================================
-- 3. transaction_history_view
--    Denormalised transaction log for account activity queries.
-- ============================================================================
CREATE TABLE IF NOT EXISTS transaction_history_view (
    transaction_id     UUID            NOT NULL,
    account_id         UUID            NOT NULL,
    type               VARCHAR(30)     NOT NULL,
    amount             BIGINT          NOT NULL,
    currency           VARCHAR(3)      NOT NULL DEFAULT 'USD',
    occurred_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    description        TEXT,
    aggregate_version  INTEGER         NOT NULL DEFAULT 0,

    PRIMARY KEY (transaction_id)
);

-- ============================================================================
-- 4. B-TREE INDEXES on transaction_history_view
--    Optimise the most frequent query patterns:
--      - Fetch transactions by account (projection handler + query API)
--      - Date-range queries for transaction history
--      - Single transaction lookup (covered by PK, explicit index for clarity)
-- ============================================================================

-- Transactions by account — used by GET /accounts/{id}/transactions
CREATE INDEX IF NOT EXISTS idx_txn_history_account_id
    ON transaction_history_view (account_id);

-- Date-range filtering — used by paginated history & audit queries
CREATE INDEX IF NOT EXISTS idx_txn_history_occurred_at
    ON transaction_history_view (occurred_at);

-- Explicit B-tree on PK column — PostgreSQL already creates one for the PK
-- constraint, but listed here for spec compliance (AC bullet 4).
-- This is a no-op since the PK implicitly defines this index.
-- CREATE INDEX IF NOT EXISTS idx_txn_history_transaction_id
--     ON transaction_history_view (transaction_id);

-- Composite index for cursor-based pagination: (occurred_at, transaction_id)
-- Used by the query API for keyset pagination (see US-8.2)
CREATE INDEX IF NOT EXISTS idx_txn_history_cursor
    ON transaction_history_view (occurred_at, transaction_id);

-- ============================================================================
-- 5. ROLE GRANTS
--    esfl_app_read gets full DML on the read model tables so that:
--      - projection-handler can INSERT/UPDATE projected rows
--      - query-api can SELECT for read queries
-- ============================================================================
GRANT SELECT, INSERT, UPDATE, DELETE ON account_summary_view      TO esfl_app_read;
GRANT SELECT, INSERT, UPDATE, DELETE ON transaction_history_view   TO esfl_app_read;
