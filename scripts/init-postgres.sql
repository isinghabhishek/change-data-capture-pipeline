-- =============================================================================
-- init-postgres.sql
-- Initialises the sourcedb PostgreSQL database for CDC pipeline use.
-- Safe to run multiple times (idempotent).
-- Target database: sourcedb
-- =============================================================================

\echo '==> [1/7] Creating cdc_user with REPLICATION and LOGIN privileges...'

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'cdc_user'
    ) THEN
        CREATE ROLE cdc_user
            WITH LOGIN
                 REPLICATION
                 PASSWORD 'cdc_password';
        RAISE NOTICE 'Role cdc_user created.';
    ELSE
        RAISE NOTICE 'Role cdc_user already exists — skipping creation.';
    END IF;
END
$$;

-- =============================================================================

\echo '==> [2/7] Creating source tables (orders, customers, products)...'

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL          PRIMARY KEY,
    customer_id INT             NOT NULL,
    amount      DECIMAL(10, 2)  NOT NULL,
    status      VARCHAR(50)     NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL          PRIMARY KEY,
    name        VARCHAR(255)    NOT NULL,
    email       VARCHAR(255)    NOT NULL UNIQUE,
    phone       VARCHAR(50),
    address     TEXT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id              SERIAL          PRIMARY KEY,
    name            VARCHAR(255)    NOT NULL,
    description     TEXT,
    price           DECIMAL(10, 2)  NOT NULL,
    stock_quantity  INT             NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- =============================================================================

\echo '==> [3/7] Creating cdc_heartbeat table (single-row, updated in-place)...'

-- Schema per design §3.4: single row (id=1), updated in-place every 60 s.
CREATE TABLE IF NOT EXISTS cdc_heartbeat (
    id          INT             PRIMARY KEY DEFAULT 1,
    ts          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline    VARCHAR(64)     NOT NULL DEFAULT 'main'
);

-- Seed the single heartbeat row if it does not already exist.
INSERT INTO cdc_heartbeat (id, ts, pipeline)
VALUES (1, NOW(), 'main')
ON CONFLICT (id) DO NOTHING;

-- =============================================================================

\echo '==> [4/7] Granting SELECT on monitored tables to cdc_user...'

-- Grants are applied after table creation so the objects exist.
GRANT SELECT ON TABLE orders        TO cdc_user;
GRANT SELECT ON TABLE customers     TO cdc_user;
GRANT SELECT ON TABLE products      TO cdc_user;
GRANT SELECT ON TABLE cdc_heartbeat TO cdc_user;

-- Allow cdc_user to read future tables added to the public schema.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO cdc_user;

-- =============================================================================

\echo '==> [5/7] Granting USAGE on schema public to cdc_user...'

GRANT USAGE ON SCHEMA public TO cdc_user;

-- =============================================================================

\echo '==> [6/7] Creating publication cdc_publication...'

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_publication WHERE pubname = 'cdc_publication'
    ) THEN
        -- Publication covers all four monitored tables.
        CREATE PUBLICATION cdc_publication
            FOR TABLE orders, customers, products, cdc_heartbeat;
        RAISE NOTICE 'Publication cdc_publication created.';
    ELSE
        RAISE NOTICE 'Publication cdc_publication already exists — skipping creation.';
    END IF;
END
$$;

-- =============================================================================

\echo '==> [7/7] Creating replication slot debezium_slot (pgoutput)...'

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_replication_slots
        WHERE  slot_name = 'debezium_slot'
    ) THEN
        PERFORM pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
        RAISE NOTICE 'Replication slot debezium_slot created.';
    ELSE
        RAISE NOTICE 'Replication slot debezium_slot already exists — skipping creation.';
    END IF;
END
$$;

-- =============================================================================

\echo '==> init-postgres.sql completed successfully.'
