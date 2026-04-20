-- ============================================================
-- RESET SCRIPT — DEVELOPMENT ONLY
-- ============================================================
--
-- WARNING: This script destroys everything in the database.
-- NEVER run this on a production database with real data.
-- Use only during development to start completely fresh.
--
-- WHAT THIS DOES:
--   1. Revokes all privileges granted to pipeline_writer and
--      app_reader (must happen before the roles can be dropped)
--   2. Drops both roles
--   3. Drops all four schemas with CASCADE, which automatically
--      drops every table, index, sequence, and view inside them
--
-- AFTER RUNNING THIS:
--   1. Run schema.sql to rebuild everything from scratch
--   2. Run: python3 database/run_seeds.py
--      (takes 10-20 minutes, mostly World Bank API calls)
--
-- ORDER MATTERS:
--   Privileges must be revoked before roles can be dropped.
--   Schemas must be dropped after roles because the REVOKE
--   statements reference the schemas.
-- ============================================================


-- ============================================================
-- STEP 1 — REVOKE ALL PRIVILEGES
--
-- PostgreSQL will not drop a role that still has privileges
-- granted to it anywhere. We must explicitly revoke every
-- grant before dropping the role.
--
-- We wrap this in a DO block with IF EXISTS checks because
-- the roles may not exist (e.g. if reset.sql is run on a
-- truly blank database). Without the IF EXISTS check,
-- the REVOKE statements would crash on a blank database.
-- ============================================================

DO $$
BEGIN

  -- ── pipeline_writer ────────────────────────────────────────
  -- Revoke every grant we gave pipeline_writer in schema.sql,
  -- in the same order they were granted (tables, then schemas).
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_writer') THEN

    -- Data tables
    EXECUTE 'REVOKE ALL PRIVILEGES ON standardized.observations           FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON standardized.observation_revisions  FROM pipeline_writer';

    -- Metadata tables
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.sources        FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.metrics        FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.countries      FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.country_codes  FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.metric_codes   FROM pipeline_writer';

    -- Ops tables (existing)
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.pipeline_runs       FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.query_log           FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.partition_registry  FROM pipeline_writer';

    -- Ops tables (new — added in V3 schema)
    -- These must be explicitly listed because they did not exist
    -- in the previous schema version.
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.checkpoints         FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.quality_runs        FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.rejection_summary   FROM pipeline_writer';

    -- Sequences (covers all sequences in these schemas)
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ops           FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA standardized  FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata      FROM pipeline_writer';

    -- Schema-level USAGE
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA metadata      FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA standardized  FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA curated       FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA ops           FROM pipeline_writer';

  END IF;

  -- ── app_reader ─────────────────────────────────────────────
  -- Revoke every grant we gave app_reader in schema.sql.
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') THEN

    -- Data tables
    EXECUTE 'REVOKE ALL PRIVILEGES ON standardized.observations           FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON standardized.observation_revisions  FROM app_reader';

    -- Metadata tables
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.sources        FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.metrics        FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.countries      FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.country_codes  FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON metadata.metric_codes   FROM app_reader';

    -- Ops tables (existing)
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.query_log     FROM app_reader';

    -- Ops tables (new — added in V3 schema)
    EXECUTE 'REVOKE ALL PRIVILEGES ON ops.quality_runs  FROM app_reader';

    -- Sequences
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ops           FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA standardized  FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata      FROM app_reader';

    -- Schema-level USAGE
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA metadata      FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA standardized  FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA curated       FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA ops           FROM app_reader';

  END IF;

END $$;


-- ============================================================
-- STEP 2 — DROP ROLES
--
-- Roles can only be dropped after all their privileges have
-- been revoked. The IF EXISTS clause makes this safe to run
-- even if the roles don't exist (e.g. on a blank database).
-- ============================================================

DROP ROLE IF EXISTS pipeline_writer;
DROP ROLE IF EXISTS app_reader;


-- ============================================================
-- STEP 3 — DROP SCHEMAS
--
-- CASCADE automatically drops every object inside each schema:
-- all tables, indexes, sequences, views, constraints.
-- This is much simpler than dropping each table individually.
--
-- We drop schemas in reverse dependency order:
--   curated depends on nothing in other schemas
--   standardized depends on nothing in other schemas
--   ops depends on nothing in other schemas
--   metadata depends on nothing in other schemas
-- (All cross-schema references are in application code,
-- not in FK constraints between schemas. So any order works.)
--
-- IF EXISTS makes this safe on a blank database.
-- ============================================================

DROP SCHEMA IF EXISTS metadata      CASCADE;
DROP SCHEMA IF EXISTS standardized  CASCADE;
DROP SCHEMA IF EXISTS curated       CASCADE;
DROP SCHEMA IF EXISTS ops           CASCADE;