-- ============================================================
-- RESET SCRIPT — DEVELOPMENT ONLY
-- WARNING: Deletes everything. Never run on real data.
-- Use only during development to start completely fresh.
-- ============================================================

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA standardized FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA curated FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ops FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ops FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA standardized FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA metadata FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA standardized FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA curated FROM pipeline_writer';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA ops FROM pipeline_writer';
  END IF;
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_reader') THEN
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA standardized FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA curated FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA ops FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ops FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA standardized FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA metadata FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA standardized FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA curated FROM app_reader';
    EXECUTE 'REVOKE ALL PRIVILEGES ON SCHEMA ops FROM app_reader';
  END IF;
END $$;

DROP ROLE IF EXISTS pipeline_writer;
DROP ROLE IF EXISTS app_reader;

DROP SCHEMA IF EXISTS metadata     CASCADE;
DROP SCHEMA IF EXISTS standardized CASCADE;
DROP SCHEMA IF EXISTS curated      CASCADE;
DROP SCHEMA IF EXISTS ops          CASCADE;
DROP SCHEMA IF EXISTS raw          CASCADE;