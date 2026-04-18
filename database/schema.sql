-- ============================================================
-- DATABASE SCHEMA
-- Project: Global AI & Economic Data Aggregator
-- Course:  MSBA 305 - Data Processing Framework
-- Team:    Haddad, Issa, Fawaz, Salloum, Tabourian
-- Instructor: Dr. Ahmad El-Hajj
-- Last updated: April 2026
-- ============================================================
--
-- ARCHITECTURE OVERVIEW
-- This database is the silver layer of a medallion architecture
-- that aggregates economic and AI readiness data from multiple
-- international sources into a standardized, queryable format.
--
-- SCHEMAS:
--   metadata      → dictionary of the system. Defines every
--                   source, metric, and country. Nothing enters
--                   standardized without being defined here first.
--
--   standardized  → the heart of the system. One unified table
--                   containing every observation from every source,
--                   cleaned and reduced to the same shape.
--                   Partitioned by year for performance and
--                   hot/cold lifecycle management.
--
--   curated       → pre-built views and summary tables constructed
--                   from query pattern evidence in ops.query_log.
--                   Empty at launch, populated over time.
--
--   ops           → operational monitoring. Pipeline run logs,
--                   researcher query logs, partition registry.
--
-- STORAGE ARCHITECTURE:
--   Hot storage   → recent year partitions in Supabase PostgreSQL
--   Cold storage  → old year partitions exported as Parquet files
--                   on Backblaze B2, re-attached as foreign tables
--   Bronze files  → raw source files on Backblaze B2 permanently
--
-- FOREIGN KEYS ON PARTITIONED TABLES:
--   PostgreSQL does not support outbound foreign key constraints
--   from partitioned tables. Referential integrity for
--   country_iso3, metric_id, and source_id in
--   standardized.observations is enforced in transformation
--   scripts instead. All other foreign keys enforced at DB level.
--
-- CHANGES FROM V1:
--   - Removed metadata.countries.iso2 (unused)
--   - Removed metadata.metrics.subcategory (unused)
--   - Removed standardized.observations.unit (join from metadata)
--   - Added standardized.observation_revisions.old_unit
--   - Added standardized.observation_revisions.new_unit
--   - Renamed pipeline_runs status 'partial' to
--     'success_with_rej_rows' for clarity
--   - Dropped raw schema (bronze files live on Backblaze only)
--   - Dropped UN as a source (SDMX API too limited)
-- ============================================================


-- ============================================================
-- STEP 1 — CREATE SCHEMAS
-- ============================================================

CREATE SCHEMA metadata;
CREATE SCHEMA standardized;
CREATE SCHEMA curated;
CREATE SCHEMA ops;


-- ============================================================
-- STEP 2 — METADATA SCHEMA
--
-- The dictionary of the entire system.
-- Defines what every code, identifier, and metric means.
-- Populated at setup and updated as sources change.
-- All tables here are small, non-partitioned, and support
-- full foreign key enforcement.
-- ============================================================


-- ------------------------------------------------------------
-- metadata.sources
-- One row per data source.
-- Master registry of all data origins.
-- Every metric and every observation references this table.
-- Adding a new source = inserting one row. Zero schema changes.
--
-- Current sources:
--   world_bank → World Bank WDI (REST API, annual)
--   imf        → IMF WEO + IFS + Fiscal Monitor + AI Prep Index
--   oxford     → Oxford Insights GAIRI (XLSX download, annual)
--   pwt        → Penn World Tables (Stata file, irregular)
--   epoch_ai   → Epoch AI Large-Scale AI Models (CSV, continuous)
-- ------------------------------------------------------------
CREATE TABLE metadata.sources (
  source_id         TEXT  PRIMARY KEY,
  -- short unique identifier used throughout the system
  -- convention: lowercase underscore separated
  -- e.g. 'world_bank', 'oxford', 'imf'

  full_name         TEXT  NOT NULL,
  -- full human-readable name
  -- e.g. 'World Bank World Development Indicators'

  url               TEXT,
  -- direct URL to source or API documentation

  access_method     TEXT,
  -- how ingestion script pulls data
  -- values: 'REST API', 'CSV download', 'XLSX download',
  --         'Stata file download'

  update_frequency  TEXT,
  -- how often source publishes new or revised data
  -- values: 'annual', 'biannual', 'quarterly', 'monthly',
  --         'continuous', 'every few years'

  license           TEXT,
  -- data license governing usage and redistribution
  -- e.g. 'CC-BY 4.0', 'CC-BY-SA 4.0'

  last_retrieved    DATE,
  -- date of last successful pull
  -- updated by ingestion scripts after each successful run
  -- used as anchor for revision detection

  notes             TEXT
  -- source quirks, format changes, known issues,
  -- revision detection strategy for this source
);


-- ------------------------------------------------------------
-- metadata.metrics
-- One row per metric tracked by the system.
-- Master dictionary of everything we measure.
-- Every metric_id used in standardized.observations
-- must be defined here first.
--
-- Metadata population strategy:
--   World Bank → fully automated from REST API
--   IMF        → fully automated from DataMapper API
--   PWT        → automated from Stata file embedded labels
--   Oxford     → 1 metric hardcoded (overall score only)
--   Epoch AI   → 1 metric hardcoded
-- ------------------------------------------------------------
CREATE TABLE metadata.metrics (
  metric_id       TEXT      PRIMARY KEY,
  -- unique identifier following naming convention:
  -- source_prefix.snake_case_name
  -- e.g. 'wb.gdp_per_capita', 'oxford.ai_readiness'
  -- prefixes: wb, imf, oxford, pwt, epoch

  metric_name     TEXT      NOT NULL,
  -- human-readable name shown in Streamlit UI
  -- e.g. 'GDP per Capita (Current USD)'

  source_id       TEXT      NOT NULL
                  REFERENCES metadata.sources(source_id),
  -- which source this metric comes from

  category        TEXT,
  -- broad thematic grouping for UI browsing and filtering
  -- comes directly from World Bank topic name or source category
  -- e.g. 'Economy & Growth', 'Trade', 'AI Readiness'
  -- researchers browse and filter by this in Streamlit

  unit            TEXT,
  -- unit of measurement — current unit as reported by source
  -- e.g. 'current USD', '% of population', '0-100 score'
  -- joined into query results at runtime (not stored per row)
  -- when unit changes, old unit is captured in observation_revisions

  description     TEXT,
  -- full description of what this metric measures
  -- sourced from original source documentation or API
  -- shown in Streamlit when researcher selects a metric

  frequency       TEXT      NOT NULL  DEFAULT 'annual',
  -- temporal frequency of this metric
  -- values: 'annual', 'quarterly', 'monthly', 'biannual'
  -- used by Streamlit frequency filter

  available_from  SMALLINT,
  -- first year this metric has data e.g. 1960
  -- auto-calculated after ingestion:
  --   SELECT MIN(year) FROM standardized.observations
  --   WHERE metric_id = '...'
  -- used by Streamlit to warn about coverage gaps

  available_to    SMALLINT
  -- last year this metric has data e.g. 2024
  -- auto-calculated after ingestion:
  --   SELECT MAX(year) FROM standardized.observations
  --   WHERE metric_id = '...'
);


-- ------------------------------------------------------------
-- metadata.countries
-- Master country reference table.
-- One row per country covered by the system.
-- Uses ISO 3166-1 alpha-3 as the universal standard.
-- Fully automated from World Bank API.
--
-- Source-specific codes stored separately in
-- metadata.country_codes — adding new sources never
-- requires changing this table.
--
-- Edge cases documented in notes column:
--   Kosovo    → XKX (informal, not officially ISO-assigned)
--   Taiwan    → TWN (listed separately)
--   Palestine → PSE (ISO assigned)
--   Hong Kong → HKG (separate from China in most sources)
--   Macao     → MAC (separate from China in most sources)
-- ------------------------------------------------------------
CREATE TABLE metadata.countries (
  iso3          CHAR(3)   PRIMARY KEY,
  -- ISO 3166-1 alpha-3 code — universal identifier
  -- e.g. 'LBN' for Lebanon, 'FRA' for France

  country_name  TEXT      NOT NULL,
  -- official country name in English

  region        TEXT,
  -- World Bank geographic region
  -- e.g. 'Middle East & North Africa', 'Europe & Central Asia'
  -- enables regional comparison queries

  income_group  TEXT,
  -- World Bank income classification
  -- values: 'Low Income', 'Lower Middle Income',
  --         'Upper Middle Income', 'High Income'
  -- enables high vs low income country comparisons

  notes         TEXT
  -- documents edge cases, disputed territories,
  -- non-standard ISO assignments, mapping decisions
);


-- ------------------------------------------------------------
-- metadata.country_codes
-- Translation dictionary between every source's country
-- identifier and the universal ISO3 standard.
-- One row per country per source.
--
-- Why this exists separately from metadata.countries:
--   Every source uses different identifiers for the same country.
--   World Bank uses 'LBN', IMF uses 'LB', PWT uses 'Lebanon'.
--   This table lets transformation scripts translate any
--   source identifier to ISO3 without hardcoding mappings.
--   Adding a new source = inserting rows. Zero schema changes.
-- ------------------------------------------------------------
CREATE TABLE metadata.country_codes (
  iso3       CHAR(3)  NOT NULL
             REFERENCES metadata.countries(iso3),
  -- universal ISO3 identifier

  source_id  TEXT     NOT NULL
             REFERENCES metadata.sources(source_id),
  -- which source uses this code

  code       TEXT     NOT NULL,
  -- source's own identifier for this country
  -- e.g. 'LB' for IMF, 'LBN' for World Bank,
  --      'Lebanon' for PWT (uses country names)

  PRIMARY KEY (iso3, source_id),
  -- one code per country per source

  UNIQUE (source_id, code)
  -- one source code maps to exactly one country
  -- prevents duplicate mappings from data entry errors
);


-- ------------------------------------------------------------
-- metadata.metric_codes
-- Translation dictionary between every source's original
-- indicator code and our standardized metric_id.
-- One row per metric per source.
--
-- Why this exists separately from metadata.metrics:
--   Same reason as country_codes. World Bank calls GDP per capita
--   'NY.GDP.PCAP.CD', IMF calls it 'NGDPD'. Transformation
--   scripts look up the original code here to find metric_id.
--   Adding a new source = inserting rows. Zero schema changes.
-- ------------------------------------------------------------
CREATE TABLE metadata.metric_codes (
  metric_id  TEXT  NOT NULL
             REFERENCES metadata.metrics(metric_id),
  -- our standardized metric identifier

  source_id  TEXT  NOT NULL
             REFERENCES metadata.sources(source_id),
  -- which source uses this original code

  code       TEXT  NOT NULL,
  -- source's original indicator code
  -- e.g. 'NY.GDP.PCAP.CD' for World Bank GDP per capita
  --      'NGDPD' for IMF GDP, 'rtfpna' for PWT TFP

  PRIMARY KEY (metric_id, source_id),
  -- one original code per metric per source

  UNIQUE (source_id, code)
  -- one source code maps to exactly one metric_id
);


-- ============================================================
-- STEP 3 — OPS SCHEMA
--
-- Operational monitoring tables.
-- Tracks ingestion runs, researcher queries, and
-- partition lifecycle status.
-- ============================================================


-- ------------------------------------------------------------
-- ops.pipeline_runs
-- One row per ingestion script execution.
-- Written automatically by every ingestion script.
-- Primary tool for debugging, auditing, and governance.
--
-- Status values:
--   running               → currently executing
--   success               → completed, zero rejections
--   success_with_rej_rows → completed, some rows rejected
--   failed                → crashed, zero rows inserted
-- ------------------------------------------------------------
CREATE TABLE ops.pipeline_runs (
  run_id          SERIAL     PRIMARY KEY,

  source_id       TEXT,
  -- which source was ingested e.g. 'world_bank'

  started_at      TIMESTAMP  NOT NULL,
  -- when the ingestion script started executing

  completed_at    TIMESTAMP,
  -- when it finished — null if still running or crashed

  rows_inserted   INTEGER,
  -- rows successfully inserted into standardized.observations

  rows_rejected   INTEGER,
  -- rows that failed validation and were not inserted
  -- logged with reason in error_message

  status          TEXT
    CHECK (status IN (
      'running',
      'success',
      'success_with_rej_rows',
      'failed'
    )),
  -- running               → currently executing
  -- success               → all rows processed successfully
  -- success_with_rej_rows → completed but some rows rejected
  -- failed                → script crashed, no rows inserted

  error_message   TEXT,
  -- full error message if failed or success_with_rej_rows
  -- null if success

  notes           TEXT
  -- additional context: source format changes,
  -- manual overrides, version changes detected
);


-- ------------------------------------------------------------
-- ops.query_log
-- Tracks every query submitted through the Streamlit app.
-- Written automatically by the app on every researcher request.
-- Never visible to researchers — internal only.
--
-- Three purposes:
--   1. Build curated views from evidence — identify top 10
--      most frequent query patterns after 3-6 months usage
--   2. Identify slow queries via high response_ms values
--   3. Understand researcher behavior — popular metrics,
--      countries, year ranges
-- ------------------------------------------------------------
CREATE TABLE ops.query_log (
  query_id        SERIAL     PRIMARY KEY,

  requested_at    TIMESTAMP  NOT NULL  DEFAULT NOW(),
  -- exact timestamp of the researcher's request

  countries       TEXT[],
  -- array of ISO3 codes requested e.g. '{LBN,FRA,USA}'

  metric_ids      TEXT[],
  -- array of metric_ids requested

  year_from       SMALLINT,
  -- start of requested year range

  year_to         SMALLINT,
  -- end of requested year range

  period          TEXT,
  -- frequency requested: 'annual', 'monthly', 'quarterly'

  rows_returned   INTEGER,
  -- number of rows returned to researcher

  response_ms     INTEGER,
  -- query execution time in milliseconds
  -- high values identify candidates for curated views

  completeness    NUMERIC
  -- average data completeness of result (0.0 to 1.0)
  -- e.g. 0.73 means 73% of requested cells had data
  -- shown to researcher as coverage warning in Streamlit
);


-- ------------------------------------------------------------
-- ops.partition_registry
-- Tracks status, size, and location of every year partition.
-- Used by annual maintenance script to determine hot/cold
-- boundary based on real measured sizes after ingestion.
--
-- Hot/cold decision process:
--   1. After full ingestion, measure real partition sizes
--   2. Run 50-year growth simulation from real data
--   3. Find when Supabase tier limit is exceeded
--   4. Set hot/cold boundary based on evidence
--   5. Migrate cold partitions to Backblaze B2 as Parquet
-- ------------------------------------------------------------
CREATE TABLE ops.partition_registry (
  partition_name    TEXT      PRIMARY KEY,
  -- e.g. 'y_1960s', 'y_2020'

  year_from         SMALLINT  NOT NULL,
  -- first year covered by this partition e.g. 1960

  year_to           SMALLINT  NOT NULL,
  -- last year covered (exclusive) e.g. 1970 for y_1960s

  status            TEXT      NOT NULL  DEFAULT 'hot'
    CHECK (status IN ('hot', 'cold', 'migrating')),
  -- hot       → in Supabase PostgreSQL, fast queries
  -- cold      → Parquet on Backblaze B2, slower queries
  -- migrating → being moved, do not query directly

  storage_location  TEXT,
  -- null if hot
  -- Backblaze B2 path if cold
  -- e.g. 's3://bucket/silver/y_1960s.parquet'

  size_bytes        BIGINT,
  -- measured size after ingestion
  -- input to hot/cold boundary simulation

  row_count         BIGINT,
  -- number of rows in this partition
  -- input to hot/cold boundary simulation

  migrated_at       DATE
  -- when partition was moved to cold storage
  -- null if still hot
);


-- ============================================================
-- STEP 4 — STANDARDIZED SCHEMA
--
-- The heart of the system. One unified table containing
-- every observation from every source, cleaned and reduced
-- to the same canonical shape.
--
-- KEY DESIGN DECISIONS:
--
-- Tall format: one row per country-year-period-metric.
--   Adding new sources or metrics requires zero schema changes.
--
-- No unit column in observations:
--   Unit is stored once in metadata.metrics and joined at
--   query time. Avoids storing the same string millions of times.
--   Unit change history captured in observation_revisions via
--   old_unit and new_unit columns.
--
-- value as TEXT:
--   Stores both numeric and categorical values.
--   value_numeric computed column handles all math.
--
-- No quality_flag:
--   Sources maintain their own QC. As a harmonization platform
--   our role is faithful representation of source data.
--
-- No NULL rows:
--   Absence of a row means absence of data. Clean, unambiguous.
--
-- Partitioned by year:
--   Enables partition pruning for fast year-range queries and
--   hot/cold lifecycle management.
--
-- Foreign key limitation:
--   PostgreSQL does not support outbound foreign key constraints
--   from partitioned tables. Referential integrity for
--   country_iso3, metric_id, and source_id is enforced in
--   transformation scripts by validating against metadata tables
--   before every insert.
-- ============================================================


-- ------------------------------------------------------------
-- standardized.observations
-- The main data table. Partitioned by year.
-- Every row = one measurement:
--   one country × one year × one period × one metric
-- ------------------------------------------------------------
CREATE TABLE standardized.observations (

  country_iso3  CHAR(3)   NOT NULL,
  -- ISO 3166-1 alpha-3 code
  -- validated against metadata.countries in transformation scripts

  year          SMALLINT  NOT NULL,
  -- 4-digit year — determines which partition this row goes into

  period        TEXT      NOT NULL  DEFAULT 'annual',
  -- temporal granularity within the year
  -- 'annual'    → full year observation (default)
  -- 'Q1'-'Q4'   → quarterly
  -- 'M01'-'M12' → monthly
  -- 'H1'-'H2'   → biannual

  metric_id     TEXT      NOT NULL,
  -- standardized metric identifier
  -- validated against metadata.metrics in transformation scripts

  value         TEXT,
  -- actual data value stored as TEXT for flexibility
  -- stores both numeric ('8214.5') and categorical ('Civil Law')
  -- never NULL — if no data, no row is inserted

  value_numeric NUMERIC
    GENERATED ALWAYS AS (
      CASE
        WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC
        ELSE NULL
      END
    ) STORED,
  -- computed column: extracts numeric value automatically
  -- NULL if value is non-numeric (categorical data)
  -- use this column for all aggregations, math, comparisons
  -- PostgreSQL manages this — never insert manually

  source_id     TEXT      NOT NULL,
  -- which source this observation came from
  -- validated against metadata.sources in transformation scripts

  retrieved_at  DATE      NOT NULL,
  -- date this value was pulled from the source
  -- used for audit trail and revision detection

  PRIMARY KEY (country_iso3, year, period, metric_id)
  -- uniquely identifies every observation

) PARTITION BY RANGE (year);


-- ============================================================
-- STEP 5 — YEAR PARTITIONS
--
-- Partition strategy:
--   1950-2009 → 6 decade partitions
--               old data, migrated to cold storage by decade
--   2010-2030 → 21 individual year partitions
--               recent + projected data, precise annual control
--   Total: 27 partitions, no default partition
--
-- No default partition — annual maintenance script always
-- creates the new year partition BEFORE ingestion runs.
-- Script order is hardcoded: create partition → run ingestion.
--
-- Hot/cold boundary decided AFTER ingestion by measuring
-- real partition sizes and running growth simulation.
-- Not decided upfront.
-- ============================================================

-- Decade partitions (1950-2009)
CREATE TABLE standardized.y_1950s
  PARTITION OF standardized.observations
  FOR VALUES FROM (1950) TO (1960);

CREATE TABLE standardized.y_1960s
  PARTITION OF standardized.observations
  FOR VALUES FROM (1960) TO (1970);

CREATE TABLE standardized.y_1970s
  PARTITION OF standardized.observations
  FOR VALUES FROM (1970) TO (1980);

CREATE TABLE standardized.y_1980s
  PARTITION OF standardized.observations
  FOR VALUES FROM (1980) TO (1990);

CREATE TABLE standardized.y_1990s
  PARTITION OF standardized.observations
  FOR VALUES FROM (1990) TO (2000);

CREATE TABLE standardized.y_2000s
  PARTITION OF standardized.observations
  FOR VALUES FROM (2000) TO (2010);

-- Individual year partitions (2010-2030)
CREATE TABLE standardized.y_2010 PARTITION OF standardized.observations FOR VALUES FROM (2010) TO (2011);
CREATE TABLE standardized.y_2011 PARTITION OF standardized.observations FOR VALUES FROM (2011) TO (2012);
CREATE TABLE standardized.y_2012 PARTITION OF standardized.observations FOR VALUES FROM (2012) TO (2013);
CREATE TABLE standardized.y_2013 PARTITION OF standardized.observations FOR VALUES FROM (2013) TO (2014);
CREATE TABLE standardized.y_2014 PARTITION OF standardized.observations FOR VALUES FROM (2014) TO (2015);
CREATE TABLE standardized.y_2015 PARTITION OF standardized.observations FOR VALUES FROM (2015) TO (2016);
CREATE TABLE standardized.y_2016 PARTITION OF standardized.observations FOR VALUES FROM (2016) TO (2017);
CREATE TABLE standardized.y_2017 PARTITION OF standardized.observations FOR VALUES FROM (2017) TO (2018);
CREATE TABLE standardized.y_2018 PARTITION OF standardized.observations FOR VALUES FROM (2018) TO (2019);
CREATE TABLE standardized.y_2019 PARTITION OF standardized.observations FOR VALUES FROM (2019) TO (2020);
CREATE TABLE standardized.y_2020 PARTITION OF standardized.observations FOR VALUES FROM (2020) TO (2021);
CREATE TABLE standardized.y_2021 PARTITION OF standardized.observations FOR VALUES FROM (2021) TO (2022);
CREATE TABLE standardized.y_2022 PARTITION OF standardized.observations FOR VALUES FROM (2022) TO (2023);
CREATE TABLE standardized.y_2023 PARTITION OF standardized.observations FOR VALUES FROM (2023) TO (2024);
CREATE TABLE standardized.y_2024 PARTITION OF standardized.observations FOR VALUES FROM (2024) TO (2025);
CREATE TABLE standardized.y_2025 PARTITION OF standardized.observations FOR VALUES FROM (2025) TO (2026);
CREATE TABLE standardized.y_2026 PARTITION OF standardized.observations FOR VALUES FROM (2026) TO (2027);
CREATE TABLE standardized.y_2027 PARTITION OF standardized.observations FOR VALUES FROM (2027) TO (2028);
CREATE TABLE standardized.y_2028 PARTITION OF standardized.observations FOR VALUES FROM (2028) TO (2029);
CREATE TABLE standardized.y_2029 PARTITION OF standardized.observations FOR VALUES FROM (2029) TO (2030);
CREATE TABLE standardized.y_2030 PARTITION OF standardized.observations FOR VALUES FROM (2030) TO (2031);


-- ============================================================
-- STEP 6 — INDEXES ON STANDARDIZED.OBSERVATIONS
--
-- Defined on parent table, automatically propagated to all
-- partitions including future ones.
--
-- Composite indexes match actual researcher query patterns:
--   idx_country_metric → researcher picks country + metric
--   idx_metric_year    → one metric across all countries
--   idx_source         → source auditing queries
--   idx_period         → frequency filtering in UI
-- ============================================================

CREATE INDEX idx_country_metric
  ON standardized.observations (country_iso3, metric_id);

CREATE INDEX idx_metric_year
  ON standardized.observations (metric_id, year);

CREATE INDEX idx_source
  ON standardized.observations (source_id);

CREATE INDEX idx_period
  ON standardized.observations (period);


-- ============================================================
-- STEP 7 — OBSERVATION REVISIONS TABLE
--
-- Records when source data changes between ingestion runs.
-- Sources silently revise historical values — without this
-- table those changes are permanently lost.
--
-- Includes old_unit and new_unit to capture unit changes.
-- When old_unit ≠ new_unit, Streamlit shows a warning to
-- the researcher that values are not directly comparable
-- across the revision date.
--
-- How it works:
--   Before every upsert, transformation scripts compare
--   incoming value AND unit to stored value AND unit.
--   If either differs, the old values are logged here
--   before being overwritten in standardized.observations.
-- ============================================================

CREATE TABLE standardized.observation_revisions (
  revision_id    SERIAL     PRIMARY KEY,

  country_iso3   CHAR(3)    NOT NULL,
  -- country whose data was revised

  year           SMALLINT   NOT NULL,
  -- historical year that was revised
  -- not the year the revision was detected

  period         TEXT       NOT NULL  DEFAULT 'annual',

  metric_id      TEXT       NOT NULL,
  -- which metric was revised

  old_value      TEXT,
  -- value as stored before this revision

  new_value      TEXT,
  -- value as reported by source after revision

  old_unit       TEXT,
  -- unit before revision
  -- null if unit did not change
  -- when old_unit != new_unit → Streamlit shows warning

  new_unit       TEXT,
  -- unit after revision
  -- null if unit did not change

  revised_at     DATE       NOT NULL,
  -- date revision was detected during ingestion

  source_id      TEXT       NOT NULL,
  -- which source published the revised value

  revision_note  TEXT
  -- context: methodology update, unit change, correction
);


-- ============================================================
-- STEP 8 — CURATED SCHEMA
--
-- Empty at launch. Three types of objects added over time
-- based on evidence from ops.query_log:
--
-- 1. REGULAR VIEWS — saved SQL shortcuts, no storage cost
--    Use when query is already fast, just a convenience
--
-- 2. MATERIALIZED VIEWS — pre-computed results stored on disk
--    Built for top 10 most frequent query patterns
--    identified from ops.query_log after 3-6 months usage
--    Refreshed annually after ingestion
--
-- 3. SUMMARY TABLES — pre-aggregated analytical results
--    Full control over updates via maintenance script
--    Powers analytical queries and Streamlit visualizations
--    Examples: regional AI readiness averages by year,
--    income group productivity comparisons
--
-- "We don't pre-build views arbitrarily. We track query
--  patterns and build views from evidence."
-- ============================================================


-- ============================================================
-- STEP 9 — DATABASE ROLES
--
-- Two roles implementing principle of least privilege.
--
-- pipeline_writer: ingestion and transformation scripts
--   Read and write observations, revisions, metadata, ops
--
-- app_reader: Streamlit researcher application
--   Read only on all data
--   INSERT only on ops.query_log (never SELECT — privacy)
--   Cannot modify any actual data under any circumstances
--
-- Neither role has DELETE permissions.
-- Data is never deleted — only upserted or migrated to cold.
-- ============================================================

CREATE ROLE pipeline_writer;

GRANT USAGE ON SCHEMA standardized, metadata, curated, ops
  TO pipeline_writer;

GRANT SELECT, INSERT, UPDATE
  ON standardized.observations
  TO pipeline_writer;

GRANT SELECT, INSERT, UPDATE
  ON standardized.observation_revisions
  TO pipeline_writer;

GRANT INSERT, SELECT
  ON ops.pipeline_runs
  TO pipeline_writer;

GRANT SELECT, INSERT, UPDATE
  ON metadata.sources, metadata.metrics, metadata.countries,
     metadata.country_codes, metadata.metric_codes
  TO pipeline_writer;

GRANT INSERT, SELECT, UPDATE
  ON ops.query_log, ops.partition_registry
  TO pipeline_writer;

GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA ops
  TO pipeline_writer;

GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA standardized
  TO pipeline_writer;


CREATE ROLE app_reader;

GRANT USAGE ON SCHEMA standardized, metadata, curated, ops
  TO app_reader;

GRANT SELECT
  ON standardized.observations
  TO app_reader;

GRANT SELECT
  ON metadata.sources, metadata.metrics, metadata.countries,
     metadata.country_codes, metadata.metric_codes
  TO app_reader;

GRANT SELECT
  ON ALL TABLES IN SCHEMA curated
  TO app_reader;

GRANT INSERT
  ON ops.query_log
  TO app_reader;
-- INSERT only — cannot read what other researchers queried
-- protects researcher query privacy

GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA ops
  TO app_reader;