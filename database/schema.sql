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
-- This database is the core of a data harmonization platform
-- that aggregates economic and AI readiness data from multiple
-- international sources and serves it to researchers in a
-- standardized, queryable format.
--
-- FOUR SCHEMAS:
--   metadata      → dictionary of the system. Defines every
--                   source, metric, and country. Nothing enters
--                   standardized without being defined here first.
--
--   raw           → raw source files live on local disk and
--                   Backblaze B2. Nothing from raw files enters
--                   this database. Schema kept for legacy reference.
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
-- SUBNATIONAL DATA:
--   Country-level data lives here in PostgreSQL forever.
--   Subnational data routes automatically to a separate MongoDB
--   instance via transformation scripts based on region granularity.
--   Researchers receive separate CSV files per granularity level.
--
-- PERIODIC DATA:
--   Annual, quarterly, and monthly data all live in this table.
--   The period column distinguishes frequency:
--   'annual', 'Q1'-'Q4', 'M01'-'M12', 'H1'-'H2', 'W01'-'W52'
--
-- FOREIGN KEYS ON PARTITIONED TABLES:
--   PostgreSQL does not support outbound foreign key constraints
--   from partitioned tables (as of PostgreSQL 17). Referential
--   integrity for country_iso3, metric_id, and source_id in
--   standardized.observations is enforced in transformation
--   scripts instead. All other foreign keys are enforced at
--   database level.
-- ============================================================


-- ============================================================
-- STEP 1 — CREATE THE FIVE SCHEMAS
-- ============================================================

CREATE SCHEMA metadata;
CREATE SCHEMA raw;
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
--   imf        → IMF WEO (API, annual/quarterly)
--   un         → UN Stats (API, annual)
--   oxford     → Oxford GAIRI (XLSX download, annual)
--   pwt        → Penn World Tables (CSV download, ~every 3 years)
--   wipo       → WIPO Global Innovation Index (TSV, annual)
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
  --         'TSV download', 'XLS download'

  update_frequency  TEXT,
  -- how often source publishes new or revised data
  -- values: 'annual', 'biannual', 'quarterly', 'monthly'

  license           TEXT,
  -- data license governing usage and redistribution
  -- e.g. 'CC-BY 4.0', 'proprietary'

  last_retrieved    DATE,
  -- date of last successful pull
  -- used by revision detection as anchor date
  -- World Bank API: pass as lastUpdated parameter
  -- IMF/UN: re-pull last 5 years since this date
  -- Oxford/PWT/WIPO: compare full file since this date

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
-- Populated automatically from APIs where possible:
--   World Bank → fully automated (name, unit, description, topics)
--   IMF        → fully automated (name, description)
--   Oxford/PWT/WIPO → manual (~15 metrics total, one-time)
-- ------------------------------------------------------------
CREATE TABLE metadata.metrics (
  metric_id       TEXT      PRIMARY KEY,
  -- unique identifier following naming convention:
  -- source_prefix.snake_case_name
  -- e.g. 'wb.gdp_per_capita', 'oxford.ai_readiness', 'pwt.tfp'
  -- source prefixes: wb, imf, un, oxford, pwt, wipo

  metric_name     TEXT      NOT NULL,
  -- human-readable name shown in Streamlit UI
  -- e.g. 'GDP per Capita (Current USD)'

  source_id       TEXT      NOT NULL
                  REFERENCES metadata.sources(source_id),
  -- which source this metric comes from
  -- foreign key enforced here — table is not partitioned

  category        TEXT,
  -- broad thematic grouping for UI browsing and filtering
  -- values: 'economics', 'ai_readiness',
  --         'digital_infrastructure', 'innovation',
  --         'productivity', 'human_capital'

  subcategory     TEXT,
  -- finer grouping within category
  -- e.g. 'growth', 'trade', 'labor', 'monetary'

  unit            TEXT,
  -- unit of measurement
  -- e.g. 'current USD', '% of population', '0-100 score'

  description     TEXT,
  -- full description of what this metric measures
  -- sourced from original source documentation

  frequency       TEXT      NOT NULL  DEFAULT 'annual',
  -- temporal frequency of this metric
  -- values: 'annual', 'quarterly', 'monthly',
  --         'biannual', 'weekly'
  -- used by Streamlit to show coverage and frequency filter
  -- needed immediately: WB and IMF publish monthly/quarterly data

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
  -- used by Streamlit to warn about coverage gaps
);


-- ------------------------------------------------------------
-- metadata.countries
-- Master country reference table.
-- One row per country covered by the system.
-- Uses ISO 3166-1 alpha-3 as the universal standard.
-- Fully automated from World Bank API.
-- Source-specific codes stored separately in
-- metadata.country_codes — adding new sources never
-- requires changing this table.
--
-- Edge cases documented in notes:
--   Kosovo    → XKX (informal, not officially ISO-assigned)
--   Taiwan    → TWN (listed separately)
--   Palestine → PSE (ISO assigned)
--   Hong Kong → HKG (separate from China in most sources)
-- ------------------------------------------------------------
CREATE TABLE metadata.countries (
  iso3          CHAR(3)   PRIMARY KEY,
  -- ISO 3166-1 alpha-3 code — universal identifier
  -- e.g. 'LBN' for Lebanon, 'FRA' for France

  iso2          CHAR(2),
  -- ISO 3166-1 alpha-2 code for reference
  -- e.g. 'LB', 'FR'

  country_name  TEXT      NOT NULL,
  -- official country name in English

  region        TEXT,
  -- geographic region for aggregation queries
  -- e.g. 'Middle East & North Africa', 'Western Europe'
  -- enables regional comparison queries without extra sources

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
-- Replaces hardcoded wb_code, imf_code, un_code columns
-- that would require ALTER TABLE for every new source.
-- Adding a new source = inserting rows. Zero schema changes.
--
-- Mostly automated:
--   World Bank, IMF, UN → codes pulled from APIs automatically
--   Oxford, WIPO        → use ISO3 directly, auto-generated
--   PWT                 → uses country names, fuzzy matched
-- ------------------------------------------------------------
CREATE TABLE metadata.country_codes (
  iso3       CHAR(3)  NOT NULL
             REFERENCES metadata.countries(iso3),
  -- universal ISO3 identifier
  -- must exist in metadata.countries first

  source_id  TEXT     NOT NULL
             REFERENCES metadata.sources(source_id),
  -- which source uses this code
  -- must exist in metadata.sources first

  code       TEXT     NOT NULL,
  -- source's own identifier for this country
  -- e.g. 'LB' for IMF, '422' for UN, 'LBN' for World Bank
  --      'Lebanon' for PWT (uses country names)

  PRIMARY KEY (iso3, source_id),
  -- one code per country per source

  UNIQUE (source_id, code)
  -- one source code maps to exactly one country
  -- prevents duplicate mappings caused by data entry errors
  -- e.g. prevents both Lebanon and Libya mapping to 'LB' in IMF
);


-- ------------------------------------------------------------
-- metadata.metric_codes
-- Translation dictionary between every source's original
-- indicator code and our standardized metric_id.
-- One row per metric per source.
-- Replaces hardcoded wb_code, imf_code, un_code columns
-- in metadata.metrics.
-- Adding a new source = inserting rows. Zero schema changes.
--
-- Automated from APIs for World Bank and IMF.
-- Extracted from file headers for Oxford, PWT, WIPO.
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
  --      'NGDPD' for IMF GDP
  --      'rtfpna' for PWT TFP

  PRIMARY KEY (metric_id, source_id),
  -- one original code per metric per source

  UNIQUE (source_id, code)
  -- one source code maps to exactly one metric_id
  -- prevents duplicate mappings
);


-- ============================================================
-- STEP 3 — OPS SCHEMA
--
-- Operational monitoring tables.
-- Tracks ingestion runs, researcher queries, and
-- partition lifecycle status.
-- All three tables are small and grow slowly.
-- No partitioning or lifecycle management needed.
-- ============================================================


-- ------------------------------------------------------------
-- ops.pipeline_runs
-- One row per ingestion script execution.
-- Written automatically by every ingestion script.
-- Primary tool for debugging, auditing, and governance.
--
-- Answers key questions:
--   What ran and when?
--   How many rows were processed vs rejected?
--   Did any source fail? What was the error?
--   When was each source last successfully ingested?
--
-- Answers the presentation question:
--   "What happens if a data source goes down?"
--   → Run logged as 'failed' with error message.
--     Other sources continue unaffected.
--     Failed source re-run independently once recovered.
--
-- Size: 6 sources × 1 run/year = 6 rows/year.
-- In 50 years: 300 rows. Completely negligible.
-- ------------------------------------------------------------
CREATE TABLE ops.pipeline_runs (
  run_id          SERIAL     PRIMARY KEY,
  -- auto-incrementing unique identifier

  source_id       TEXT,
  -- which source was ingested e.g. 'world_bank'

  started_at      TIMESTAMP  NOT NULL,
  -- when the ingestion script started executing

  completed_at    TIMESTAMP,
  -- when it finished
  -- null if still running or crashed

  rows_inserted   INTEGER,
  -- rows successfully inserted into standardized.observations

  rows_rejected   INTEGER,
  -- rows that failed validation and were not inserted

  status          TEXT
    CHECK (status IN ('running', 'success', 'failed', 'partial')),
  -- 'running'  → currently executing
  -- 'success'  → all rows processed successfully
  -- 'failed'   → script crashed, no rows inserted
  -- 'partial'  → some rows succeeded, some failed

  error_message   TEXT,
  -- full error message if failed or partial
  -- null if successful

  notes           TEXT
  -- additional context: source format changes,
  -- manual overrides, version changes detected
);


-- ------------------------------------------------------------
-- ops.query_log
-- Tracks every query submitted through the Streamlit app.
-- Written automatically by app on every researcher request.
-- Never visible to researchers — internal system table only.
--
-- Three purposes:
--   1. Build curated views from evidence
--      After sufficient usage, identify top 10 most frequent
--      query patterns and build materialized views for them.
--      Data-driven optimization, not guesswork.
--
--   2. Identify slow queries
--      High response_ms values that repeat are candidates
--      for new indexes or curated views.
--
--   3. Understand researcher behavior
--      Which countries, metrics, year ranges are most popular?
--      Informs future decisions about data to prioritize.
--
-- Size: even at 1000 queries/day = 365,000 rows/year. Tiny.
-- No partitioning or lifecycle management needed.
-- ------------------------------------------------------------
CREATE TABLE ops.query_log (
  query_id        SERIAL     PRIMARY KEY,
  -- auto-incrementing unique identifier

  requested_at    TIMESTAMP  NOT NULL  DEFAULT NOW(),
  -- exact timestamp of the researcher's request

  countries       TEXT[],
  -- array of ISO3 codes requested
  -- e.g. '{LBN,FRA,USA}'

  metric_ids      TEXT[],
  -- array of metric_ids requested
  -- e.g. '{wb.gdp_per_capita,oxford.ai_readiness}'

  year_from       SMALLINT,
  -- start of requested year range

  year_to         SMALLINT,
  -- end of requested year range

  period          TEXT,
  -- frequency requested: 'annual', 'monthly', 'quarterly'

  rows_returned   INTEGER,
  -- number of rows returned after pivoting

  response_ms     INTEGER,
  -- query execution time in milliseconds
  -- high values identify candidates for curated views

  completeness    NUMERIC
  -- average data completeness of result (0.0 to 1.0)
  -- e.g. 0.73 means 73% of requested cells had data
);


-- ------------------------------------------------------------
-- ops.partition_registry
-- Tracks status, size, and location of every year partition.
-- Used by annual maintenance script to know what is hot,
-- what is cold, and what needs to be migrated.
--
-- Populated at setup with one row per partition.
-- Updated after ingestion with real sizes.
-- Updated during migration with cold storage location.
--
-- Powers the growth simulation:
--   After ingestion → query this table for real sizes
--   → project 50 years of growth
--   → find when Supabase tier limit is exceeded
--   → set hot/cold boundary based on evidence not guesswork
-- ------------------------------------------------------------
CREATE TABLE ops.partition_registry (
  partition_name    TEXT      PRIMARY KEY,
  -- e.g. 'y_1960s', 'y_2020'

  year_from         SMALLINT  NOT NULL,
  -- first year covered by this partition
  -- e.g. 1960 for y_1960s, 2020 for y_2020

  year_to           SMALLINT  NOT NULL,
  -- last year covered by this partition (exclusive)
  -- e.g. 1970 for y_1960s, 2021 for y_2020

  status            TEXT      NOT NULL  DEFAULT 'hot'
    CHECK (status IN ('hot', 'cold', 'migrating')),
  -- hot       → lives in Supabase PostgreSQL, fast queries
  -- cold      → lives as Parquet on Backblaze, slower queries
  -- migrating → currently being moved, do not query directly

  storage_location  TEXT,
  -- null if hot
  -- Backblaze B2 path if cold
  -- e.g. 's3://your-bucket/silver/y_1960s.parquet'

  size_bytes        BIGINT,
  -- actual measured size after ingestion
  -- populated by measurement script post-ingestion
  -- used in growth simulation

  row_count         BIGINT,
  -- number of rows in this partition
  -- populated by measurement script post-ingestion

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
-- Adding new sources or metrics = zero schema changes.
-- Missing data has no row — absence means absence of data.
-- No NULL values ever stored in value column.
--
-- value as TEXT: stores both numeric and categorical values.
-- value_numeric computed column handles all math automatically.
-- Enforcement of numeric validity in transformation scripts.
--
-- No quality_flag: removed entirely. Sources like World Bank
-- and IMF maintain their own quality control. As a
-- harmonization platform our role is faithful representation
-- of source data, not editorial judgment on its validity.
-- Researchers apply their own domain-specific quality criteria.
--
-- Partitioned by year: enables partition pruning for fast
-- year-range queries and hot/cold lifecycle management.
--
-- Country level only: subnational data routes automatically
-- to MongoDB via transformation scripts. This table will
-- never contain subnational rows.
--
-- Foreign key limitation: PostgreSQL does not support outbound
-- foreign key constraints from partitioned tables. Referential
-- integrity for country_iso3, metric_id, and source_id is
-- enforced in transformation scripts by validating against
-- metadata tables before every insert.
-- ============================================================


-- ------------------------------------------------------------
-- standardized.observations
-- The main data table.
-- Every row = one measurement:
--   one country × one year × one period × one metric
-- All sources reduced to the same shape.
-- ------------------------------------------------------------
CREATE TABLE standardized.observations (

  country_iso3  CHAR(3)   NOT NULL,
  -- ISO 3166-1 alpha-3 country code
  -- validated against metadata.countries in transformation scripts
  -- e.g. 'LBN', 'FRA', 'USA'
  -- never subnational — subnational routes to MongoDB

  year          SMALLINT  NOT NULL,
  -- 4-digit year of the observation e.g. 2020
  -- determines which physical partition this row goes into

  period        TEXT      NOT NULL  DEFAULT 'annual',
  -- temporal granularity within the year
  -- 'annual'    → full year observation (default)
  -- 'Q1'-'Q4'   → quarterly observations
  -- 'M01'-'M12' → monthly observations
  -- 'H1'-'H2'   → biannual observations
  -- 'W01'-'W52' → weekly observations
  -- required because World Bank and IMF publish
  -- monthly and quarterly data alongside annual indicators

  metric_id     TEXT      NOT NULL,
  -- standardized metric identifier from metadata.metrics
  -- naming convention: source_prefix.snake_case_name
  -- e.g. 'wb.gdp_per_capita', 'oxford.ai_readiness'
  -- validated against metadata.metrics in transformation scripts

  value         TEXT,
  -- actual data value stored as TEXT for flexibility
  -- stores numeric ('8214.5') and categorical ('Civil Law')
  -- absence of row = absence of data
  -- no NULL rows ever inserted — if source has no data,
  -- no row is created for that country-year-period-metric
  -- use value_numeric for all mathematical operations

  value_numeric NUMERIC
    GENERATED ALWAYS AS (
      CASE
        WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC
        ELSE NULL
      END
    ) STORED,
  -- computed column: automatically extracts numeric value
  -- null if value is non-numeric (categorical data)
  -- PostgreSQL manages this — never insert manually
  -- use this column for all aggregations, math, comparisons

  source_id     TEXT      NOT NULL,
  -- which source this observation came from
  -- validated against metadata.sources in transformation scripts

  unit          TEXT,
  -- unit of measurement at time of ingestion
  -- stored for provenance alongside value

  retrieved_at  DATE      NOT NULL,
  -- date this value was pulled from the source
  -- critical for audit trail and revision detection

  PRIMARY KEY (country_iso3, year, period, metric_id)
  -- four-part composite primary key
  -- uniquely identifies every observation
  -- who (country) + when (year + period) + what (metric)
  -- also serves as index for direct lookups on all four columns

) PARTITION BY RANGE (year);
-- partitioned by year so each year's data is physically separate
-- enables partition pruning: queries filtered by year only scan
-- relevant partitions
-- enables hot/cold tiering: old partitions migrate to Parquet
-- on Backblaze B2 and re-attach as foreign tables
-- all queries remain transparent regardless of storage tier


-- ============================================================
-- STEP 5 — YEAR PARTITIONS
--
-- Partition strategy:
--   1950–2009 → 6 decade partitions (old data, cold migration
--               by decade, fewer partitions, less overhead)
--   2010–2030 → 21 year partitions (recent + projected data,
--               precise annual control for hot/cold decisions)
--   Total: 27 partitions
--
-- No default partition — annual maintenance script always
-- creates the new year partition before ingestion runs,
-- guaranteeing every row has a home. Order is hardcoded:
--   Step 1: create new partition
--   Step 2: run ingestion
--
-- Partitions created by Python script, not manual SQL:
--   for year in range(1950, 2031):
--       CREATE TABLE IF NOT EXISTS standardized.y_{year}
--       PARTITION OF standardized.observations
--       FOR VALUES FROM ({year}) TO ({year+1});
--
-- Coverage rationale:
--   Penn World Tables → from 1950
--   World Bank WDI   → from 1960
--   UN Stats         → from 1970
--   Oxford GAIRI     → from 2019
--   IMF projections  → up to 2030
--
-- Hot/cold boundary:
--   Determined AFTER ingestion by measuring real partition sizes
--   and running 50-year growth simulation.
--   Not decided upfront — evidence-based decision.
-- ============================================================

-- Decade partitions (1950–2009)
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

-- Individual year partitions (2010–2030)
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
-- partitions including new ones created in the future.
--
-- Composite indexes chosen over single-column indexes based
-- on actual researcher query patterns:
--
-- Query pattern 1: researcher picks country + metric
--   "give me AI readiness for Lebanon across all years"
--   → idx_country_metric covers both filters together
--
-- Query pattern 2: one metric across all countries
--   "give me GDP per capita globally 2015-2020"
--   → idx_metric_year covers metric + year range together
--
-- Query pattern 3: source auditing
--   "show me everything from World Bank"
--   → idx_source (single column, source alone is sufficient)
--
-- Query pattern 4: frequency filtering
--   "show me only annual data"
--   → idx_period (single column, period alone is sufficient)
--
-- Primary key already covers direct lookups on
-- (country_iso3, year, period, metric_id) combined.
--
-- Trade-off: more indexes = slightly more storage and slower
-- writes. At annual ingestion frequency write speed is
-- irrelevant. Read speed is everything.
-- ============================================================

CREATE INDEX idx_country_metric
  ON standardized.observations (country_iso3, metric_id);
-- most common researcher query pattern
-- covers combined country + metric filter efficiently

CREATE INDEX idx_metric_year
  ON standardized.observations (metric_id, year);
-- analytical queries scanning one metric across all countries
-- covers the 5 required analytical queries heavily

CREATE INDEX idx_source
  ON standardized.observations (source_id);
-- source auditing and data governance queries

CREATE INDEX idx_period
  ON standardized.observations (period);
-- frequency filtering in Streamlit UI


-- ============================================================
-- STEP 7 — OBSERVATION REVISIONS TABLE
--
-- Tracks when source data changes between annual ingestion runs.
-- Sources like World Bank silently revise historical data.
-- Without this table those revisions are permanently lost.
--
-- How it works:
--   Before every upsert, transformation scripts compare
--   incoming value to stored value. If different, old value
--   is logged here before being overwritten.
--   Main observations table always holds current value.
--   This table holds complete revision history.
--
-- Revision detection strategy per source:
--   World Bank → API lastUpdated parameter
--                pull only records modified since last_retrieved
--   IMF        → re-pull last 5 years annually, compare
--   Oxford     → compare full annual XLSX to stored values
--   PWT        → full compare when new version detected
--   UN/WIPO    → re-pull last 5 years annually, compare
--
-- Why researchers care:
--   A paper written in 2024 using the original value should
--   be reproducible in 2030. Revision history enables
--   point-in-time reconstruction of the dataset.
--   Critical for academic reproducibility.
--
-- Size: revisions are rare. A few hundred rows per year max.
-- No partitioning needed.
-- ============================================================

CREATE TABLE standardized.observation_revisions (
  revision_id    SERIAL     PRIMARY KEY,
  -- auto-incrementing unique identifier

  country_iso3   CHAR(3)    NOT NULL,
  -- country whose data was revised

  year           SMALLINT   NOT NULL,
  -- historical year that was revised
  -- not the year revision was detected

  period         TEXT       NOT NULL  DEFAULT 'annual',
  -- period of the revised observation

  metric_id      TEXT       NOT NULL,
  -- which metric was revised

  old_value      TEXT,
  -- value as stored before this revision
  -- TEXT to match observations.value

  new_value      TEXT,
  -- value as reported by source after revision
  -- TEXT to match observations.value

  revised_at     DATE       NOT NULL,
  -- date revision was detected during ingestion
  -- not when source published the revision

  source_id      TEXT       NOT NULL,
  -- which source published the revised value

  revision_note  TEXT
  -- context about the revision
  -- e.g. 'World Bank methodology update April 2027'
  --      'IMF WEO October 2026 revision'
);


-- ============================================================
-- STEP 8 — CURATED SCHEMA
--
-- Empty at launch. Three types of objects added over time:
--
-- 1. REGULAR VIEWS
--    Saved SQL shortcuts with no storage cost.
--    Use when query is already fast — just a convenience.
--    e.g. filter observations to annual data only
--
-- 2. MATERIALIZED VIEWS
--    Pre-computed results stored on disk. Fast reads.
--    Refreshed annually after ingestion.
--    Built for top 10 most frequent query patterns
--    identified from ops.query_log after 3-6 months usage.
--    Selection criteria: metric combination + year range
--    appearing most frequently in query_log.
--
-- 3. SUMMARY TABLES
--    Pre-aggregated analytical results.
--    Full control over updates — populated by maintenance script.
--    Can be indexed and partitioned independently.
--    Used for: regional averages, income group comparisons,
--    global trends, rankings.
--    Powers the 5 required analytical queries and
--    Streamlit visualizations.
--
-- Process for creating curated objects:
--   1. Analyze ops.query_log after 3-6 months
--   2. Identify top 10 metric+year combinations by frequency
--   3. Build materialized views for those patterns
--   4. Identify common aggregation needs from analytical queries
--   5. Build summary tables for those aggregations
--   6. Add refresh commands to annual maintenance script
--
-- Note: curated schema intentionally empty here.
-- Objects are data-driven decisions, not upfront assumptions.
-- "We don't pre-build views arbitrarily. We track query
-- patterns and build views from evidence."
-- ============================================================


-- ============================================================
-- STEP 9 — DATABASE ROLES
--
-- Two roles implementing the principle of least privilege.
-- Each role gets exactly the permissions it needs, no more.
--
-- pipeline_writer: ingestion and transformation scripts.
--   Read and write observations, revisions, metadata.
--   Insert pipeline run logs and partition registry updates.
--
-- app_reader: Streamlit researcher application.
--   Read only on all data tables.
--   One exception: INSERT on ops.query_log to track usage.
--   Cannot read query_log (researcher privacy).
--   Cannot modify any actual data under any circumstances.
--
-- Neither role has DELETE permissions.
-- Data is never deleted — only upserted or migrated to cold.
-- Enforced at database level, not just by convention.
-- ============================================================

CREATE ROLE pipeline_writer;

GRANT USAGE ON SCHEMA standardized, metadata, raw, curated, ops
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
  ON ALL SEQUENCES IN SCHEMA raw
  TO pipeline_writer;

GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA metadata
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