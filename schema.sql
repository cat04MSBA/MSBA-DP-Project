-- ============================================================
-- DATABASE SCHEMA
-- Project: Global AI & Economic Data Aggregator
-- Course:  MSBA 305 - Data Processing Framework
-- Team:    Haddad, Issa, Fawaz, Salloum, Tabourian
-- Instructor: Dr. Ahmad El-Hajj
-- Last updated: April 2026
-- ============================================================
--
-- HOW TO USE THIS FILE
-- Run this file once on a fresh database to build everything
-- from scratch. It is NOT safe to run on a live database that
-- already has tables — it will crash because CREATE TABLE
-- will conflict with existing tables.
-- To reset a live database, run reset.sql first, then this.
--
-- ARCHITECTURE OVERVIEW
-- This database is the silver layer of a medallion architecture
-- that aggregates economic and AI readiness data from multiple
-- international sources into a standardized, queryable format.
--
-- Three storage layers:
--   Bronze → raw source files on Backblaze B2, never deleted.
--            Every API response and file download is saved here
--            before any transformation. If a transformation bug
--            is found, we re-transform from B2 without hitting
--            the source API again.
--
--   Silver → this database. Standardized observations in
--            Supabase PostgreSQL. One row per country × year ×
--            period × metric. This is what researchers query.
--
--   Curated → pre-built views and summary tables in Supabase,
--             built from evidence in ops.query_log after 3-6
--             months of researcher usage. Empty at launch.
--
-- FOUR SCHEMAS:
--   metadata      → the dictionary of the system. Defines every
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
--                   researcher query logs, partition registry,
--                   checkpoints, quality check results, and
--                   rejection summaries.
--
-- FOREIGN KEYS ON PARTITIONED TABLES:
--   PostgreSQL does not support outbound foreign key constraints
--   from partitioned tables. This means country_iso3, metric_id,
--   and source_id in standardized.observations cannot be enforced
--   at the database level. Instead, the transformation scripts
--   validate these values against metadata tables before every
--   insert. Any row with an unknown value is rejected and logged
--   in ops.rejection_summary.
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
--
-- CHANGES FROM V2 (current):
--   - Added ops.checkpoints (pipeline restart recovery)
--   - Added ops.quality_runs (quality check audit trail)
--   - Added ops.rejection_summary (per-source rejection tracking)
--   - Added grants for all three new tables to both roles
--   - Updated reset.sql to drop all three new tables
-- ============================================================


-- ============================================================
-- STEP 1 — CREATE SCHEMAS
--
-- All four schemas must exist before any tables are created
-- because tables reference their schema in their name.
-- Order does not matter here — schemas have no dependencies
-- between them at creation time.
-- ============================================================

CREATE SCHEMA metadata;
    -- The dictionary layer. Defines every source, country, and
    -- metric in the system. Small tables, fully indexed,
    -- full foreign key enforcement.

CREATE SCHEMA standardized;
    -- The data layer. Contains all observations from all sources
    -- in a single unified structure. Partitioned by year.

CREATE SCHEMA curated;
    -- The pre-built query layer. Empty at launch. Populated
    -- over time based on evidence from ops.query_log.
    -- Contains views, materialized views, and summary tables.

CREATE SCHEMA ops;
    -- The operational layer. Pipeline run logs, researcher
    -- query logs, checkpoints, quality checks, rejections,
    -- and partition lifecycle tracking.


-- ============================================================
-- STEP 2 — METADATA SCHEMA
--
-- The dictionary of the entire system.
-- Defines what every code, identifier, and metric means.
-- Populated at setup via seed scripts and updated as sources
-- add new metrics or change their structures.
--
-- All tables here are:
--   - Small (hundreds to low thousands of rows)
--   - Non-partitioned (no need, data volume is tiny)
--   - Fully foreign-key enforced (no partitioning limitation)
--   - Safe to re-seed at any time (all seeds use upsert logic)
--
-- Seeding order (mandatory due to foreign key dependencies):
--   1. seed_sources.py       → no dependencies
--   2. seed_countries.py     → no dependencies
--   3. seed_metrics.py       → depends on sources
--   4. seed_country_codes.py → depends on sources + countries
--   5. seed_metric_codes.py  → depends on sources + metrics
-- ============================================================


-- ------------------------------------------------------------
-- metadata.sources
-- One row per data source.
-- The master registry of all data origins in the system.
-- Every metric and every observation references this table,
-- so it must be populated before anything else.
--
-- Design principle: adding a new data source never requires
-- a schema change. Insert one row here, add rows to
-- country_codes and metric_codes, write the scripts.
-- That is the entire process.
--
-- Current sources (7 confirmed):
--   world_bank  → World Bank WDI (REST API, annual)
--   imf         → IMF DataMapper (REST API, annual)
--   oxford      → Oxford Insights GAIRI (XLSX, annual)
--   pwt         → Penn World Tables (Stata file, irregular)
--   openalex    → OpenAlex AI Publications (REST API, annual)
--   wipo_ip     → WIPO IP Statistics AI Patents (bulk CSV, annual)
--   oecd_msti   → OECD MSTI (SDMX REST API, biannual)
-- ------------------------------------------------------------
CREATE TABLE metadata.sources (
  source_id         TEXT  PRIMARY KEY,
  -- Short unique identifier used as a foreign key throughout
  -- the entire system. Convention: lowercase_underscore.
  -- Examples: 'world_bank', 'imf', 'oxford', 'pwt'

  full_name         TEXT  NOT NULL,
  -- Full human-readable name of the source.
  -- Shown in Streamlit UI and documentation.
  -- Example: 'World Bank World Development Indicators'

  url               TEXT,
  -- Direct URL to the source homepage or API documentation.
  -- Used for the Streamlit data provenance panel.

  access_method     TEXT,
  -- How the ingestion script retrieves data from this source.
  -- Values: 'REST API', 'CSV download', 'XLSX download',
  --         'Stata file download', 'bulk download'

  update_frequency  TEXT,
  -- How often the source publishes new or revised data.
  -- Values: 'annual', 'biannual', 'quarterly', 'monthly',
  --         'continuous', 'every few years'
  -- Determines the cron schedule for API sources.

  license           TEXT,
  -- The data license governing usage and redistribution.
  -- Examples: 'CC-BY 4.0', 'CC-BY-SA 4.0', 'CC0'

  last_retrieved    DATE,
  -- Date of the last successful ingestion run for this source.
  -- Updated by ingestion scripts after each successful run.
  -- Used as the anchor for change detection:
  --   next run pulls from (last_retrieved - 5 years) to catch
  --   retroactive revisions that sources publish silently.
  -- NULL means this source has never been successfully ingested.

  notes             TEXT
  -- Source-specific quirks, format changes, known issues,
  -- and the revision detection strategy for this source.
);


-- ------------------------------------------------------------
-- metadata.metrics
-- One row per metric tracked by the system.
-- The master dictionary of everything we measure.
--
-- Every metric_id used in standardized.observations must be
-- registered here first. Transformation scripts enforce this
-- by checking incoming codes against this table before any
-- upsert. Unknown metrics are parked and an email is sent.
--
-- Naming convention for metric_id:
--   prefix.snake_case_description
--   Prefixes: wb, imf, oxford, pwt, openalex, wipo, oecd
--   Examples: 'wb.ny_gdp_pcap_cd', 'oxford.ai_readiness'
--
-- available_from and available_to are NULL here.
-- Calculated automatically after ingestion by
-- calculate_coverage.py. Seed scripts never touch them.
-- ------------------------------------------------------------
CREATE TABLE metadata.metrics (
  metric_id       TEXT      PRIMARY KEY,
  -- Standardized identifier. Prefix identifies the source.
  -- Examples: 'wb.ny_gdp_pcap_cd', 'imf.ngdp_rpch'

  metric_name     TEXT      NOT NULL,
  -- Human-readable name shown in the Streamlit UI.
  -- Sourced directly from the source API or documentation.

  source_id       TEXT      NOT NULL
                  REFERENCES metadata.sources(source_id),
  -- Which source this metric comes from.
  -- Foreign key enforced at DB level (table is not partitioned).

  category        TEXT,
  -- Broad thematic grouping for browsing and filtering in UI.
  -- Examples: 'Economy & Growth', 'Trade', 'AI Readiness',
  --           'Research & Development', 'AI Research Output'

  unit            TEXT,
  -- Unit of measurement as reported by the source.
  -- Stored once here, joined at query time.
  -- Not stored per-row in observations (avoids millions of
  -- redundant copies of the same string).
  -- Examples: 'current USD', '% of GDP', '0-100 score', 'count'

  description     TEXT,
  -- Full description of what this metric measures.
  -- Sourced from the original source API or documentation.
  -- Never invented — always taken from the source.

  frequency       TEXT      NOT NULL  DEFAULT 'annual',
  -- Temporal frequency of observations for this metric.
  -- Values: 'annual', 'quarterly', 'monthly', 'biannual'

  available_from  SMALLINT,
  -- First year this metric has data. NULL until
  -- calculate_coverage.py runs after ingestion.

  available_to    SMALLINT
  -- Last year this metric has data. NULL until
  -- calculate_coverage.py runs after ingestion.
);


-- ------------------------------------------------------------
-- metadata.countries
-- Master country reference table.
-- One row per country covered by the system.
-- Uses ISO 3166-1 alpha-3 as the universal identifier.
--
-- Why ISO3 and not ISO2?
--   ISO3 is unambiguous. Used by World Bank, IMF, and most
--   international statistical agencies. Natural pivot key.
--
-- Source-specific codes stored in metadata.country_codes.
-- This table never changes when a new source is added.
--
-- Edge cases documented in notes column (5 countries):
--   Kosovo    → XKX (informal, not officially ISO-assigned)
--   Taiwan    → TWN (listed separately by World Bank)
--   Palestine → PSE (listed as West Bank and Gaza)
--   Hong Kong → HKG (separate from mainland China)
--   Macao     → MAC (separate from mainland China)
-- ------------------------------------------------------------
CREATE TABLE metadata.countries (
  iso3          CHAR(3)   PRIMARY KEY,
  -- ISO 3166-1 alpha-3 code. The universal pivot identifier.
  -- Examples: 'LBN' (Lebanon), 'FRA' (France), 'USA'

  country_name  TEXT      NOT NULL,
  -- Official English name. Sourced from World Bank API.

  region        TEXT,
  -- World Bank geographic region. Used for regional comparisons.
  -- Examples: 'Middle East & North Africa', 'Europe & Central Asia'

  income_group  TEXT,
  -- World Bank income classification. Used for income comparisons.
  -- Values: 'Low Income', 'Lower Middle Income',
  --         'Upper Middle Income', 'High Income'

  notes         TEXT
  -- Documents edge cases. Only populated for the 5 above.
  -- All other countries have NULL notes.
);


-- ------------------------------------------------------------
-- metadata.country_codes
-- Translation dictionary: source country identifier → ISO3.
-- One or more rows per country per source.
--
-- Why this exists:
--   Every source uses different identifiers for the same country.
--   World Bank uses 'LBN', IMF uses 'LB', PWT uses the name
--   'Lebanon', OpenAlex uses ISO2 'LB'. This table lets any
--   transformation script translate any source code to ISO3
--   in one lookup without hardcoded logic.
--
-- WHY THE PRIMARY KEY INCLUDES code (not just iso3 + source_id):
--   Some sources use different name variants for the same country
--   across editions. Oxford is the primary example — it uses both
--   'Antigua & Barbuda' (2019 edition) and 'Antigua and Barbuda'
--   (2022 edition) for the same country (ATG). With a primary key
--   of (iso3, source_id) only one variant could be stored, causing
--   the other to fail the country lookup at ingestion time.
--   Including code in the primary key allows ALL variants to be
--   stored. The ingestion script builds a {code: iso3} dict from
--   all rows for that source — any variant resolves correctly.
--   The UNIQUE (source_id, code) constraint is preserved to prevent
--   one source code from mapping to two different countries.
--
-- Design principle: adding a new source = new rows only.
-- Zero schema changes ever.
-- ------------------------------------------------------------
CREATE TABLE metadata.country_codes (
  iso3       CHAR(3)  NOT NULL
             REFERENCES metadata.countries(iso3),
  -- The universal ISO3 identifier this code maps to.

  source_id  TEXT     NOT NULL
             REFERENCES metadata.sources(source_id),
  -- Which source uses this code.

  code       TEXT     NOT NULL,
  -- The source's own identifier for this country.
  -- Examples: 'LB' (IMF), 'LBN' (World Bank),
  --           'Lebanon' (PWT), 'LB' (OpenAlex/WIPO ISO2)
  -- For Oxford: may have multiple rows per country to cover
  -- name variants across editions (e.g. 'Antigua & Barbuda'
  -- and 'Antigua and Barbuda' both mapping to ATG).

  PRIMARY KEY (iso3, source_id, code),
  -- iso3 + source_id + code uniquely identifies each row.
  -- Allows multiple name variants per country per source.

  UNIQUE (source_id, code)
  -- One source code maps to exactly one country.
  -- Prevents accidental duplicate mappings across countries.
);


-- ------------------------------------------------------------
-- metadata.metric_codes
-- Translation dictionary: source indicator code → metric_id.
-- One row per metric per source.
--
-- Why this exists:
--   Same reason as country_codes. World Bank calls GDP per capita
--   'NY.GDP.PCAP.CD', IMF calls it 'NGDPD', PWT calls it 'rgdpe'.
--   This table lets transformation scripts translate any source
--   code to the correct metric_id without hardcoded logic.
--
-- Design principle: adding a new source = new rows only.
-- Zero schema changes ever.
-- ------------------------------------------------------------
CREATE TABLE metadata.metric_codes (
  metric_id  TEXT  NOT NULL
             REFERENCES metadata.metrics(metric_id),
  -- Our standardized metric identifier.

  source_id  TEXT  NOT NULL
             REFERENCES metadata.sources(source_id),
  -- Which source uses this original code.

  code       TEXT  NOT NULL,
  -- The source's original indicator code.
  -- Examples: 'NY.GDP.PCAP.CD' (World Bank),
  --           'NGDPD' (IMF), 'rtfpna' (PWT)

  PRIMARY KEY (metric_id, source_id),
  -- One original code per metric per source.

  UNIQUE (source_id, code)
  -- One source code maps to exactly one metric_id.
);


-- ============================================================
-- STEP 3 — OPS SCHEMA (EXISTING TABLES)
--
-- Operational monitoring tables.
-- Everything the pipeline writes about itself goes here.
-- Researchers do not see this schema directly.
-- Used by the team for debugging, auditing, and governance.
-- ============================================================


-- ------------------------------------------------------------
-- ops.pipeline_runs
-- One row per ingestion or transformation script execution.
-- Written automatically by every pipeline script.
-- Primary audit trail for the entire system.
--
-- Status lifecycle:
--   'running'               → inserted at script start
--   'success'               → updated at end, zero rejections
--   'success_with_rej_rows' → updated at end, some rejections
--   'failed'                → updated when script crashes
-- ------------------------------------------------------------
CREATE TABLE ops.pipeline_runs (
  run_id          SERIAL     PRIMARY KEY,
  -- Auto-incrementing ID. Referenced by ops.checkpoints,
  -- ops.quality_runs, and ops.rejection_summary to link all
  -- records from one run together.

  source_id       TEXT,
  -- Which source was being processed.
  -- NULL only for maintenance runs (retire_ops.py).

  started_at      TIMESTAMP  NOT NULL,
  -- When the script started executing.

  completed_at    TIMESTAMP,
  -- When the script finished.
  -- NULL if still running or crashed before completion log.

  rows_inserted   INTEGER,
  -- Total rows successfully inserted into standardized.observations.

  rows_rejected   INTEGER,
  -- Total rows that failed validation and were not inserted.
  -- Detail is in ops.rejection_summary.

  status          TEXT
    CHECK (status IN (
      'running',
      'success',
      'success_with_rej_rows',
      'failed'
    )),

  error_message   TEXT,
  -- Full error or rejection summary. NULL if status = 'success'.

  notes           TEXT
  -- Additional context: format changes, manual overrides, retries.
);


-- ------------------------------------------------------------
-- ops.query_log
-- Tracks every query submitted through the Streamlit app.
-- Written by the app on every researcher request. Internal only.
--
-- Three purposes:
--   1. Build curated views from evidence — identify frequent
--      query patterns after 3-6 months of usage.
--   2. Identify slow queries (>10s = curated view candidate).
--   3. Understand researcher behavior (popular metrics, etc).
--
-- Privacy: app_reader has INSERT only, not SELECT.
-- The app logs queries but cannot read other researchers' queries.
-- ------------------------------------------------------------
CREATE TABLE ops.query_log (
  query_id        SERIAL     PRIMARY KEY,

  requested_at    TIMESTAMP  NOT NULL  DEFAULT NOW(),

  countries       TEXT[],
  -- Array of ISO3 codes requested. Example: '{LBN,FRA,USA}'

  metric_ids      TEXT[],
  -- Array of metric_ids requested.

  year_from       SMALLINT,
  year_to         SMALLINT,

  period          TEXT,
  -- Frequency requested: 'annual', 'monthly', 'quarterly'

  rows_returned   INTEGER,

  response_ms     INTEGER,
  -- Query execution time in milliseconds.
  -- Queries > 10,000ms are curated view candidates.

  completeness    NUMERIC
  -- Average data completeness (0.0 to 1.0).
  -- Shown to researcher as coverage warning when below threshold.
);


-- ------------------------------------------------------------
-- ops.partition_registry
-- Tracks status, size, and location of every year partition.
-- Used by retire_ops.py to determine the hot/cold boundary.
--
-- Hot/cold decision is deferred until after full ingestion.
-- Real partition sizes are measured, a 50-year growth simulation
-- is run, and the boundary is set based on evidence.
-- ------------------------------------------------------------
CREATE TABLE ops.partition_registry (
  partition_name    TEXT      PRIMARY KEY,
  -- Examples: 'y_1960s', 'y_2020', 'y_2024'

  year_from         SMALLINT  NOT NULL,
  -- First year covered by this partition (inclusive).

  year_to           SMALLINT  NOT NULL,
  -- Last year covered (exclusive, PostgreSQL convention).

  status            TEXT      NOT NULL  DEFAULT 'hot'
    CHECK (status IN ('hot', 'cold', 'migrating')),
  -- 'hot'       → in Supabase PostgreSQL, fast queries
  -- 'cold'      → Parquet on Backblaze B2, slower queries
  -- 'migrating' → being moved, do not query directly

  storage_location  TEXT,
  -- NULL if hot. B2 path if cold.
  -- Example: 's3://bucket/silver/y_1960s.parquet'

  size_bytes        BIGINT,
  -- Measured partition size. Input to hot/cold simulation.

  row_count         BIGINT,
  -- Row count. Input to hot/cold simulation.

  migrated_at       DATE
  -- When this partition was moved to cold storage. NULL if hot.
);


-- ============================================================
-- STEP 4 — OPS SCHEMA (NEW TABLES)
--
-- Three new operational tables required by the full pipeline.
-- All three reference ops.pipeline_runs via run_id so that
-- every record can be traced back to the exact run that
-- produced it.
-- ============================================================


-- ------------------------------------------------------------
-- ops.checkpoints
-- Records the completion of each unit of work within a pipeline
-- run, along with a SHA256 checksum of the data at that point.
--
-- WHY THIS TABLE EXISTS — TWO REASONS:
--
-- 1. RESTART RECOVERY
--    If a script crashes halfway through 400 World Bank
--    indicators, the restart reads this table to find the last
--    completed batch_unit and resumes from the next one.
--    Without checkpoints, every crash forces a full restart.
--
-- 2. TRANSMISSION INTEGRITY
--    The checksum stored at stage='ingestion_batch' is read
--    by the transformation script at stage='transformation_pre'.
--    If the checksums differ, the data was corrupted on B2
--    between the two stages. The pipeline halts immediately.
--
-- HOW THE CHECKSUM IS COMPUTED:
--    SHA256 of sorted tuples: (country_iso3, metric_id, year, value)
--    Sorted before hashing to be order-independent — two identical
--    datasets in different row orders produce the same checksum.
--
-- CHECKPOINT GRANULARITY (by source):
--    World Bank  → per topic per year (e.g. 'topic_1_2020')
--    IMF         → per year (e.g. '2020')
--    OpenAlex    → per year (e.g. '2020')
--    OECD        → per year (e.g. '2020')
--    Oxford      → per file year (e.g. '2023')
--    WIPO        → per full file ('full_file')
--    PWT         → per full file ('full_file')
--
-- RETENTION: 10 days in Supabase, then migrated to B2 by retire_ops.py.
--    10 days covers any realistic restart window. After 10 days
--    a full re-run is performed rather than restarting from checkpoint.
-- ------------------------------------------------------------
CREATE TABLE ops.checkpoints (
  checkpoint_id   SERIAL     PRIMARY KEY,

  run_id          INTEGER    NOT NULL
                  REFERENCES ops.pipeline_runs(run_id),
  -- Which pipeline run this checkpoint belongs to.
  -- Used by restart logic to find all checkpoints for the
  -- current run and determine where to resume.

  source_id       TEXT       NOT NULL,
  -- Stored directly (not just via run_id join) to make
  -- restart queries simpler — no join needed.

  stage           TEXT       NOT NULL
    CHECK (stage IN (
      'ingestion_batch',
      'ingestion_complete',
      'transformation_batch'
    )),
  -- 'ingestion_batch'      → one batch successfully landed on B2
  --                          and verified. Written per batch_unit.
  -- 'ingestion_complete'   → ALL batches for this source are on B2
  --                          and verified. Written once per run.
  --                          This is the signal that transformation
  --                          is safe to start.
  -- 'transformation_batch' → one batch successfully upserted to
  --                          Supabase and post-upsert verified.

  batch_unit      TEXT       NOT NULL,
  -- Human-readable identifier for this unit of work.
  -- Used by restart logic to determine which batches to skip.
  -- Format: see CHECKPOINT GRANULARITY above.

  checksum        TEXT       NOT NULL,
  -- SHA256 hash of the data at this checkpoint.
  -- Computed on sorted tuples of (country_iso3, metric_id, year, value).
  -- The next pipeline stage re-computes this hash on the same
  -- data and compares. A mismatch = data corrupted = CRITICAL halt.

  row_count       INTEGER    NOT NULL,
  -- Row count at this checkpoint. Secondary verification.
  -- A row_count mismatch alongside a checksum mismatch confirms
  -- rows were added or dropped, not just values changed.

  status          TEXT       NOT NULL
    CHECK (status IN ('complete', 'in_progress')),
  -- 'in_progress' → written at batch start. Detects batches
  --                 that started but never finished (crash).
  -- 'complete'    → written at batch end after verification.

  checkpointed_at TIMESTAMP  NOT NULL  DEFAULT NOW()
  -- Used by retire_ops.py to find checkpoints older than 3 months.
);


-- ------------------------------------------------------------
-- ops.quality_runs
-- Records the result of every quality check performed by the
-- pipeline. One row per check per batch_unit per run.
--
-- WHY THIS TABLE EXISTS:
--    Quality checks happen at multiple pipeline stages. Without
--    this table their results are printed to logs and lost.
--    This table creates a permanent, queryable audit trail.
--    The team can query it to see which checks fail most often,
--    which sources have the most transmission issues, and whether
--    data quality is improving or degrading over time.
--
-- CHECK TYPES, STAGES, AND SEVERITIES:
--
--    check_name            stage               severity
--    ─────────────────────────────────────────────────────
--    checksum_match        ingestion_pre       CRITICAL
--                          ingestion_post      CRITICAL
--                          transformation_pre  CRITICAL
--                          transformation_post CRITICAL
--    row_count_match       (same as above)     CRITICAL
--    pk_uniqueness         pre_upsert          REJECTION
--    fk_validity           pre_upsert          REJECTION
--    empty_string          pre_upsert          REJECTION
--    year_range            pre_upsert          REJECTION
--    metadata_consistency  query               CRITICAL
--
-- CRITICAL vs REJECTION:
--    CRITICAL  → halt pipeline, do not update last_retrieved,
--                send immediate email. Used for transmission
--                integrity failures only.
--    REJECTION → reject individual rows, continue pipeline.
--                Aggregated in ops.rejection_summary.
--    INFORMATIONAL → log only, no pipeline action, no email.
--                    Used for coverage warnings shown to researcher.
--
-- RETENTION: 2 years, then archived to B2 by retire_ops.py.
-- ------------------------------------------------------------
CREATE TABLE ops.quality_runs (
  quality_run_id  SERIAL     PRIMARY KEY,

  run_id          INTEGER    NOT NULL
                  REFERENCES ops.pipeline_runs(run_id),
  -- Which pipeline run this quality check belongs to.

  source_id       TEXT       NOT NULL,
  -- Stored directly to make quality monitoring queries simpler.

  stage           TEXT       NOT NULL
    CHECK (stage IN (
      'ingestion_pre',
      'ingestion_post',
      'transformation_pre',
      'transformation_post',
      'pre_upsert',
      'query'
    )),
  -- 'ingestion_pre'       → raw API response vs parsed DataFrame
  -- 'ingestion_post'      → parsed DataFrame vs B2 write-back
  -- 'transformation_pre'  → B2 read vs stored ingestion checkpoint
  -- 'pre_upsert'          → DataFrame checks before touching DB
  -- 'transformation_post' → Supabase read-back vs pre-upsert DF
  -- 'query'               → metadata consistency at query time

  check_name      TEXT       NOT NULL
    CHECK (check_name IN (
      'checksum_match',
      'row_count_match',
      'pk_uniqueness',
      'fk_validity',
      'empty_string',
      'year_range',
      'metadata_consistency'
    )),
  -- Which specific check this row represents.
  -- checksum_match      → SHA256 hashes match between stages
  -- row_count_match     → row counts match between stages
  -- pk_uniqueness       → no duplicate (country,year,period,metric)
  -- fk_validity         → all country/metric/source codes are known
  -- empty_string        → no row has value = ''
  -- year_range          → all years are within 1950-2030
  -- metadata_consistency→ source_id matches what metadata says

  severity        TEXT       NOT NULL
    CHECK (severity IN ('CRITICAL', 'REJECTION', 'INFORMATIONAL')),
  -- How the pipeline responds to a failure of this check.

  passed          BOOLEAN    NOT NULL,
  -- TRUE = check passed, no problem found.
  -- FALSE = check failed, problem detected.

  expected_value  TEXT,
  -- What the check expected.
  -- For checksum: the stored SHA256 from previous stage.
  -- For row_count: the stored count from previous stage.
  -- For pk_uniqueness: '0 duplicates expected'
  -- Stored as TEXT so numeric and string checks use same column.

  actual_value    TEXT,
  -- What the check actually found.
  -- For checksum: the computed SHA256 of current data.
  -- For pk_uniqueness: number of duplicates found as string.

  batch_unit      TEXT,
  -- Which batch this check applies to.
  -- Matches ops.checkpoints.batch_unit for cross-referencing.
  -- NULL for query-stage checks (no batch concept).

  details         TEXT,
  -- Human-readable explanation of the result.
  -- For failures: what went wrong, which rows, what values.
  -- This text is included verbatim in CRITICAL alert emails.

  checked_at      TIMESTAMP  NOT NULL  DEFAULT NOW()
  -- Used by retire_ops.py to find records older than 2 years.
);


-- ------------------------------------------------------------
-- ops.rejection_summary
-- Aggregates rejected rows by reason per batch per run.
-- One row per rejection reason per batch_unit per run.
--
-- WHY THIS TABLE EXISTS:
--    When a batch has 150 rows with unknown country codes,
--    logging 150 individual rows would be slow and noisy.
--    This table stores one aggregate row: "150 rows rejected
--    for unknown_country_code in batch 2020 of the IMF run."
--    Full row-level detail is written to B2 as a CSV file
--    at /logs/rejections_SOURCE_DATE.csv for human reference.
--
-- REJECTION REASONS:
--    unknown_country_code → source code not in country_codes table
--    unknown_metric_id    → metric code not in metric_codes table
--    year_out_of_range    → year outside 1950-2030
--    duplicate_pk         → duplicate (country, year, period, metric)
--    empty_string         → value field is an empty string ''
--    unparseable_value    → value cannot be cast to expected type
--
-- WHY year_out_of_range IS A PRE-CHECK (not DB constraint):
--    Without a default partition, PostgreSQL raises an ungraceful
--    error if a row's year has no matching partition. Pre-checking
--    gives controlled rejection with logging instead of a crash.
--
-- WHY duplicate_pk IS A PRE-CHECK (not DB constraint):
--    A PK violation on a partitioned table crashes the entire
--    batch. Pre-checking rejects only the duplicate rows while
--    the rest of the batch proceeds normally.
--
-- RETENTION: 2 years, then archived to B2 by retire_ops.py.
-- ------------------------------------------------------------
CREATE TABLE ops.rejection_summary (
  rejection_id      SERIAL     PRIMARY KEY,

  run_id            INTEGER    NOT NULL
                    REFERENCES ops.pipeline_runs(run_id),
  -- Which pipeline run produced these rejected rows.

  source_id         TEXT       NOT NULL,
  -- Stored directly for simpler query joins.

  batch_unit        TEXT       NOT NULL,
  -- Which batch the rejected rows came from.
  -- Matches ops.checkpoints.batch_unit.

  rejection_reason  TEXT       NOT NULL
    CHECK (rejection_reason IN (
      'unknown_country_code',
      'unknown_metric_id',
      'year_out_of_range',
      'duplicate_pk',
      'empty_string',
      'unparseable_value',
      'metadata_drift_pending_review'
      -- metadata_drift_pending_review → rows parked because their
      -- metric's unit or frequency drifted from stored metadata.
      -- Rows are held until a human approves or rejects the drift
      -- via ops.metadata_changes. Added with drift detection system.
    )),
  -- Why these rows were rejected.
  -- Each unique reason gets its own row in this table.
  -- Example: 5 unknown country codes AND 2 out-of-range years
  -- → 2 rows in rejection_summary:
  --     {reason: 'unknown_country_code', row_count: 5}
  --     {reason: 'year_out_of_range',    row_count: 2}

  row_count         INTEGER    NOT NULL,
  -- How many rows were rejected for this reason in this batch.

  logged_at         TIMESTAMP  NOT NULL  DEFAULT NOW()
  -- Used by retire_ops.py to find records older than 2 years.
);


-- ============================================================
-- STEP 4b — ops.metadata_changes (ADDED WITH DRIFT DETECTION)
--
-- Records every detected change in source metadata between runs.
-- One row per field change per entity per run.
--
-- WHY THIS TABLE EXISTS:
--    The seed scripts originally used ON CONFLICT DO UPDATE which
--    silently overwrote metadata (metric units, country codes) when
--    a source changed its reporting conventions. This is dangerous:
--    - A unit change makes historical observations uninterpretable.
--    - A code mapping change could silently remap historical rows
--      to the wrong country.
--    This table intercepts those changes and routes them to a
--    controlled human review process instead of silent overwriting.
--
-- TWO CATEGORIES OF CHANGE (action_taken values):
--    auto_applied     → safe editorial changes (name, description).
--                       Applied immediately, logged here for audit.
--    pending_review   → critical semantic changes (unit, frequency,
--                       any code mapping). NOT applied to metadata.
--                       Human must approve or reject via add_metric.py.
--
-- RELATIONSHIP TO OTHER OPS TABLES:
--    Every drift event also writes to ops.quality_runs with
--    severity='INFORMATIONAL' (auto_applied) or 'CRITICAL'
--    (pending_review). This table stores the before/after values;
--    ops.quality_runs stores the pass/fail outcome. Both are needed
--    for a complete audit trail:
--    - ops.quality_runs answers "did this check pass?"
--    - ops.metadata_changes answers "what exactly changed?"
--
-- RETENTION: 2 years in Supabase, then migrated to B2.
--    Same retention as ops.quality_runs — metadata governance
--    events have the same analytical value over time.
-- ============================================================
CREATE TABLE ops.metadata_changes (
  change_id     SERIAL     PRIMARY KEY,

  run_id        INTEGER    NOT NULL
                REFERENCES ops.pipeline_runs(run_id),
  -- Which pipeline run detected this drift.

  source_id     TEXT       NOT NULL,
  -- Which source's metadata drifted.
  -- Stored directly for simpler query joins.

  entity_type   TEXT       NOT NULL
    CHECK (entity_type IN (
      'metric',
      'country_code',
      'metric_code',
      'source'
    )),
  -- What kind of metadata entity drifted.
  -- 'metric'       → a field in metadata.metrics changed
  -- 'country_code' → a code in metadata.country_codes changed
  -- 'metric_code'  → a code in metadata.metric_codes changed
  -- 'source'       → a field in metadata.sources changed

  entity_id     TEXT       NOT NULL,
  -- The specific entity that drifted.
  -- For metrics:       the metric_id (e.g. 'wb.ny_gdp_pcap_cd')
  -- For country_codes: the iso3 (e.g. 'LBN')
  -- For metric_codes:  the metric_id
  -- For sources:       the source_id

  field_changed TEXT       NOT NULL,
  -- Which specific field changed.
  -- Examples: 'unit', 'frequency', 'metric_name', 'code'

  old_value     TEXT,
  -- The value currently stored in metadata before this change.
  -- NULL if the field had no previous value.

  new_value     TEXT,
  -- The value the source now reports.
  -- What metadata would be updated to if approved.

  detected_at   TIMESTAMP  NOT NULL  DEFAULT NOW(),
  -- When drift was detected. Used by retire_ops.py for archival.

  action_taken  TEXT       NOT NULL
    CHECK (action_taken IN (
      'auto_applied',
      'pending_review',
      'rejected',
      'approved'
    ))
  -- What happened with this drift:
  -- 'auto_applied'   → safe change applied immediately (name/description)
  -- 'pending_review' → critical change parked for human review
  -- 'approved'       → human approved the change via add_metric.py
  -- 'rejected'       → human rejected the change via add_metric.py
);



--
-- The heart of the system. One unified table containing every
-- observation from every source, reduced to one canonical shape:
--   one row = one country × one year × one period × one metric
--
-- KEY DESIGN DECISIONS (and why):
--
-- Tall format (one row per observation, not one column per metric):
--   Adding new sources or metrics = zero schema changes.
--   A wide format would require ALTER TABLE for every new metric.
--
-- value stored as TEXT:
--   Stores both numeric ('8214.5') and categorical ('Civil Law').
--   value_numeric (computed column) extracts the number automatically.
--   Use value_numeric for all math. Use value for display.
--
-- No unit column in observations:
--   Unit stored once in metadata.metrics, joined at query time.
--   Storing it per-row wastes millions of bytes on redundant strings.
--
-- No NULL rows:
--   Absence of a row = absence of data. Cleaner and more efficient
--   than storing NULLs. No row for Lebanon+2005+GDP = no data.
--
-- No quality_flag:
--   Sources maintain their own QC. Our role is faithful transmission.
--
-- Partitioned by year:
--   Enables partition pruning (only relevant partitions scanned)
--   and hot/cold lifecycle management (old decades → B2 Parquet).
--
-- Foreign key limitation:
--   PostgreSQL cannot enforce FK constraints from partitioned tables.
--   country_iso3, metric_id, source_id integrity is enforced by
--   transformation scripts (pre-upsert fk_validity check logged
--   in ops.quality_runs, rejections in ops.rejection_summary).
-- ============================================================


-- ------------------------------------------------------------
-- standardized.observations
-- The main data table. Partitioned by year.
-- ------------------------------------------------------------
CREATE TABLE standardized.observations (

  country_iso3  CHAR(3)   NOT NULL,
  -- ISO 3166-1 alpha-3 code. Validated by transformation scripts.

  year          SMALLINT  NOT NULL,
  -- 4-digit year. Determines which partition receives this row.
  -- Valid range: 1950-2030 (enforced by pre-upsert year_range check,
  -- not by a DB constraint — CHECK constraints cannot overlap
  -- with partition boundaries in PostgreSQL).

  period        TEXT      NOT NULL  DEFAULT 'annual',
  -- Temporal granularity within the year.
  -- 'annual'    → full-year observation (default, most sources)
  -- 'Q1'-'Q4'   → quarterly
  -- 'M01'-'M12' → monthly
  -- 'H1'-'H2'   → biannual

  metric_id     TEXT      NOT NULL,
  -- Standardized metric ID. Validated by transformation scripts.
  -- Examples: 'wb.ny_gdp_pcap_cd', 'oxford.ai_readiness'

  value         TEXT,
  -- Actual data value stored as TEXT.
  -- Numeric: '8214.5', '0.73', '-2.1'
  -- Categorical: 'Civil Law', 'High Income'
  -- Never NULL — no data = no row.
  -- Use value_numeric for all math and comparisons.

  value_numeric NUMERIC
    GENERATED ALWAYS AS (
      CASE
        WHEN value ~ '^-?[0-9]+\.?[0-9]*$' THEN value::NUMERIC
        ELSE NULL
      END
    ) STORED,
  -- Computed column. PostgreSQL automatically extracts the numeric
  -- value from the TEXT value column and stores it to disk (STORED).
  -- NULL if value is non-numeric (categorical data).
  -- NEVER insert into this column — PostgreSQL manages it.
  -- Use for all aggregations, math, filtering, and comparisons.

  source_id     TEXT      NOT NULL,
  -- Which source this observation came from. Validated by scripts.

  retrieved_at  DATE      NOT NULL,
  -- Date this value was pulled from the source (date.today()).
  -- Used for audit trail and revision detection between runs.

  PRIMARY KEY (country_iso3, year, period, metric_id)
  -- Four-part PK uniquely identifies every observation.
  -- This is the target of the ON CONFLICT clause in upserts.

) PARTITION BY RANGE (year);
-- PostgreSQL routes each row to the correct partition by year.


-- ============================================================
-- STEP 6 — YEAR PARTITIONS
--
-- Strategy:
--   1950-2009 → 6 decade partitions (historical, likely cold)
--   2010-2030 → 21 individual year partitions (recent, hot)
--   Total: 27 partitions. No default partition.
--
-- No default partition:
--   A row with year=1945 or year=2031 triggers a PostgreSQL
--   error rather than silently landing in a catch-all partition.
--   This forces the pre-upsert year_range check to reject such
--   rows before they reach the database — controlled failure.
--
-- Annual maintenance:
--   Before each new year's ingestion, a new partition must be
--   created (e.g. y_2031). The orchestration script does this
--   automatically before triggering ingestion.
-- ============================================================

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
-- STEP 7 — INDEXES ON STANDARDIZED.OBSERVATIONS
--
-- Defined on the parent table — PostgreSQL propagates them
-- automatically to all existing and future partitions.
--
-- Each index matches a documented researcher query pattern.
-- Indexes without a matching query pattern were omitted to
-- avoid unnecessary write overhead on every insert.
-- ============================================================

CREATE INDEX idx_country_metric
  ON standardized.observations (country_iso3, metric_id);
  -- Most common pattern: researcher picks country + metric.
  -- Example: Lebanon + GDP per capita + all years.

CREATE INDEX idx_metric_year
  ON standardized.observations (metric_id, year);
  -- Global comparison: one metric across all countries for a year.
  -- Example: AI Readiness score for all countries in 2023.

CREATE INDEX idx_source
  ON standardized.observations (source_id);
  -- Source auditing queries. Not typically used by researchers.
  -- Example: COUNT(*) WHERE source_id = 'world_bank'

CREATE INDEX idx_period
  ON standardized.observations (period);
  -- Frequency filtering in Streamlit UI.
  -- Example: show annual-only data (exclude quarterly).


-- ============================================================
-- STEP 8 — OBSERVATION REVISIONS TABLE
--
-- Records when source data changes between ingestion runs.
-- International sources regularly revise historical values
-- without announcement. Without this table those changes
-- are permanently overwritten and lost.
--
-- How it works:
--   Before every upsert, the transformation script compares
--   the incoming value to the stored value. If they differ,
--   the old value is logged here before being overwritten.
--
-- old_unit / new_unit:
--   When a source changes a metric's unit, the unit change is
--   captured here. Streamlit warns researchers when old_unit ≠
--   new_unit — values before and after the revision date are
--   not directly comparable.
-- ============================================================

CREATE TABLE standardized.observation_revisions (
  revision_id    SERIAL     PRIMARY KEY,

  country_iso3   CHAR(3)    NOT NULL,

  year           SMALLINT   NOT NULL,
  -- The data year that was revised (e.g. 2019), NOT the year
  -- the revision was detected (e.g. 2024 run detected a 2019 change).

  period         TEXT       NOT NULL  DEFAULT 'annual',

  metric_id      TEXT       NOT NULL,

  old_value      TEXT,
  -- Value stored before this revision.

  new_value      TEXT,
  -- Value now reported by the source.

  old_unit       TEXT,
  -- Unit before revision. NULL if unit did not change.

  new_unit       TEXT,
  -- Unit after revision. NULL if unit did not change.

  revised_at     DATE       NOT NULL,
  -- Date the revision was detected (our system's detection date).

  source_id      TEXT       NOT NULL,

  revision_note  TEXT
  -- Optional context: 'methodology update', 'unit change',
  -- 'base year rebased from 2015 to 2021', etc.
);


-- ============================================================
-- STEP 9 — CURATED SCHEMA
--
-- Empty at launch. Populated over time based on evidence from
-- ops.query_log. Nothing pre-built arbitrarily.
--
-- Three types of objects will be added here:
--   1. REGULAR VIEWS — saved SQL shortcuts, no storage cost.
--   2. MATERIALIZED VIEWS — pre-computed results on disk,
--      built for the top-N most frequent query patterns.
--   3. SUMMARY TABLES — pre-aggregated analytical results.
--
-- Rule: "We build views from evidence of what researchers
-- actually query, not from guesses about what they might want."
-- ============================================================


-- ============================================================
-- STEP 10 — DATABASE ROLES AND GRANTS
--
-- Two roles. Principle of least privilege.
-- Neither role has DELETE — data is never deleted.
--
-- pipeline_writer:
--   All ingestion and transformation scripts run as this role.
--   Can read and write observations, revisions, metadata, and
--   all ops tables it needs for logging and checkpointing.
--
-- app_reader:
--   The Streamlit app runs as this role.
--   Read-only on all data. INSERT-only on ops.query_log.
--   Cannot modify any actual data under any circumstances.
-- ============================================================

CREATE ROLE pipeline_writer;

-- Schema access: pipeline_writer must be able to reference
-- tables in all four schemas.
GRANT USAGE ON SCHEMA standardized, metadata, curated, ops
  TO pipeline_writer;

-- Observations: insert and update (upsert). No DELETE.
GRANT SELECT, INSERT, UPDATE
  ON standardized.observations
  TO pipeline_writer;

-- Revisions: log when source data changes.
GRANT SELECT, INSERT, UPDATE
  ON standardized.observation_revisions
  TO pipeline_writer;

-- Pipeline runs: open run at start, update status at end.
-- UPDATE is required to change status from 'running' to final.
GRANT INSERT, SELECT, UPDATE
  ON ops.pipeline_runs
  TO pipeline_writer;

-- Checkpoints: read to find restart point, write after each batch.
GRANT SELECT, INSERT, UPDATE
  ON ops.checkpoints
  TO pipeline_writer;
-- UPDATE needed to change status from 'in_progress' to 'complete'.

-- Quality runs: write check results. Immutable once written.
GRANT SELECT, INSERT
  ON ops.quality_runs
  TO pipeline_writer;

-- Rejection summary: write aggregated rejection counts.
-- Immutable once written.
GRANT SELECT, INSERT
  ON ops.rejection_summary
  TO pipeline_writer;

-- Metadata changes: write drift detections, update action_taken
-- when human approves or rejects via add_metric.py.
-- SELECT needed to read pending_review rows for email formatting.
GRANT SELECT, INSERT, UPDATE
  ON ops.metadata_changes
  TO pipeline_writer;

-- Query log and partition registry: pipeline_writer can update
-- partition sizes and read query log for curated view planning.
GRANT INSERT, SELECT, UPDATE
  ON ops.query_log, ops.partition_registry
  TO pipeline_writer;

-- Metadata: read for validation, update last_retrieved and
-- new auto-discovered metrics.
GRANT SELECT, INSERT, UPDATE
  ON metadata.sources, metadata.metrics, metadata.countries,
     metadata.country_codes, metadata.metric_codes
  TO pipeline_writer;

-- Sequences: required to generate SERIAL primary key values.
GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA ops
  TO pipeline_writer;

GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA standardized
  TO pipeline_writer;


CREATE ROLE app_reader;

-- Schema access.
GRANT USAGE ON SCHEMA standardized, metadata, curated, ops
  TO app_reader;

-- Observations: read-only. No write access.
GRANT SELECT
  ON standardized.observations
  TO app_reader;

-- Revisions: read to show researchers when source data changed.
GRANT SELECT
  ON standardized.observation_revisions
  TO app_reader;

-- Metadata: read to populate Streamlit metric/country pickers
-- and data provenance panel.
GRANT SELECT
  ON metadata.sources, metadata.metrics, metadata.countries,
     metadata.country_codes, metadata.metric_codes
  TO app_reader;

-- Curated schema: read all pre-built views and summary tables.
GRANT SELECT
  ON ALL TABLES IN SCHEMA curated
  TO app_reader;

-- Query log: INSERT only.
-- App logs each researcher query for pipeline monitoring.
-- Cannot SELECT — that would expose one researcher's query
-- history to another. Privacy by design.
GRANT INSERT
  ON ops.query_log
  TO app_reader;

-- Quality runs: read-only.
-- App reads quality check results to populate the data
-- provenance panel shown to researchers.
GRANT SELECT
  ON ops.quality_runs
  TO app_reader;

-- Metadata changes: read-only.
-- App reads drift detection history to show researchers when
-- source metadata changed and what was done about it.
-- This supports the data provenance panel in Streamlit.
GRANT SELECT
  ON ops.metadata_changes
  TO app_reader;

-- No access to: ops.checkpoints (internal restart recovery),
--               ops.rejection_summary (internal operational),
--               ops.pipeline_runs (internal audit trail),
--               ops.partition_registry (internal storage lifecycle).

-- Sequences: app_reader needs ops sequence to insert into
-- ops.query_log (which has a SERIAL primary key).
GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA ops
  TO app_reader;