# 🗄 Database

Shared infrastructure: connection management, base classes for ingestion and transformation, quality checks, B2 storage wrapper, alerting, ops utilities, and seed scripts.

---

## Schemas

The Postgres database is split into three logical schemas, each with a single responsibility.

### `metadata.*` — reference data

Shared dimensions used across all sources. Updated by seed scripts and by the coverage task.

| Table | Purpose |
|---|---|
| `countries` | ~200 ISO3 countries with name, region, income group |
| `metrics` | 383 canonical metrics + coverage stats (year span, country count, observations, missing rate) |
| `sources` | 7 source providers + `last_retrieved` timestamp |
| `country_codes` | source → ISO3 cross-walk (e.g. WIPO uses ISO2, IMF uses mixed codes) |
| `metric_codes` | source → canonical `metric_id` cross-walk |

### `standardized.*` — silver layer

The cleaned, queryable data. This is what the Streamlit app reads.

| Table | Purpose |
|---|---|
| `observations` | 3M+ rows · PK `(country_iso3, year, period, metric_id)` |
| `observation_revisions` | history of value changes when sources retroactively update past data |

### `ops.*` — audit & operations

Every pipeline action is logged here. 10-day retention in Supabase, then migrated to B2 by `retire_ops.py`.

| Table | Purpose |
|---|---|
| `pipeline_runs` | one row per pipeline execution · status, started/completed timestamps, totals |
| `checkpoints` | one row per batch per stage · status (`in_progress` / `complete`), checksum, row count — enables restart-after-crash |
| `quality_runs` | every quality check execution · stage, check_name, severity, passed, expected vs actual |
| `rejection_summary` | rejected rows aggregated by reason (FK violation, empty string, year out of range, duplicate PK, etc.) |
| `metadata_changes` | drift events awaiting human review |

Full schema in [`schema.sql`](../schema.sql) at the project root.

---

## Base classes

The two abstract base classes that source-specific scripts inherit from. They handle the entire pipeline lifecycle so source code only implements the four-or-so methods that differ.

### `base_ingestor.py` · `BaseIngestor`

Subclasses implement:

| Method | Purpose |
|---|---|
| `fetch(batch_unit, since_date)` | pull raw data from the source · return `(raw_row_count, df)` |
| `serialize(df)` / `deserialize(bytes)` | convert between DataFrame and the on-disk B2 format |
| `get_b2_key(batch_unit)` | object key naming convention |
| `get_batch_units(since_date)` | list of batch units to process this run |
| `fetch_metric_metadata(metric_id)` | for unknown-metric detection emails |

The base class then orchestrates: open run, detect new metrics, skip done batches, per-batch fetch → quality checks → B2 upload → checksum verify → checkpoint complete, flush rejections, close run, update `last_retrieved`.

### `base_transformer.py` · `BaseTransformer`

Subclasses implement:

| Method | Purpose |
|---|---|
| `parse(df_raw)` | standardize a raw B2 DataFrame into the canonical observation shape |
| `get_batch_units()` | list of batch units to transform (read from `ops.checkpoints`) |
| `get_b2_key(batch_unit)` | reconstruct the same key the ingestor used |
| `deserialize(bytes)` | parse B2 bytes back into a DataFrame |

The base class handles: open run, first-run detection (skip revision/read-back when silver is empty), per-batch download → checksum verify → parse → row-level checks → revision detection → multi-row VALUES upsert → read-back → checkpoint complete, zero-rows guard, close run.

---

## Utility scripts

### Pipeline core

| Script | Purpose |
|---|---|
| `connection.py` | SQLAlchemy engine factory · reads `DATABASE_URL` |
| `b2_upload.py` | Backblaze B2 client wrapper (S3-compatible API) — `upload`, `download`, `list` |
| `checkpoint.py` | `write_start`, `write_complete`, `get_completed_batches`, `get_ingestion_checksum` |
| `quality_checks.py` | all check functions (`check_ingestion_pre`, `check_ingestion_post`, `check_transformation_pre`, `check_pre_upsert`, `check_transformation_post`) + `compute_checksum` |
| `email_utils.py` | SMTP wrapper · `send_critical_alert`, `send_email` |
| `metadata_drift.py` | detect upstream changes to metric units / frequencies |

### Operations

| Script | Purpose |
|---|---|
| `calculate_coverage.py` | refresh `metadata.metrics` (year span, country count, observation count, missing rate) |
| `reset_source.py` | wipe a source clean for re-run · clears ops tables + B2 files + optionally silver |
| `retire_ops.py` | migrate ops rows older than 10 days to B2 `logs/` |
| `add_metric.py` | approve a parked unknown metric and replay its parked rows |
| `add_api_source.py`, `add_file_source.py` | scaffold metadata rows for a new source |
| `upload_to_b2.py` | manual file upload + auto-trigger Prefect flow (Oxford / WIPO / PWT) |

### Seeding (run once at setup)

```bash
python3 -m database.seed_country_codes    # populate metadata.country_codes from each source
python3 -m database.seed_metric_codes     # populate metadata.metric_codes
python3 -m database.seed_sources          # 7 source rows
python3 -m database.seed_metrics          # 383 canonical metrics
python3 -m database.seed_countries        # ~200 ISO3 countries
python3 -m database.run_seeds             # convenience: run all the above in order
python3 -m database.validate_seeds        # post-seed sanity check
```

---

## Quality checks deep dive

All quality checks live in `quality_checks.py` and write to `ops.quality_runs` via the shared `log_check` helper. Three severity tiers:

| Severity | Behavior |
|---|---|
| **CRITICAL** | raises `CriticalCheckError` · halts the pipeline · triggers email alert |
| **REJECTION** | drops the offending row · logs to `ops.rejection_summary` · pipeline continues |
| **INFORMATIONAL** | logged only · no halt, no email (used for `metadata_drift_pending_review`) |

### Checksum verification

Three stages, two of which use SHA256:

- `ingestion_pre` — **row count only**. Raw bytes and a parsed DataFrame are not bit-equivalent (parsing drops nulls, casts types), so a checksum is not meaningful here.
- `ingestion_post` — **SHA256 + row count** between the DataFrame just before B2 upload and the same DataFrame re-read from B2. Catches B2 round-trip corruption.
- `transformation_pre` — **SHA256** between the DataFrame downloaded from B2 at transform time and the hash stored in `ops.checkpoints` at ingestion time. The cryptographic link between bronze and silver — catches storage-level corruption between pipeline stages.

Checksums are computed on sorted tuples of `(country_iso3, metric_id, year, value)` so two identical datasets in different row orders produce the same hash.

### Why SHA256

- **Collision-resistant** — 2^256 output space makes accidental collision effectively impossible. Weaker hashes (MD5, CRC32) are not safe for a correctness-critical guarantee.
- **Standard library** — `hashlib.sha256` ships with Python; no external dependency on a core pipeline guarantee.
- **Fast enough** — multi-million-row DataFrames hash in under a second; quality checks add <1% overhead per batch.

---

## Common operations

```bash
# Reset a source completely (clears ops + B2 + silver) — destructive, requires --confirm
python3 -m database.reset_source --source pwt --silver --confirm

# Recalculate coverage stats after a manual data fix
python3 -m database.calculate_coverage --source world_bank

# Migrate old ops rows to B2 (run weekly via cron)
python3 -m database.retire_ops

# Approve a parked unknown metric (after team review)
python3 -m database.add_metric --metric_id wb.new_indicator_id

# Validate that all seed tables are populated correctly
python3 -m database.validate_seeds
```

## File layout

```
database/
├── README.md                   ← you are here
├── connection.py               ← SQLAlchemy engine
├── b2_upload.py                ← Backblaze B2 wrapper
├── base_ingestor.py            ← abstract base class
├── base_transformer.py         ← abstract base class
├── checkpoint.py               ← ops.checkpoints read/write
├── quality_checks.py           ← all check functions + checksum
├── email_utils.py              ← SMTP alerts
├── metadata_drift.py           ← drift detection
├── calculate_coverage.py       ← refresh metadata.metrics
├── reset_source.py             ← per-source teardown
├── retire_ops.py               ← migrate old ops to B2
├── add_metric.py               ← approve parked metric
├── add_api_source.py           ← scaffold new API source
├── add_file_source.py          ← scaffold new file source
├── upload_to_b2.py             ← manual file upload + flow trigger
├── retire_ops.py               ← ops table migration
└── seed_*.py                   ← initial seeding scripts
```
