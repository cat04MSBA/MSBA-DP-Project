<div align="center">

# 🌐 AI & Economic Data Aggregator

**A research platform unifying AI readiness, economic productivity, and innovation metrics from seven international sources into a single queryable dataset.**

*MSBA 305 · American University of Beirut · 2026*

[![Sources](https://img.shields.io/badge/sources-7-63b3ed?style=flat-square)]()
[![Observations](https://img.shields.io/badge/observations-3M+-48bb78?style=flat-square)]()
[![Coverage](https://img.shields.io/badge/coverage-1950–2025-9a75ea?style=flat-square)]()
[![License](https://img.shields.io/badge/license-academic-718096?style=flat-square)]()

</div>

---

## What this is

A production-quality data pipeline that ingests, validates, transforms, and serves cross-source data on AI readiness and economic indicators. Researchers can filter across sources, countries, and time ranges through a Streamlit interface, then export wide-format CSVs for downstream analysis.

The pipeline is **idempotent** (safe to re-run), **checkpointed** (restartable after crashes), **auditable** (every step logged), and **defensive** (multiple quality gates before data lands in the queryable layer).

## Architecture at a glance

```
External sources    →    Orchestration    →    Pipeline           →    Storage              →    App
─────────────────       ──────────────         ──────────────────     ──────────────────         ──────────
World Bank · IMF        Prefect (cron          Ingestion → QC →       Bronze (B2) →               Streamlit
OpenAlex · OECD         + manual)              Transform → QC →       Silver (Supabase)
Oxford · WIPO · PWT                            Coverage
                                                                                                   ▲
                                                                                                   │
Audit (ops schema) and alerting (SMTP) wrap every stage          ────────────────────────────────┘
```

> **Interactive architecture explorer:** open [`docs/architecture.html`](docs/architecture.html) in any browser. Click any box to drill into its internals — orchestration, quality gates, storage layout, audit trail.

## Data sources

| Source | Type | Trigger | Coverage | Metrics |
|---|---|---|---|---|
| World Bank WDI | REST API | Monthly cron | 1960– | 362 economic indicators |
| IMF DataMapper | REST API | Monthly cron | 1950– | 132 macro indicators |
| OpenAlex | REST API | Monthly cron | 1990– | AI publications by country/year |
| OECD MSTI | REST API | Monthly cron | 1981– | R&D + science & tech statistics |
| Oxford GAIRI | `.xlsx` | Manual upload | 2019– | AI readiness index |
| WIPO Patents | `.csv` | Manual upload | 1980– | AI patent filings by origin |
| Penn World Tables | `.dta` | Manual upload | 1950–2023 | Productivity & capital stock |

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Prefect 2.x |
| Compute | Python 3.11 + pandas |
| Bronze storage | Backblaze B2 (S3-compatible) |
| Silver storage | Supabase (managed Postgres) |
| App | Streamlit · Plotly · SQLAlchemy |
| Email | SMTP (Gmail) |

## Project structure

```
MSBA-DP-Project/
├── README.md                  ← you are here
├── schema.sql                 ← full Postgres schema (metadata + standardized + ops)
├── reset.sql                  ← teardown for clean re-init
├── requirements.txt           ← Python dependencies
├── .env.example               ← required environment variables
│
├── ingestion/                 ← per-source ingestion scripts
│   └── README.md              ← how to add a new ingestion source
│
├── transformation/            ← per-source transformation scripts
│   └── README.md              ← how to add a new transformer
│
├── orchestration/             ← Prefect flows
│   ├── pipeline.py            ← master api_flow (monthly cron)
│   └── *_flow.py              ← one per source
│
├── database/                  ← shared infrastructure + utility scripts
│   └── README.md              ← schema, utilities, audit subsystem
│
├── app/                       ← Streamlit application
│   └── README.md              ← page-by-page docs and customization
│
├── data/raw/                  ← local input files (PWT .dta, Oxford .xlsx)
│
├── docs/
│   ├── README.md
│   └── architecture.html      ← interactive architecture explorer
│
└── output_logs/               ← captured logs from past pipeline runs
```

Each major folder has its own `README.md` with focused docs. Start here for the big picture, then dive into a folder when you need detail.

## Quick start

### Prerequisites

- Python 3.11+
- A Supabase project (or any Postgres 15+ instance)
- A Backblaze B2 bucket
- A Gmail account with an app password (for alerts)

### 1. Clone and install

```bash
git clone <your-repo-url>
cd MSBA-DP-Project
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required variables:

```bash
DATABASE_URL=postgresql://user:pass@host:5432/postgres
B2_KEY_ID=...
B2_APPLICATION_KEY=...
B2_BUCKET_NAME=...
B2_BUCKET_REGION=us-east-005
SMTP_SENDER=you@gmail.com
SMTP_PASSWORD=<gmail-app-password>
SMTP_RECIPIENT=team@example.com
```

### 3. Initialize the database

```bash
psql "$DATABASE_URL" -f schema.sql
python3 -m database.seed_country_codes
python3 -m database.seed_metric_codes
python3 -m database.seed_sources
python3 -m database.seed_metrics
python3 -m database.validate_seeds
```

### 4. Run the pipeline (manual trigger)

For all four API sources concurrently:

```bash
python3 -c "from orchestration.pipeline import api_flow; api_flow()"
```

For a single source:

```bash
python3 -c "from orchestration.world_bank_flow import world_bank_flow; world_bank_flow()"
```

For file sources, upload + auto-trigger flow:

```bash
python3 database/upload_to_b2.py --source pwt  --file data/raw/pwt/pwt110.dta
python3 database/upload_to_b2.py --source wipo --file data/raw/wipo/wipo_ai_patents.csv
python3 database/upload_to_b2.py --source oxford --file data/raw/oxford/oxford_2025.xlsx --year 2025
```

> **Note:** all scripts must be run from the project root using `python3 -m module.script_name`.

### 5. Launch the Streamlit app

```bash
streamlit run app/Home.py
```

Open <http://localhost:8501> in your browser.

## Pipeline lifecycle

Every batch follows this sequence:

1. **Trigger** — monthly Prefect cron (APIs) or manual upload (files).
2. **Ingestion** — fetch raw data, parse to DataFrame, verify row count (`ingestion_pre`), serialize and upload to B2, re-download and verify SHA256 checksum (`ingestion_post`), write `complete` checkpoint with verified hash.
3. **Transformation** — download from B2, verify SHA256 against stored hash (`transformation_pre`), parse and map source codes to ISO3, run row-level quality checks (`pre_upsert`: FK validity, empty strings, year range, duplicate PK), detect revisions, upsert into silver, verify read-back (`transformation_post`).
4. **Coverage** — refresh `metadata.metrics` with current statistics (year span, country count, observation count, missing rate).
5. **Summary** — for monthly API runs, send a single team email with per-source status.

Failures at any CRITICAL gate halt the batch, leave the `in_progress` checkpoint visible for debugging, and trigger an alert email. Row-level rejections drop the row and log the reason to `ops.rejection_summary` without halting.

## Quality checks summary

| Stage | Check | Severity | On fail |
|---|---|---|---|
| `ingestion_pre` | row_count_match | CRITICAL | halt + email |
| `ingestion_post` | SHA256 + row_count | CRITICAL | halt + email |
| `transformation_pre` | SHA256 vs stored hash | CRITICAL | halt + email (no retry) |
| `pre_upsert` | FK validity, empty_string, year_range, duplicate_pk | REJECTION | drop row + log |
| `transformation_post` | read-back match | CRITICAL | halt + email |
| zero-rows | total_inserted == 0 | CRITICAL | halt + email |
| metadata_drift | unit/frequency change | INFO → REJECTION | park row pending review |

## Storage layout

**Bronze (Backblaze B2):**

```
bronze/<source>/<metric>_<YYYY-MM-DD>.json    raw API response per metric per ingestion run
parked/<source>/<batch_unit>.json             rows with unknown metrics, awaiting approval
logs/<source>/                                retired ops checkpoints (>10 days old)
```

**Silver + metadata + ops (Supabase Postgres):**

```
standardized.observations                     3M+ rows · PK (country_iso3, year, period, metric_id)
standardized.observation_revisions            value-change history across runs

metadata.countries                            ~200 countries
metadata.metrics                              383 canonical metrics + coverage stats
metadata.sources                              7 providers + last_retrieved
metadata.country_codes                        source ↔ ISO3 cross-walks
metadata.metric_codes                         source ↔ canonical metric cross-walks

ops.pipeline_runs                             one row per pipeline execution
ops.checkpoints                               one row per batch per stage (in_progress / complete)
ops.quality_runs                              every quality check execution logged
ops.rejection_summary                         rejected rows aggregated by reason
```

10-day retention on `ops.*` in Supabase; older rows are migrated to B2 `logs/` by `retire_ops.py`.

## Common operations

| Task | Command |
|---|---|
| Reset a source (clears ops + B2 + optionally silver) | `python3 -m database.reset_source --source <name> [--silver --confirm]` |
| Recalculate coverage stats | `python3 -m database.calculate_coverage --source <name>` |
| Migrate old ops rows to B2 | `python3 -m database.retire_ops` |
| Approve a parked unknown metric | `python3 -m database.add_metric --metric_id <id>` |
| Validate seed completeness | `python3 -m database.validate_seeds` |

## Adding a new source

See [`ingestion/README.md`](ingestion/README.md) and [`transformation/README.md`](transformation/README.md) for step-by-step guides. The base classes (`BaseIngestor`, `BaseTransformer`) handle the entire lifecycle (checkpoints, quality checks, rejections, alerts) — source-specific code is only the four-or-so abstract methods.

## Performance notes

- **Multi-row VALUES INSERT** in `_upsert_chunks` — one round-trip per 1,000-row chunk instead of one per row. Eliminates SSL disconnects under load on Supabase Session Pooler.
- **Per-batch commits** (not per chunk) — keeps Supabase WAL sync count low; fits comfortably within the Small tier's 174 Mbps baseline disk IO.
- **Parameterized VALUES CTEs** in `_readback_from_supabase` and `_detect_revisions` — safe and fast for chunked verification.
- **First-run optimization** — when the silver layer is empty for a source, transformation skips the per-batch revision-detection and read-back queries (saves hundreds of round-trips).
- **Streamlit caching** — page queries cached 2–10 min; charts wrapped in `@st.fragment` so toggling a filter re-renders only that chart, not the entire page.

## Acknowledgments

- **MSBA 305** *Data Processing Frameworks*, American University of Beirut, Spring 2026.
- Data sourced under the open-data licenses of World Bank, IMF, OECD, OpenAlex, WIPO, Oxford Insights, and Penn World Tables.

---

<div align="center">

*Built with Python, Prefect, Supabase, Backblaze B2, and Streamlit.*
*All data publicly available. All code intended for academic use.*

</div>
