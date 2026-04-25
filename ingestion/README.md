# Ingestion

One ingestion script per source. Each subclasses `BaseIngestor` (in [`../database/base_ingestor.py`](../database/base_ingestor.py)) which handles the entire lifecycle — checkpoints, quality checks, B2 upload + verification, rejection logging, alerting. Source-specific code only implements four-or-so methods.

---

## Existing ingestors

| File | Source | Type | Notable |
|---|---|---|---|
| `world_bank_ingest.py` | World Bank WDI | REST API | adaptive year-chunking on large indicators · `DEPRECATED_INDICATORS` blocklist |
| `imf_ingest.py` | IMF DataMapper | REST API | one indicator = one B2 file (no pagination) |
| `openalex_ingest.py` | OpenAlex | REST API | one batch per year · AI-topic filtered |
| `oecd_ingest.py` | OECD MSTI | REST API | SDMX-JSON parsing |
| `oxford_ingest.py` | Oxford GAIRI | XLSX file | one file per year · sheet detection across editions |
| `wipo_ingest.py` | WIPO Patents | CSV file | header detection · ISO2 country codes |
| `pwt_ingest.py` | Penn World Tables | STATA file | one upload → split into 47 per-metric files |

---

## What `BaseIngestor` gives you

When `BaseIngestor.run()` is called, it executes this sequence (you do not implement these):

1. Open `ops.pipeline_runs` row, status=`running`.
2. Compare API indicator list against `metadata.metric_codes` — flag new metrics, send INFO email, park rows for review.
3. Read completed checkpoints (10-day window) — skip batches that already finished, enabling restart-after-crash.
4. For each remaining batch:
   1. `write_start` checkpoint (status=`in_progress`).
   2. Call your `fetch()`.
   3. **Empty-source guard** — if 0 rows, delete the in-progress checkpoint and skip B2 upload (prevents bronze-layer pollution from deprecated metrics).
   4. `check_ingestion_pre` — row count match between raw and parsed.
   5. Call your `serialize()` to produce bytes, upload to B2.
   6. Re-download from B2, call your `deserialize()`.
   7. `check_ingestion_post` — SHA256 + row count between pre-upload and post-download.
   8. `write_complete` with verified checksum and row count.
5. Bulk insert `ops.rejection_summary` for any rejected rows accumulated during the run.
6. Close `ops.pipeline_runs` row with final status.
7. Update `metadata.sources.last_retrieved`.

Source code only implements the source-specific bits.

---

## Adding a new ingestion source

Step-by-step. Suppose you're adding a hypothetical `acme` API.

### 1. Register the source in metadata

```bash
python3 -m database.add_api_source \
    --source_id acme \
    --full_name "ACME Indicators" \
    --base_url https://api.acme.org/v1
```

This creates the row in `metadata.sources` and prints next steps.

### 2. Seed metric codes and country codes

Edit `database/seed_metric_codes.py` and `database/seed_country_codes.py` to add a `seed_acme_*` function for your source. Re-run:

```bash
python3 -m database.seed_metric_codes
python3 -m database.seed_country_codes
```

### 3. Create `ingestion/acme_ingest.py`

```python
from database.base_ingestor import BaseIngestor
import pandas as pd
from datetime import date

class ACMEIngestor(BaseIngestor):
    def __init__(self):
        super().__init__(source_id='acme')
        self.run_date = date.today().isoformat()

    def get_batch_units(self, since_date):
        # Return list of identifiers (strings) — one per API call
        return [...]

    def fetch(self, batch_unit, since_date):
        # Pull from API, parse to DataFrame
        # Must return (raw_row_count: int, df: pd.DataFrame)
        # df columns: country_iso3, year, period, metric_id, value, source_id, retrieved_at
        return raw_count, df

    def serialize(self, df) -> bytes:
        return df.to_json(orient='records').encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        from io import BytesIO
        return pd.read_json(BytesIO(data), orient='records', dtype=False)

    def get_b2_key(self, batch_unit) -> str:
        return f"bronze/acme/{batch_unit}_{self.run_date}.json"

    def fetch_metric_metadata(self, metric_id) -> dict:
        # Used when an unknown metric is detected — returns metadata for the email
        return {
            'metric_id':   metric_id,
            'metric_name': ...,
            'source_id':   'acme',
            'unit':        ...,
            'description': ...,
            'frequency':   'annual',
        }

if __name__ == "__main__":
    ACMEIngestor().run()
```

### 4. Create the matching transformer

See [`../transformation/README.md`](../transformation/README.md) — same pattern for `BaseTransformer`.

### 5. Create `orchestration/acme_flow.py`

Copy an existing flow file (e.g. `orchestration/world_bank_flow.py`), rename the tasks and the source. Three tasks per flow: ingestion, transformation, coverage.

### 6. Wire into `pipeline.py` (only if it's an API source)

Import the new flow's tasks at the top of `orchestration/pipeline.py`, then add them to `api_flow()` so they run on the monthly cron with the others.

### 7. Test

```bash
# Run just the new source's flow first to make sure it works in isolation
python3 -c "from orchestration.acme_flow import acme_flow; acme_flow()"

# Then verify rows landed
python3 -c "
from database.connection import get_engine
from sqlalchemy import text
with get_engine().connect() as c:
    print(c.execute(text(\"SELECT COUNT(*) FROM standardized.observations WHERE source_id='acme'\")).scalar())
"
```

---

## Conventions

- **B2 key format:** `bronze/<source>/<batch_unit>_<YYYY-MM-DD>.json` — date is the run_date (set once at ingestor `__init__` so all files in one run share the same date).
- **DataFrame columns:** `country_iso3`, `year`, `period`, `metric_id`, `value`, `source_id`, `retrieved_at`. Other columns are dropped by quality checks.
- **`metric_id` format:** `<source_id>.<metric_code_lowercase>` (e.g. `wb.ny_gdp_pcap_cd`, `imf.ngdp_rpch`).
- **`retrieved_at`** is a string in `YYYY-MM-DD` format — Postgres casts implicitly to DATE on insert.
- **`value`** is a string — preserves source precision exactly. Numeric casting happens in the Streamlit app, not the pipeline.
- **`period`** is one of `annual`, `quarterly`, `monthly`. All current sources are `annual`.
- **Country codes are stored RAW at ingestion time** (e.g. IMF stores `WEO_AFG`, OpenAlex stores `US`). Mapping to ISO3 via `metadata.country_codes` happens in transformation.

## Common patterns

### Adaptive chunking on large APIs

World Bank uses this — try the full year range first, retry 3 times, then split the range in half and recurse. See `_fetch_with_chunking` in `world_bank_ingest.py`.

### Deprecated-indicator blocklist

When a source's metric listing contains codes that the data endpoint says don't exist, add them to a constant set (see `DEPRECATED_INDICATORS` in `world_bank_ingest.py`) and filter them out of `_fetch_api_indicator_codes`. Prevents per-run unknown-metric emails for codes you've already determined are dead.

### Manual upload sources

For Oxford / WIPO / PWT, ingestion reads from `bronze/<source>/source_<date>.<ext>` instead of hitting an API. The file is uploaded by `database/upload_to_b2.py` which then auto-triggers the flow. See `pwt_ingest.py` for the pattern.

## File layout

```
ingestion/
├── README.md               ← you are here
├── world_bank_ingest.py
├── imf_ingest.py
├── openalex_ingest.py
├── oecd_ingest.py
├── oxford_ingest.py
├── wipo_ingest.py
└── pwt_ingest.py
```

Run any ingestor standalone for testing:

```bash
python3 -m ingestion.world_bank_ingest
```

This invokes the `if __name__ == "__main__"` block at the bottom of each file, which calls `<Class>().run()`.
