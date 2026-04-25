# Transformation

One transformer per source. Each subclasses `BaseTransformer` (in [`../database/base_transformer.py`](../database/base_transformer.py)). The base class handles checksum verification, quality checks, revision detection, multi-row VALUES upsert, and read-back verification — source-specific code only standardizes the bronze data into the canonical observation shape.

---

## Existing transformers

| File | Source | Notable |
|---|---|---|
| `world_bank_transform.py` | World Bank | trivial — ingestion already produced canonical shape |
| `imf_transform.py` | IMF | maps source-specific country codes (mixed ISO2 + custom) → ISO3 via `metadata.country_codes` |
| `openalex_transform.py` | OpenAlex | maps ISO2 → ISO3 |
| `oecd_transform.py` | OECD | parses SDMX-style CSV |
| `oxford_transform.py` | Oxford | per-edition column reconciliation (sheet structure changes year-to-year) |
| `wipo_transform.py` | WIPO | parses tall format from CSV |
| `pwt_transform.py` | PWT | reads pre-melted JSON from B2 |

---

## What `BaseTransformer` gives you

When `BaseTransformer.run()` is called, it executes this sequence (you do not implement these):

1. Open `ops.pipeline_runs` row (separate from ingestion's run).
2. **First-run detection** — if zero `transformation_batch` checkpoints exist for this source, set `self._first_run = True` and skip per-batch revision detection + read-back (saves hundreds of round-trips on initial historical loads).
3. Load FK validation sets once (`metadata.countries`, `metadata.metrics`, `metadata.sources`) — passed to per-batch checks.
4. Read completed `transformation_batch` checkpoints — skip already-done batches.
5. For each remaining batch:
   1. `write_start` checkpoint (status=`in_progress`).
   2. Call your `get_b2_key()` → download from B2 → call your `deserialize()`.
   3. `check_transformation_pre` — SHA256 vs the hash stored in `ops.checkpoints` at ingestion time.
   4. Call your `parse()`.
   5. `check_pre_upsert` — FK validity, empty strings, year range, duplicate PK. Rejected rows accumulate in `self.all_rejections`.
   6. (Non-first-run only) `_detect_revisions` — log value changes to `standardized.observation_revisions`.
   7. `_upsert_chunks` — multi-row VALUES INSERT into `standardized.observations` with ON CONFLICT DO UPDATE. One round-trip per 1000-row chunk.
   8. (Non-first-run only) `_readback_from_supabase` + `check_transformation_post` — verify upserted rows actually landed.
   9. `write_complete` checkpoint.
6. **Zero-rows guard** — if `total_inserted == 0` across the whole run, raise CRITICAL (signals silent data loss).
7. Bulk insert rejections.
8. Close `ops.pipeline_runs` row with final status (`success` or `success_with_rej_rows`).
9. Update `metadata.sources.last_retrieved`.

---

## What you implement

```python
from database.base_transformer import BaseTransformer
import pandas as pd

class ACMETransformer(BaseTransformer):
    def __init__(self):
        super().__init__(source_id='acme')
        self.run_date = None  # set in get_batch_units

    def get_batch_units(self) -> list:
        # Read completed ingestion checkpoints from ops.checkpoints
        # to find which B2 files actually exist for transformation.
        # Also extract run_date from the most recent checkpoint.
        ...
        self.run_date = rows[0][1].date().isoformat()
        return sorted({row[0] for row in rows})

    def get_b2_key(self, batch_unit) -> str:
        # Must reconstruct the exact key the ingestor used
        return f"bronze/acme/{batch_unit}_{self.run_date}.json"

    def deserialize(self, data: bytes) -> pd.DataFrame:
        from io import BytesIO
        return pd.read_json(BytesIO(data), orient='records', dtype=False)

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        # Standardize to canonical shape
        # Required columns: country_iso3, year, period, metric_id, value, source_id, retrieved_at
        # Drop nulls, cast types, map source codes → ISO3 if needed
        df = df_raw.copy()
        # ... your parsing logic
        return df

if __name__ == "__main__":
    ACMETransformer().run()
```

---

## Conventions

- **Canonical observation shape**: `country_iso3` (ISO3), `year` (Int64), `period` (str: 'annual'), `metric_id` (str: `<source>.<code>`), `value` (str — preserves source precision), `source_id` (str), `retrieved_at` (str: 'YYYY-MM-DD').
- **`run_date` reconstruction**: must match what the ingestor used in B2 keys. Read from `ops.checkpoints.checkpointed_at` of the most recent completed ingestion checkpoint, not from `date.today()` (transformation may run a day after ingestion).
- **Country code mapping** lives in transformation, not ingestion. Ingestion stores raw source codes; transformation maps to ISO3 via a one-time `metadata.country_codes` lookup (cached in instance variable for the run).
- **Value casting**: `f"{float(v):.10g}"` — up to 10 significant figures, no scientific notation. Preserves source precision without inventing digits.

---

## The `_first_run` and `since_year` interaction

This caused a real bug worth understanding.

`BaseTransformer._first_run` is `True` when no `transformation_batch` checkpoints exist for the source — i.e. the silver layer is empty. On first run, the base class skips per-batch revision detection and read-back to avoid hundreds of useless round-trips against an empty silver layer.

Several transformers (IMF, PWT, OECD) also apply a `since_year` filter inside `parse()` to only upsert rows within a 5-year revision window. The window is calculated as `metadata.sources.last_retrieved - 5 years`.

**The bug**: ingestion sets `last_retrieved` to today *before* transformation runs. So on a first-ever transformation, `since_year = today - 5 = 2021`. This silently dropped all historical data from indicators like IMF's `DEBT1` (data ends 2015) or all of PWT (data ends 2023).

**The fix**: `_get_since_year` now checks `self._first_run` and returns a deep-history value (1950 for IMF/PWT, 1981 for OECD) on first run. The 5-year window only applies on subsequent runs, where it correctly bounds the revision window.

If you add a new transformer that uses a `since_year` filter, follow the same pattern:

```python
def _get_since_year(self) -> int:
    if self._first_run:
        return 1950   # or whatever your source's earliest data year is
    # subsequent runs: 5-year revision window
    last = ...read last_retrieved...
    return last.year - 5
```

---

## Common operations

```bash
# Run a single transformer for testing
python3 -m transformation.world_bank_transform

# Re-run the full pipeline for one source (ingest + transform + coverage)
python3 -c "from orchestration.world_bank_flow import world_bank_flow; world_bank_flow()"

# If transformation crashed mid-run, just re-run — checkpoints make it idempotent
python3 -c "from orchestration.world_bank_flow import world_bank_flow; world_bank_flow()"

# To force a full re-transformation from scratch (clears silver for that source)
python3 -m database.reset_source --source world_bank --silver --confirm
python3 -c "from orchestration.world_bank_flow import world_bank_flow; world_bank_flow()"
```

## File layout

```
transformation/
├── README.md
├── world_bank_transform.py
├── imf_transform.py
├── openalex_transform.py
├── oecd_transform.py
├── oxford_transform.py
├── wipo_transform.py
└── pwt_transform.py
```

Run any transformer standalone for testing:

```bash
python3 -m transformation.imf_transform
```

This invokes the `if __name__ == "__main__"` block at the bottom of each file, which calls `<Class>().run()`.
