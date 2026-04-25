# Orchestration

Prefect flows that wire ingestion + transformation + coverage tasks together. One subflow per source, plus a master `api_flow()` for the four scheduled API sources.

---

## Layout

| File | Purpose |
|---|---|
| `pipeline.py` | master `api_flow()` — runs all 4 API sources on monthly cron · sends summary email |
| `world_bank_flow.py` | per-source subflow (3 tasks: ingest, transform, coverage) |
| `imf_flow.py` | same |
| `openalex_flow.py` | same |
| `oecd_flow.py` | same |
| `oxford_flow.py` | triggered by `database/upload_to_b2.py` after a manual file upload |
| `wipo_flow.py` | same |
| `pwt_flow.py` | same |

---

## Concurrency design

**API sources** run with a `ConcurrentTaskRunner` so all four ingestion tasks fire in parallel — they hit independent APIs with no shared state, so concurrency cuts total ingestion time from sum-of-all to max-of-all.

**Transformation tasks** run sequentially (the master flow `.result()`s each one before starting the next). Sequential is required to avoid saturating Supabase's free-tier connection pool — running four transformers in parallel would each open multiple connections and cause timeouts.

This is why the design uses `ConcurrentTaskRunner` at the master flow level but the transformation tasks block on each other's results.

---

## Per-source subflow shape

Every source flow has the same three-task structure:

```python
@task(retries=3, retry_delay_seconds=[60, 300, 600], on_failure=[on_task_failure])
def <source>_ingest_task():
    <Source>Ingestor().run()

@task(retries=3, retry_delay_seconds=[60, 300, 600], on_failure=[on_task_failure])
def <source>_transform_task():
    <Source>Transformer().run()

@task(retries=1, retry_delay_seconds=[60])
def <source>_coverage_task():
    calculate_coverage(source_id='<source>')

@flow(name="<source>-flow", timeout_seconds=...)
def <source>_flow():
    <source>_ingest_task()
    <source>_transform_task()
    <source>_coverage_task()
```

**Why three retries on ingest/transform but only one on coverage?** Coverage is non-critical — a failed coverage refresh doesn't affect the underlying data, just the metadata stats shown in the Streamlit app. One retry is enough; if it still fails the team can run `python3 -m database.calculate_coverage --source <name>` manually.

**Why `on_failure` only on ingest and transform?** Those are the data-correctness tasks. A failure means data is missing or wrong in the silver layer, which is critical and warrants an immediate alert email. Coverage failures are tracked but don't email.

---

## Email triggers from orchestration

| Event | Severity | Where it fires |
|---|---|---|
| Task fails after all 3 retries | CRITICAL | `on_task_failure` hook in each source flow |
| Whole `api_flow()` finishes | INFORMATIONAL | `_send_summary_email()` in `pipeline.py` — per-source status, rows in/rejected, retries used |
| (Plus all critical quality-check failures, fired from inside the tasks themselves) | CRITICAL | inside `BaseIngestor` / `BaseTransformer` |

---

## Manual triggers

For one-off runs (testing, re-runs, debugging):

```bash
# All API sources at once (mimics the monthly cron)
python3 -c "from orchestration.pipeline import api_flow; api_flow()"

# Single API source
python3 -c "from orchestration.world_bank_flow import world_bank_flow; world_bank_flow()"

# File source — usually triggered automatically by upload_to_b2.py,
# but you can call it directly if the B2 file is already in place
python3 -c "from orchestration.pwt_flow import pwt_flow; pwt_flow()"
```

For the file sources, the typical flow is:

```bash
python3 database/upload_to_b2.py --source pwt --file data/raw/pwt/pwt110.dta
# This uploads the file to B2 AND triggers pwt_flow() automatically.
```

---

## Adding a new source flow

1. Copy `world_bank_flow.py` to `<source>_flow.py`.
2. Replace `world_bank` / `WorldBank` references with your source.
3. Adjust `timeout_seconds` based on expected runtime (e.g. World Bank uses `57600` for the 16h overnight first-load).
4. If it's an API source, edit `pipeline.py`:
   - Import the three task functions at the top.
   - Submit the ingestion task in the concurrent block.
   - Add the transformation + coverage calls in the sequential block.
5. Test end-to-end with the manual trigger.

---

## Scheduling

The monthly cron lives in `prefect.yaml` (not in code) so it can be changed without modifying the flow files. Default: `0 2 1 * *` (1st of each month, 02:00 UTC).

To deploy and schedule:

```bash
prefect deploy --all
```

To trigger ad hoc from the Prefect UI: open the deployment for `api-flow` and click *Run*.

## Common gotchas

- **`ConcurrentTaskRunner` does not parallelize transformation** — calling `.result()` on a future blocks. The master flow uses this intentionally for the transform/coverage block. If you change one to run in parallel, you'll hit Supabase pool exhaustion.
- **`on_failure` fires only after retries exhausted** — Prefect treats each retry as a new attempt, not a final failure. Don't expect the email on the first failure.
- **Subflows inherit `securityLevel`** but not retry policies — each task's retry config is local.
- **Timeouts apply at the flow level** — if a flow exceeds its `timeout_seconds`, Prefect kills the run regardless of which task is in flight. Set generous timeouts for first-load runs (World Bank historical = 16h on the Session Pooler).
