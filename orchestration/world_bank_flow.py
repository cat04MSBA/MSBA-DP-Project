"""
orchestration/world_bank_flow.py
================================
Prefect flow for World Bank WDI ingestion and transformation.

WHAT THIS FILE DOES:
    Defines the World Bank Prefect flow. When triggered — either
    by api_flow() on the monthly cron or independently for
    debugging/re-runs — it runs ingestion then transformation
    for the World Bank source.

WHY ONE FLOW PER SOURCE:
    Each source flow is independently triggerable. api_flow()
    calls all API source flows together on the monthly cron,
    but any individual source can be re-triggered alone if it
    fails or needs a manual re-run. The checkpoint system ensures
    re-runs skip already-completed batches.

FLOW STRUCTURE:
    world_bank_flow()
        ├── world_bank_ingest_task()  → WorldBankIngestor.run()
        └── world_bank_transform_task() → WorldBankTransformer.run()

WHY TASKS WRAP THE BASE CLASS run() METHODS:
    Prefect @task gives each step its own status in the UI —
    you can see whether ingestion or transformation failed
    without reading logs. Tasks also enable Prefect's built-in
    retry logic as a second layer of protection on top of our
    own retry logic inside the base classes.

WHY TRANSFORMATION RUNS SEQUENTIALLY AFTER INGESTION:
    Transformation reads from B2 files written by ingestion.
    Running them concurrently would cause transformation to
    read files that ingestion has not finished writing yet.
    Sequential execution is mandatory for correctness.

RETRY CONFIGURATION:
    Task-level retries: 3 attempts with exponential backoff.
    This is on top of the internal retry logic in base classes
    which handles transient API failures. Task retries handle
    broader failures like Supabase connection drops or B2
    outages that affect the entire run, not just one batch.

INDEPENDENT TRIGGERING:
    To run World Bank alone (e.g. after a failure):
        python3 -c "from orchestration.world_bank_flow import
                    world_bank_flow; world_bank_flow()"
    Or via Prefect UI: select world_bank_flow deployment,
    click "Run" with default parameters.
"""

from prefect import flow, task, get_run_logger
from datetime import timedelta

from ingestion.world_bank_ingest import WorldBankIngestor
from transformation.world_bank_transform import WorldBankTransformer


# ═══════════════════════════════════════════════════════════════
# TASKS
# ═══════════════════════════════════════════════════════════════

@task(
    name            = "world-bank-ingestion",
    retries         = 3,
    retry_delay_seconds = [60, 300, 600],
    # Task-level retries handle broad failures (Supabase down,
    # B2 outage) that affect the entire run. Internal base class
    # retries handle per-batch transient API failures.
)
def world_bank_ingest_task():
    """
    Run World Bank ingestion.
    Fetches all registered WB indicators from the API,
    saves raw JSON to B2, verifies checksums.
    """
    logger = get_run_logger()
    logger.info("Starting World Bank ingestion")
    ingestor = WorldBankIngestor()
    ingestor.run()
    logger.info("World Bank ingestion complete")


@task(
    name            = "world-bank-transformation",
    retries         = 3,
    retry_delay_seconds = [60, 300, 600],
)
def world_bank_transform_task():
    """
    Run World Bank transformation.
    Reads JSON from B2, validates, casts values, upserts
    to standardized.observations.
    """
    logger = get_run_logger()
    logger.info("Starting World Bank transformation")
    transformer = WorldBankTransformer()
    transformer.run()
    logger.info("World Bank transformation complete")


# ═══════════════════════════════════════════════════════════════
# FLOW
# ═══════════════════════════════════════════════════════════════

@flow(
    name        = "world-bank-flow",
    description = (
        "Ingestion and transformation for World Bank WDI. "
        "Fetches all registered indicators, saves to B2, "
        "transforms and upserts to standardized.observations."
    ),
    timeout_seconds = 7200,
    # 2 hour timeout. World Bank is the largest API source —
    # 439 indicators × ~60 years on first run. Subsequent
    # incremental runs are much faster (5-year window).
)
def world_bank_flow():
    """
    World Bank full pipeline: ingestion then transformation.
    Called by api_flow() on the monthly cron, or independently
    for debugging and re-runs.
    """
    logger = get_run_logger()
    logger.info("world_bank_flow started")

    # Ingestion must complete before transformation starts.
    # Transformation reads from B2 files written by ingestion.
    world_bank_ingest_task()
    world_bank_transform_task()

    logger.info("world_bank_flow complete")


if __name__ == "__main__":
    world_bank_flow()
