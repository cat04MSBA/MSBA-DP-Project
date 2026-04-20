"""
orchestration/oecd_flow.py
==========================
Prefect flow for OECD MSTI ingestion and transformation.

Same structure as world_bank_flow.py — see that file for
full design justification. OECD-specific notes below.

OECD-SPECIFIC:
    One API call returns the full MSTI dataset as a CSV.
    3 metrics × ~50 countries × ~45 years = ~6,750 rows max.
    Smallest and fastest source — timeout set to 15 minutes.

    OECD publishes biannually (March and September).
    The monthly cron catches both releases. Change detection
    via the SDMX updatedAfter parameter ensures we skip months
    where OECD has not published new data.
"""

from prefect import flow, task, get_run_logger

from ingestion.oecd_ingest import OECDIngestor
from transformation.oecd_transform import OECDTransformer


@task(
    name                = "oecd-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def oecd_ingest_task():
    """Run OECD ingestion — download full MSTI CSV from SDMX API."""
    logger = get_run_logger()
    logger.info("Starting OECD ingestion")
    OECDIngestor().run()
    logger.info("OECD ingestion complete")


@task(
    name                = "oecd-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def oecd_transform_task():
    """Run OECD transformation — validate measures, upsert."""
    logger = get_run_logger()
    logger.info("Starting OECD transformation")
    OECDTransformer().run()
    logger.info("OECD transformation complete")


@flow(
    name            = "oecd-flow",
    description     = (
        "Ingestion and transformation for OECD MSTI. "
        "Downloads full MSTI CSV (BERD, GOVERD, researchers), "
        "validates and upserts to standardized.observations."
    ),
    timeout_seconds = 900,  # 15 minutes
)
def oecd_flow():
    """
    OECD full pipeline: ingestion then transformation.
    Called by api_flow() on the monthly cron, or independently.
    """
    logger = get_run_logger()
    logger.info("oecd_flow started")
    oecd_ingest_task()
    oecd_transform_task()
    logger.info("oecd_flow complete")


if __name__ == "__main__":
    oecd_flow()
