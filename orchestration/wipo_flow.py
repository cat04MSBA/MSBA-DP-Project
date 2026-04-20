"""
orchestration/wipo_flow.py
==========================
Prefect flow for WIPO IP Statistics ingestion and transformation.

Same pattern as oxford_flow.py — triggered by upload_to_b2.py
when a team member uploads a new WIPO CSV to B2.

WIPO-SPECIFIC:
    WIPO updates the AI patent dataset on a rolling basis with
    no announced schedule. The team downloads the full CSV from
    wipo.int/en/web/ip-statistics when a new edition is noticed.
    The CSV covers 1980 to the most recent available year.

    One batch unit (full_file) — the entire CSV is processed
    in one pass. Timeout set to 30 minutes which is generous
    for a ~136 country × 45 year dataset.
"""

from prefect import flow, task, get_run_logger

from ingestion.wipo_ingest import WIPOIngestor
from transformation.wipo_transform import WIPOTransformer


@task(
    name                = "wipo-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def wipo_ingest_task():
    """
    Run WIPO ingestion.
    Reads CSV from B2, melts wide→tall, filters by since_date,
    saves to B2 as CSV.
    """
    logger = get_run_logger()
    logger.info("Starting WIPO ingestion")
    WIPOIngestor().run()
    logger.info("WIPO ingestion complete")


@task(
    name                = "wipo-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def wipo_transform_task():
    """
    Run WIPO transformation.
    Reads CSV from B2, maps ISO2→ISO3, casts counts, upserts.
    """
    logger = get_run_logger()
    logger.info("Starting WIPO transformation")
    WIPOTransformer().run()
    logger.info("WIPO transformation complete")


@flow(
    name            = "wipo-flow",
    description     = (
        "Ingestion and transformation for WIPO IP Statistics AI Patents. "
        "Triggered manually when a new WIPO CSV is uploaded to B2. "
        "Processes the full dataset (1980 to latest available year)."
    ),
    timeout_seconds = 1800,  # 30 minutes
)
def wipo_flow():
    """
    WIPO full pipeline: ingestion then transformation.
    Triggered by upload_to_b2.py when a new file is uploaded.
    """
    logger = get_run_logger()
    logger.info("wipo_flow started")
    wipo_ingest_task()
    wipo_transform_task()
    logger.info("wipo_flow complete")


if __name__ == "__main__":
    wipo_flow()
