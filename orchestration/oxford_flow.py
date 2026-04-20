"""
orchestration/oxford_flow.py
============================
Prefect flow for Oxford Insights GAIRI ingestion and transformation.

WHAT THIS FILE DOES:
    Defines the Oxford Prefect flow. Triggered by upload_to_b2.py
    when a team member uploads a new Oxford XLSX file to B2.
    Never called by api_flow() — Oxford has no fixed schedule.

WHY THIS IS INDEPENDENT (not inside api_flow()):
    Oxford publishes annually with no fixed date — typically
    November to January. The trigger is a human file upload,
    not a monthly timer. Making it part of api_flow() would
    require the master flow to wait for a human action, which
    is the wrong design.

HOW IT GETS TRIGGERED:
    Team member runs:
        python3 database/upload_to_b2.py --source oxford
                --file oxford_2025.xlsx --year 2025
    upload_to_b2.py uploads the file to B2 then calls:
        oxford_flow()
    Prefect tracks the run in the UI exactly like any other flow.

TIMEOUT:
    30 minutes. Oxford has 7 years of files at most, each with
    ~190 countries and one score value. Fast to process.
"""

from prefect import flow, task, get_run_logger

from ingestion.oxford_ingest import OxfordIngestor
from transformation.oxford_transform import OxfordTransformer


@task(
    name                = "oxford-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def oxford_ingest_task():
    """
    Run Oxford ingestion.
    Scans B2 for uploaded Oxford XLSX files, reads each,
    maps country names to ISO3, saves parsed JSON to B2.
    """
    logger = get_run_logger()
    logger.info("Starting Oxford ingestion")
    OxfordIngestor().run()
    logger.info("Oxford ingestion complete")


@task(
    name                = "oxford-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def oxford_transform_task():
    """
    Run Oxford transformation.
    Reads parsed JSON from B2, validates scores, upserts.
    """
    logger = get_run_logger()
    logger.info("Starting Oxford transformation")
    OxfordTransformer().run()
    logger.info("Oxford transformation complete")


@flow(
    name            = "oxford-flow",
    description     = (
        "Ingestion and transformation for Oxford Insights GAIRI. "
        "Triggered manually when a new Oxford XLSX is uploaded to B2. "
        "Processes all unprocessed year files found on B2."
    ),
    timeout_seconds = 1800,  # 30 minutes
)
def oxford_flow():
    """
    Oxford full pipeline: ingestion then transformation.
    Triggered by upload_to_b2.py when a new file is uploaded.
    Can also be triggered independently for re-runs.
    """
    logger = get_run_logger()
    logger.info("oxford_flow started")
    oxford_ingest_task()
    oxford_transform_task()
    logger.info("oxford_flow complete")


if __name__ == "__main__":
    oxford_flow()
