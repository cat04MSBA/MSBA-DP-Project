"""
orchestration/pwt_flow.py
=========================
Prefect flow for Penn World Tables ingestion and transformation.

Same pattern as oxford_flow.py — triggered by upload_to_b2.py
when a team member uploads a new PWT .dta file to B2.

PWT-SPECIFIC:
    PWT releases every few years (PWT 10.0 in 2021, PWT 11.0
    in 2024). The team downloads the new .dta file from
    rug.nl/ggdc/productivity/pwt/ when a new version is published.

    Largest source by row count — up to 640,000 observations
    (185 countries × 74 years × 47 metrics before NaN dropping).
    Timeout set to 2 hours to handle the full first-time load.
    Subsequent runs after a new PWT version are also full reloads
    since PWT revises historical values significantly between
    major releases.
"""

from prefect import flow, task, get_run_logger

from ingestion.pwt_ingest import PWTIngestor
from transformation.pwt_transform import PWTTransformer


@task(
    name                = "pwt-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def pwt_ingest_task():
    """
    Run PWT ingestion.
    Reads .dta from B2, melts 47 variables wide→tall,
    filters by since_date, saves tall JSON to B2.
    """
    logger = get_run_logger()
    logger.info("Starting PWT ingestion")
    PWTIngestor().run()
    logger.info("PWT ingestion complete")


@task(
    name                = "pwt-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def pwt_transform_task():
    """
    Run PWT transformation.
    Reads tall JSON from B2, validates, casts values, upserts.
    PWT uses ISO3 directly — no country code mapping needed.
    """
    logger = get_run_logger()
    logger.info("Starting PWT transformation")
    PWTTransformer().run()
    logger.info("PWT transformation complete")


@flow(
    name            = "pwt-flow",
    description     = (
        "Ingestion and transformation for Penn World Tables 11.0. "
        "Triggered manually when a new PWT .dta file is uploaded to B2. "
        "Largest source: up to 640,000 observations across 47 metrics."
    ),
    timeout_seconds = 7200,  # 2 hours — largest source
)
def pwt_flow():
    """
    PWT full pipeline: ingestion then transformation.
    Triggered by upload_to_b2.py when a new .dta is uploaded.
    """
    logger = get_run_logger()
    logger.info("pwt_flow started")
    pwt_ingest_task()
    pwt_transform_task()
    logger.info("pwt_flow complete")


if __name__ == "__main__":
    pwt_flow()
