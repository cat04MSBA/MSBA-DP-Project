"""
orchestration/imf_flow.py
=========================
Prefect flow for IMF DataMapper ingestion and transformation.

Same structure as world_bank_flow.py — see that file for
full design justification. IMF-specific notes below.

IMF-SPECIFIC:
    Cron timing: api_flow() runs monthly. IMF publishes WEO
    in April and October. The monthly cron catches both releases
    without needing a separate schedule. Change detection
    (comparing against last_retrieved) ensures we only process
    when IMF has actually published new data.

    Timeout: 30 minutes. IMF has fewer indicators than World Bank
    and one call per indicator returns full history.
"""

from prefect import flow, task, get_run_logger

from ingestion.imf_ingest import IMFIngestor
from transformation.imf_transform import IMFTransformer


@task(
    name                = "imf-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def imf_ingest_task():
    """Run IMF ingestion — fetch all indicators from DataMapper API."""
    logger = get_run_logger()
    logger.info("Starting IMF ingestion")
    IMFIngestor().run()
    logger.info("IMF ingestion complete")


@task(
    name                = "imf-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def imf_transform_task():
    """Run IMF transformation — read B2, map ISO2→ISO3, upsert."""
    logger = get_run_logger()
    logger.info("Starting IMF transformation")
    IMFTransformer().run()
    logger.info("IMF transformation complete")


@flow(
    name            = "imf-flow",
    description     = (
        "Ingestion and transformation for IMF DataMapper. "
        "Fetches all registered indicators, saves to B2, "
        "maps country codes, upserts to standardized.observations."
    ),
    timeout_seconds = 1800,  # 30 minutes
)
def imf_flow():
    """
    IMF full pipeline: ingestion then transformation.
    Called by api_flow() on the monthly cron, or independently.
    """
    logger = get_run_logger()
    logger.info("imf_flow started")
    imf_ingest_task()
    imf_transform_task()
    logger.info("imf_flow complete")


if __name__ == "__main__":
    imf_flow()
