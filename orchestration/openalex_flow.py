"""
orchestration/openalex_flow.py
==============================
Prefect flow for OpenAlex AI Publications ingestion and transformation.

Same structure as world_bank_flow.py — see that file for
full design justification. OpenAlex-specific notes below.

OPENALEX-SPECIFIC:
    One API call per year (1990 to current year).
    ~36 calls per full run, ~6 calls per incremental run.
    Fast source — timeout set to 15 minutes.

    OpenAlex is CC0 (public domain) so no API key is strictly
    required, but the OPENALEX_KEY env var increases the rate
    limit significantly. The ingestion script handles both cases.
"""

from prefect import flow, task, get_run_logger

from ingestion.openalex_ingest import OpenAlexIngestor
from transformation.openalex_transform import OpenAlexTransformer


@task(
    name                = "openalex-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def openalex_ingest_task():
    """Run OpenAlex ingestion — fetch AI publication counts per year."""
    logger = get_run_logger()
    logger.info("Starting OpenAlex ingestion")
    OpenAlexIngestor().run()
    logger.info("OpenAlex ingestion complete")


@task(
    name                = "openalex-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
)
def openalex_transform_task():
    """Run OpenAlex transformation — map ISO2→ISO3, upsert."""
    logger = get_run_logger()
    logger.info("Starting OpenAlex transformation")
    OpenAlexTransformer().run()
    logger.info("OpenAlex transformation complete")


@flow(
    name            = "openalex-flow",
    description     = (
        "Ingestion and transformation for OpenAlex AI Publications. "
        "Fetches AI publication counts per country per year (1990+), "
        "maps ISO2 country codes, upserts to standardized.observations."
    ),
    timeout_seconds = 900,  # 15 minutes
)
def openalex_flow():
    """
    OpenAlex full pipeline: ingestion then transformation.
    Called by api_flow() on the monthly cron, or independently.
    """
    logger = get_run_logger()
    logger.info("openalex_flow started")
    openalex_ingest_task()
    openalex_transform_task()
    logger.info("openalex_flow complete")


if __name__ == "__main__":
    openalex_flow()
