"""
orchestration/openalex_flow.py
==================================
Prefect flow for OpenAlex ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.openalex_ingest import OpenAlexIngestor
from transformation.openalex_transform import OpenAlexTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'openalex',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: openalex-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "openalex-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def openalex_ingest_task():
    logger = get_run_logger()
    logger.info("Starting OpenAlex ingestion")
    OpenAlexIngestor().run()
    logger.info("OpenAlex ingestion complete")


@task(
    name                = "openalex-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def openalex_transform_task():
    logger = get_run_logger()
    logger.info("Starting OpenAlex transformation")
    OpenAlexTransformer().run()
    logger.info("OpenAlex transformation complete")


@task(
    name                = "openalex-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def openalex_coverage_task():
    """Update coverage statistics in metadata.metrics for OpenAlex."""
    logger = get_run_logger()
    logger.info("Updating OpenAlex coverage statistics")
    calculate_coverage(source_id='openalex')
    logger.info("OpenAlex coverage statistics updated")


@flow(
    name            = "openalex-flow",
    description     = "Ingestion, transformation, and coverage update for OpenAlex.",
    timeout_seconds = 3600,
)
def openalex_flow():
    logger = get_run_logger()
    logger.info("openalex_flow started")
    openalex_ingest_task()
    openalex_transform_task()
    openalex_coverage_task()
    logger.info("openalex_flow complete")


if __name__ == "__main__":
    openalex_flow()
