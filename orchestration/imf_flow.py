"""
orchestration/imf_flow.py
=============================
Prefect flow for IMF ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.imf_ingest import IMFIngestor
from transformation.imf_transform import IMFTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'imf',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: imf-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "imf-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def imf_ingest_task():
    logger = get_run_logger()
    logger.info("Starting IMF ingestion")
    IMFIngestor().run()
    logger.info("IMF ingestion complete")


@task(
    name                = "imf-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def imf_transform_task():
    logger = get_run_logger()
    logger.info("Starting IMF transformation")
    IMFTransformer().run()
    logger.info("IMF transformation complete")


@task(
    name                = "imf-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def imf_coverage_task():
    """Update coverage statistics in metadata.metrics for IMF."""
    logger = get_run_logger()
    logger.info("Updating IMF coverage statistics")
    calculate_coverage(source_id='imf')
    logger.info("IMF coverage statistics updated")


@flow(
    name            = "imf-flow",
    description     = "Ingestion, transformation, and coverage update for IMF.",
    timeout_seconds = 28800,  # 8 hours — needed on Session Pooler (IPv4 networks)
)
def imf_flow():
    logger = get_run_logger()
    logger.info("imf_flow started")
    imf_ingest_task()
    imf_transform_task()
    imf_coverage_task()
    logger.info("imf_flow complete")


if __name__ == "__main__":
    imf_flow()
