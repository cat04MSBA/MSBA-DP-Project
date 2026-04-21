"""
orchestration/imf_flow.py
=============================
Prefect flow for imf ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.imf_ingest import IMFIngestor
from transformation.imf_transform import IMFTransformer
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
    logger.info("Starting imf ingestion")
    IMFIngestor().run()
    logger.info("imf ingestion complete")


@task(
    name                = "imf-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def imf_transform_task():
    logger = get_run_logger()
    logger.info("Starting imf transformation")
    IMFTransformer().run()
    logger.info("imf transformation complete")


@flow(
    name            = "imf-flow",
    description     = "Ingestion and transformation for imf.",
    timeout_seconds = 1800,
)
def imf_flow():
    logger = get_run_logger()
    logger.info("imf_flow started")
    imf_ingest_task()
    imf_transform_task()
    logger.info("imf_flow complete")


if __name__ == "__main__":
    imf_flow()
