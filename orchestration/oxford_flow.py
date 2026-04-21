"""
orchestration/oxford_flow.py
================================
Prefect flow for oxford ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.oxford_ingest import OxfordIngestor
from transformation.oxford_transform import OxfordTransformer
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'oxford',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: oxford-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "oxford-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def oxford_ingest_task():
    logger = get_run_logger()
    logger.info("Starting oxford ingestion")
    OxfordIngestor().run()
    logger.info("oxford ingestion complete")


@task(
    name                = "oxford-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def oxford_transform_task():
    logger = get_run_logger()
    logger.info("Starting oxford transformation")
    OxfordTransformer().run()
    logger.info("oxford transformation complete")


@flow(
    name            = "oxford-flow",
    description     = "Ingestion and transformation for oxford.",
    timeout_seconds = 1800,
)
def oxford_flow():
    logger = get_run_logger()
    logger.info("oxford_flow started")
    oxford_ingest_task()
    oxford_transform_task()
    logger.info("oxford_flow complete")


if __name__ == "__main__":
    oxford_flow()
