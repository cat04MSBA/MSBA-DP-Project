"""
orchestration/oxford_flow.py
================================
Prefect flow for Oxford GAIRI ingestion and transformation.
Triggered by upload_to_b2.py when a new Oxford XLSX is uploaded.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.oxford_ingest import OxfordIngestor
from transformation.oxford_transform import OxfordTransformer
from database.calculate_coverage import calculate_coverage
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
    logger.info("Starting Oxford ingestion")
    OxfordIngestor().run()
    logger.info("Oxford ingestion complete")


@task(
    name                = "oxford-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def oxford_transform_task():
    logger = get_run_logger()
    logger.info("Starting Oxford transformation")
    OxfordTransformer().run()
    logger.info("Oxford transformation complete")


@task(
    name                = "oxford-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def oxford_coverage_task():
    """Update coverage statistics in metadata.metrics for Oxford."""
    logger = get_run_logger()
    logger.info("Updating Oxford coverage statistics")
    calculate_coverage(source_id='oxford')
    logger.info("Oxford coverage statistics updated")


@flow(
    name            = "oxford-flow",
    description     = "Ingestion, transformation, and coverage update for Oxford GAIRI.",
    timeout_seconds = 7200,
)
def oxford_flow():
    logger = get_run_logger()
    logger.info("oxford_flow started")
    oxford_ingest_task()
    oxford_transform_task()
    oxford_coverage_task()
    logger.info("oxford_flow complete")


if __name__ == "__main__":
    oxford_flow()
