"""
orchestration/wipo_flow.py
==============================
Prefect flow for WIPO IP Statistics ingestion and transformation.
Triggered by upload_to_b2.py when a new WIPO CSV is uploaded.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.wipo_ingest import WIPOIngestor
from transformation.wipo_transform import WIPOTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'wipo_ip',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: wipo-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "wipo-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def wipo_ingest_task():
    logger = get_run_logger()
    logger.info("Starting WIPO ingestion")
    WIPOIngestor().run()
    logger.info("WIPO ingestion complete")


@task(
    name                = "wipo-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def wipo_transform_task():
    logger = get_run_logger()
    logger.info("Starting WIPO transformation")
    WIPOTransformer().run()
    logger.info("WIPO transformation complete")


@task(
    name                = "wipo-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def wipo_coverage_task():
    """Update coverage statistics in metadata.metrics for WIPO IP."""
    logger = get_run_logger()
    logger.info("Updating WIPO coverage statistics")
    calculate_coverage(source_id='wipo_ip')
    logger.info("WIPO coverage statistics updated")


@flow(
    name            = "wipo-flow",
    description     = "Ingestion, transformation, and coverage update for WIPO IP Statistics.",
    timeout_seconds = 7200,
)
def wipo_flow():
    logger = get_run_logger()
    logger.info("wipo_flow started")
    wipo_ingest_task()
    wipo_transform_task()
    wipo_coverage_task()
    logger.info("wipo_flow complete")


if __name__ == "__main__":
    wipo_flow()
