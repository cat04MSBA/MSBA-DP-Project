"""
orchestration/pwt_flow.py
=============================
Prefect flow for Penn World Tables ingestion and transformation.
Triggered by upload_to_b2.py when a new PWT .dta file is uploaded.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.pwt_ingest import PWTIngestor
from transformation.pwt_transform import PWTTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'pwt',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: pwt-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "pwt-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def pwt_ingest_task():
    logger = get_run_logger()
    logger.info("Starting PWT ingestion")
    PWTIngestor().run()
    logger.info("PWT ingestion complete")


@task(
    name                = "pwt-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def pwt_transform_task():
    logger = get_run_logger()
    logger.info("Starting PWT transformation")
    PWTTransformer().run()
    logger.info("PWT transformation complete")


@task(
    name                = "pwt-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def pwt_coverage_task():
    """Update coverage statistics in metadata.metrics for PWT."""
    logger = get_run_logger()
    logger.info("Updating PWT coverage statistics")
    calculate_coverage(source_id='pwt')
    logger.info("PWT coverage statistics updated")


@flow(
    name            = "pwt-flow",
    description     = "Ingestion, transformation, and coverage update for Penn World Tables.",
    timeout_seconds = 7200,
)
def pwt_flow():
    logger = get_run_logger()
    logger.info("pwt_flow started")
    pwt_ingest_task()
    pwt_transform_task()
    pwt_coverage_task()
    logger.info("pwt_flow complete")


if __name__ == "__main__":
    pwt_flow()
