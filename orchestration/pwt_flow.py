"""
orchestration/pwt_flow.py
=============================
Prefect flow for pwt ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.pwt_ingest import PWTIngestor
from transformation.pwt_transform import PWTTransformer
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
    logger.info("Starting pwt ingestion")
    PWTIngestor().run()
    logger.info("pwt ingestion complete")


@task(
    name                = "pwt-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def pwt_transform_task():
    logger = get_run_logger()
    logger.info("Starting pwt transformation")
    PWTTransformer().run()
    logger.info("pwt transformation complete")


@flow(
    name            = "pwt-flow",
    description     = "Ingestion and transformation for pwt.",
    timeout_seconds = 7200,
)
def pwt_flow():
    logger = get_run_logger()
    logger.info("pwt_flow started")
    pwt_ingest_task()
    pwt_transform_task()
    logger.info("pwt_flow complete")


if __name__ == "__main__":
    pwt_flow()
