"""
orchestration/oecd_flow.py
==========================
Prefect flow for OECD MSTI ingestion and transformation.

EMAIL STRATEGY — on_failure hook:
    Emails are sent by the on_task_failure hook, which Prefect
    calls only after ALL retries are exhausted. This means one
    email per failure regardless of retry count.

    The base classes (base_ingestor, base_transformer) only log
    to ops.pipeline_runs and re-raise. They never send emails
    directly. This separation ensures:
    - Exactly one email per real failure
    - No emails during retry attempts
    - Email contains the final error state after all retries
"""

from prefect import flow, task, get_run_logger

from ingestion.oecd_ingest import OECDIngestor
from transformation.oecd_transform import OECDTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


# ═══════════════════════════════════════════════════════════════
# ON_FAILURE HOOK
# ═══════════════════════════════════════════════════════════════

def on_task_failure(task, task_run, state):
    """
    Called by Prefect only after ALL retries are exhausted.
    Sends exactly one CRITICAL alert email per final failure.

    WHY A HOOK NOT A TRY/EXCEPT IN THE FLOW:
        A try/except in the flow body would catch the first
        failure immediately. Prefect hooks fire on the final
        task state — after retries. One hook, one email.

    Args:
        task:     The Prefect task object.
        task_run: The Prefect task run object.
        state:    The final Failed state with error details.
    """
    logger = get_run_logger()
    error  = state.result(raise_on_failure=False)
    error_text = (
        f"Task '{task.name}' failed after all retries.\n"
        f"Flow: oecd-flow\n"
        f"Error: {error}\n\n"
        f"Check ops.pipeline_runs for full details."
    )
    logger.error(f"Task failed after all retries: {task.name}")
    send_critical_alert(
        source_id  = 'oecd_msti',
        run_id     = None,
        error_text = error_text,
        stage      = 'pipeline',
    )


# ═══════════════════════════════════════════════════════════════
# TASKS
# ═══════════════════════════════════════════════════════════════

@task(
    name                = "oecd-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def oecd_ingest_task():
    logger = get_run_logger()
    logger.info("Starting OECD ingestion")
    OECDIngestor().run()
    logger.info("OECD ingestion complete")


@task(
    name                = "oecd-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def oecd_transform_task():
    logger = get_run_logger()
    logger.info("Starting OECD transformation")
    OECDTransformer().run()
    logger.info("OECD transformation complete")


@task(
    name                = "oecd-coverage",
    retries             = 1,
    retry_delay_seconds = [60],
)
def oecd_coverage_task():
    """Update coverage statistics in metadata.metrics for OECD MSTI."""
    logger = get_run_logger()
    logger.info("Updating OECD coverage statistics")
    calculate_coverage(source_id='oecd_msti')
    logger.info("OECD coverage statistics updated")


# ═══════════════════════════════════════════════════════════════
# FLOW
# ═══════════════════════════════════════════════════════════════

@flow(
    name            = "oecd-flow",
    description     = (
        "Ingestion, transformation, and coverage update for OECD MSTI. "
        "3 metrics: BERD, GOVERD, researchers per thousand."
    ),
    timeout_seconds = 3600,
)
def oecd_flow():
    logger = get_run_logger()
    logger.info("oecd_flow started")
    oecd_ingest_task()
    oecd_transform_task()
    oecd_coverage_task()
    logger.info("oecd_flow complete")


if __name__ == "__main__":
    oecd_flow()
