"""
orchestration/world_bank_flow.py
================================
Prefect flow for World Bank WDI ingestion and transformation.
on_failure hook sends email only after all retries exhausted.
"""

from prefect import flow, task, get_run_logger
from ingestion.world_bank_ingest import WorldBankIngestor
from transformation.world_bank_transform import WorldBankTransformer
from database.calculate_coverage import calculate_coverage
from database.email_utils import send_critical_alert


def on_task_failure(task, task_run, state):
    """Called by Prefect only after ALL retries are exhausted."""
    error = state.result(raise_on_failure=False)
    send_critical_alert(
        source_id  = 'world_bank',
        run_id     = None,
        error_text = (
            f"Task '{task.name}' failed after all retries.\n"
            f"Flow: world-bank-flow\n"
            f"Error: {error}\n\n"
            f"Check ops.pipeline_runs for full details."
        ),
        stage = 'pipeline',
    )


@task(
    name                = "world-bank-ingestion",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def world_bank_ingest_task():
    logger = get_run_logger()
    logger.info("Starting World Bank ingestion")
    WorldBankIngestor().run()
    logger.info("World Bank ingestion complete")


@task(
    name                = "world-bank-transformation",
    retries             = 3,
    retry_delay_seconds = [60, 300, 600],
    on_failure          = [on_task_failure],
)
def world_bank_transform_task():
    logger = get_run_logger()
    logger.info("Starting World Bank transformation")
    WorldBankTransformer().run()
    logger.info("World Bank transformation complete")


@task(
    name   = "world-bank-coverage",
    retries = 1,
    retry_delay_seconds = [60],
)
def world_bank_coverage_task():
    """
    Update coverage statistics in metadata.metrics for World Bank.
    Runs after transformation so available_from, available_to,
    country_count, observation_count, and missing_value_rate
    reflect the latest ingested data.

    WHY retries=1 (not 3):
        Coverage is non-critical — a failure here does not affect
        the data itself. One retry is sufficient. If it still fails,
        the team can run calculate_coverage.py manually.
    """
    logger = get_run_logger()
    logger.info("Updating World Bank coverage statistics")
    calculate_coverage(source_id='world_bank')
    logger.info("World Bank coverage statistics updated")


@flow(
    name            = "world-bank-flow",
    description     = "Ingestion, transformation, and coverage update for World Bank WDI.",
    timeout_seconds = 57600,  # 16 hours — World Bank has 439 metrics, overnight run expected on Session Pooler
)
def world_bank_flow():
    logger = get_run_logger()
    logger.info("world_bank_flow started")
    world_bank_ingest_task()
    world_bank_transform_task()
    world_bank_coverage_task()
    logger.info("world_bank_flow complete")


if __name__ == "__main__":
    world_bank_flow()
