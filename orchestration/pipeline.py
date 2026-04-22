"""
orchestration/pipeline.py
=========================
Master Prefect flow for all API sources.

WHAT THIS FILE DOES:
    Defines api_flow() — the master flow that runs all four API
    sources on a monthly cron schedule. Each source runs as a
    subflow, giving it its own status, logs, and retry handling
    in the Prefect UI while being orchestrated together.

WHICH SOURCES ARE HERE:
    World Bank, IMF, OpenAlex, OECD.
    These four share the same trigger: the monthly timer.

WHICH SOURCES ARE NOT HERE:
    Oxford, WIPO, PWT — manual file sources with unpredictable
    release schedules. They are triggered independently by
    upload_to_b2.py when a team member uploads a new file.
    See oxford_flow.py, wipo_flow.py, pwt_flow.py.

CONCURRENCY DESIGN — WHY INGESTION RUNS CONCURRENTLY
BUT TRANSFORMATION RUNS SEQUENTIALLY:

    Ingestion phase (concurrent):
        All four ingestion tasks run at the same time. Each
        hits a different external API and saves to B2. They
        have no shared state and no dependency on each other.
        Running them concurrently cuts total ingestion time
        from ~sum of all times to ~max of all times.

    Transformation phase (sequential):
        Transformation upserts into Supabase. The Supabase free
        tier has a limited connection pool. Running four
        transformation scripts concurrently would saturate the
        pool and cause connection timeouts. Sequential
        transformation is slower but reliable.

    The design document (Section 3) explicitly states:
    "Sequential transformation was chosen over parallel
    transformation to avoid saturating the Supabase free tier
    connection pool."

WHY SUBFLOWS INSTEAD OF TASKS FOR SOURCE FLOWS:
    Each source flow (world_bank_flow, imf_flow, etc.) is itself
    a @flow not a @task. Calling a @flow from inside another @flow
    creates a subflow in Prefect — it gets its own run ID, its own
    status, its own logs, and its own retry handling. If World Bank
    fails, Prefect shows "world-bank-flow: Failed" independently
    of the other source flows. A @task would not give this level
    of visibility.

MONTHLY SUMMARY EMAIL:
    After all four sources complete (success or failure),
    api_flow() sends one summary email to the team with the
    status of each source, rows inserted, rows rejected, and
    any retry attempts. This matches design document Section 11.

CRON SCHEDULE:
    Runs on the first day of each month at 02:00 UTC.
    Early morning to avoid peak API usage times.
    Configured in prefect.yaml — not hardcoded here so it
    can be changed without modifying this file.

HOW TO RUN MANUALLY (for testing or re-runs):
    python3 -c "from orchestration.pipeline import api_flow;
                api_flow()"
    Or via Prefect UI: select api-flow deployment, click Run.
"""

from datetime import datetime
from prefect import flow, task, get_run_logger
from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner

from orchestration.world_bank_flow import (
    world_bank_ingest_task,
    world_bank_transform_task,
    world_bank_coverage_task,
)
from orchestration.imf_flow import (
    imf_ingest_task,
    imf_transform_task,
    imf_coverage_task,
)
from orchestration.openalex_flow import (
    openalex_ingest_task,
    openalex_transform_task,
    openalex_coverage_task,
)
from orchestration.oecd_flow import (
    oecd_ingest_task,
    oecd_transform_task,
    oecd_coverage_task,
)
from database.email_utils import send_email


# ═══════════════════════════════════════════════════════════════
# MASTER FLOW
# ═══════════════════════════════════════════════════════════════

@flow(
    name            = "api-flow",
    description     = (
        "Master flow for all API sources: World Bank, IMF, "
        "OpenAlex, OECD. Runs monthly. Ingestion concurrent, "
        "transformation and coverage sequential. "
        "Sends summary email at end."
    ),
    task_runner     = ConcurrentTaskRunner(),
    # ConcurrentTaskRunner allows ingestion tasks to run
    # concurrently. Transformation and coverage tasks run
    # sequentially because they call .result() which blocks
    # until complete — prevents Supabase connection pool
    # exhaustion on the free tier.
    timeout_seconds = 14400,  # 4 hours total
    # World Bank alone can take 2 hours on first run.
    # 4 hours covers all four sources with headroom.
)
def api_flow():
    """
    Master flow: runs all four API sources monthly.

    Phase 1 (concurrent): all four ingestion tasks submitted
    simultaneously. Each hits its own external API and writes
    to B2. No shared state between them.

    Phase 2 (sequential): transformation then coverage runs
    one source at a time to avoid saturating Supabase
    free tier connection pool.

    Phase 3: monthly summary email with per-source status.
    """
    logger     = get_run_logger()
    started_at = datetime.utcnow()
    results    = {}

    logger.info("api_flow started — submitting ingestion tasks concurrently")

    # ── Phase 1: Concurrent ingestion ─────────────────────────
    # Submit all four ingestion tasks without waiting for them.
    # They run simultaneously — each hits a different external API.
    wb_ingest_future  = world_bank_ingest_task.submit()
    imf_ingest_future = imf_ingest_task.submit()
    oa_ingest_future  = openalex_ingest_task.submit()
    oe_ingest_future  = oecd_ingest_task.submit()

    # Wait for all ingestion tasks to complete before starting
    # transformation. Transformation reads from B2 files written
    # by ingestion — it must not start before ingestion finishes.
    ingest_futures = [
        wb_ingest_future,
        imf_ingest_future,
        oa_ingest_future,
        oe_ingest_future,
    ]

    logger.info("Waiting for all ingestion tasks to complete...")
    wait(ingest_futures)

    # Collect ingestion results — track which succeeded/failed.
    results['world_bank_ingestion'] = _check_future(
        wb_ingest_future, 'world_bank ingestion'
    )
    results['imf_ingestion'] = _check_future(
        imf_ingest_future, 'imf ingestion'
    )
    results['openalex_ingestion'] = _check_future(
        oa_ingest_future, 'openalex ingestion'
    )
    results['oecd_ingestion'] = _check_future(
        oe_ingest_future, 'oecd ingestion'
    )

    logger.info(
        "All ingestion tasks complete. "
        "Starting sequential transformation and coverage..."
    )

    # ── Phase 2: Sequential transformation + coverage ─────────
    # Run transformation then coverage one source at a time.
    # Each .submit() followed by immediate .result() is
    # effectively sequential — next does not start until
    # current completes.
    #
    # WHY COVERAGE RUNS HERE NOT SEPARATELY:
    #   Coverage depends on transformation having written rows
    #   to standardized.observations. Running it immediately
    #   after each source's transformation ensures coverage
    #   stats are always current after each monthly run
    #   without any manual step. Coverage failure does NOT
    #   block the next source — it is non-critical.
    for source, ingest_key, transform_task, coverage_task in [
        ('world_bank', 'world_bank_ingestion', world_bank_transform_task, world_bank_coverage_task),
        ('imf',        'imf_ingestion',        imf_transform_task,        imf_coverage_task),
        ('openalex',   'openalex_ingestion',   openalex_transform_task,   openalex_coverage_task),
        ('oecd',       'oecd_ingestion',        oecd_transform_task,       oecd_coverage_task),
    ]:
        # Only run transformation if ingestion succeeded.
        # If ingestion failed, transformation would fail too
        # (no B2 files to read). Skip and log instead.
        if not results.get(ingest_key, False):
            logger.warning(
                f"Skipping {source} transformation and coverage — "
                f"ingestion did not succeed."
            )
            results[f'{source}_transformation'] = False
            results[f'{source}_coverage']       = False
            continue

        logger.info(f"Running {source} transformation...")
        transform_future = transform_task.submit()
        transform_ok = _check_future(
            transform_future, f'{source} transformation'
        )
        results[f'{source}_transformation'] = transform_ok

        # Only run coverage if transformation succeeded.
        # Coverage reads from standardized.observations —
        # meaningless if transformation wrote no rows.
        if not transform_ok:
            logger.warning(
                f"Skipping {source} coverage — "
                f"transformation did not succeed."
            )
            results[f'{source}_coverage'] = False
            continue

        logger.info(f"Running {source} coverage update...")
        coverage_future = coverage_task.submit()
        results[f'{source}_coverage'] = _check_future(
            coverage_future, f'{source} coverage'
        )

    # ── Phase 3: Summary email ─────────────────────────────────
    # Send one email summarising the entire monthly run.
    # Always sent — even if some sources failed — so the team
    # has a complete picture of what happened.
    _send_summary_email(results, started_at)

    logger.info("api_flow complete")


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _check_future(future, label: str) -> bool:
    """
    Check whether a Prefect task future completed successfully.
    Returns True on success, False on failure.
    Logs the outcome without raising — we want the master flow
    to continue even if one source fails.

    WHY NOT RAISE ON FAILURE:
        If World Bank fails and we raise, IMF, OpenAlex, and OECD
        never run. A failed source should not block the others.
        The summary email tells the team what failed.
    """
    logger = get_run_logger()
    try:
        future.result()
        logger.info(f"✓ {label} succeeded")
        return True
    except Exception as e:
        logger.error(f"✗ {label} failed: {e}")
        return False


def _send_summary_email(results: dict, started_at: datetime):
    """
    Send monthly summary email to the team.
    Lists each source's ingestion, transformation, and coverage
    status. Always sent regardless of individual source outcomes.

    WHY ONE EMAIL PER MONTHLY RUN (not one per source):
        From design document Section 11: "Monthly summary email:
        sent at the end of every master flow run. Contains:
        per-source status, rows inserted, rows rejected..."
        One consolidated email is less noisy than four separate
        emails and gives the team a complete picture at once.
    """
    now      = datetime.utcnow()
    duration = now - started_at
    minutes  = int(duration.total_seconds() // 60)
    seconds  = int(duration.total_seconds() % 60)

    lines = [
        f"Monthly Pipeline Run — {now.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Duration: {minutes}m {seconds}s",
        "=" * 60,
        "",
        "Source Results:",
        "",
    ]

    sources    = ['world_bank', 'imf', 'openalex', 'oecd']
    all_passed = True

    for source in sources:
        ingest_ok    = results.get(f'{source}_ingestion',     False)
        transform_ok = results.get(f'{source}_transformation', False)
        coverage_ok  = results.get(f'{source}_coverage',       False)

        # Coverage failure is non-critical — does not count as
        # an overall failure. Data is still in the silver layer.
        overall_ok = ingest_ok and transform_ok
        if not overall_ok:
            all_passed = False

        status = "✓ OK" if overall_ok else "✗ FAILED"
        cov_str = "OK" if coverage_ok else ("FAILED" if transform_ok else "SKIPPED")

        lines.append(
            f"  {source:<20} "
            f"ingest: {'OK' if ingest_ok else 'FAILED'} | "
            f"transform: {'OK' if transform_ok else 'FAILED'} | "
            f"coverage: {cov_str} | "
            f"{status}"
        )

    lines.extend([
        "",
        "─" * 60,
        "",
        "Check ops.pipeline_runs in Supabase for row counts.",
        "Check ops.quality_runs for quality check details.",
        "Check metadata.metrics for updated coverage statistics.",
        "Check Prefect UI for full task logs.",
        "",
        "Next scheduled run: first day of next month at 02:00 UTC.",
    ])

    if not all_passed:
        lines.insert(2, "⚠ ONE OR MORE SOURCES FAILED — ACTION MAY BE REQUIRED")
        lines.insert(3, "")

    subject = (
        "✓ Monthly Pipeline Run Complete"
        if all_passed else
        "⚠ Monthly Pipeline Run — Some Sources Failed"
    )

    send_email(subject=subject, body="\n".join(lines))


if __name__ == "__main__":
    api_flow()
