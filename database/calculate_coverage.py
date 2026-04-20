"""
database/calculate_coverage.py
==============================
Populates available_from and available_to in metadata.metrics
after ingestion by computing MIN(year) and MAX(year) from
standardized.observations for each metric.

WHAT THIS FILE DOES:
    Runs after ingestion completes. For every metric in
    metadata.metrics, queries standardized.observations to find
    the earliest and latest year that actually has data, then
    updates available_from and available_to accordingly.

WHY THIS EXISTS AS A SEPARATE SCRIPT:
    available_from and available_to cannot be set at seeding time
    because no data exists yet. They cannot be set during ingestion
    because ingestion writes one batch at a time — the MIN/MAX
    across all batches is only known after ingestion completes.
    A separate post-ingestion script computing these values from
    actual data is the cleanest design.

WHY NOT COMPUTED AT QUERY TIME:
    The Streamlit app shows researchers which years a metric
    covers before they submit a query. Computing MIN/MAX on
    standardized.observations at query time would be expensive
    for a table with millions of rows. Storing it in metadata.metrics
    makes this a single-row lookup.

WHY available_from AND available_to ARE EXCLUDED FROM SEED SCRIPT UPDATES:
    seed_metrics.py intentionally excludes these columns from
    its ON CONFLICT DO UPDATE. If seed scripts could overwrite
    them, a re-seed after ingestion would reset coverage to NULL.
    Only this script ever writes to these columns.

WHEN TO RUN:
    After every ingestion run. The orchestration flows call this
    automatically at the end of each source's transformation.
    Can also be run manually:
        python3 database/calculate_coverage.py
        python3 database/calculate_coverage.py --source world_bank

PERFORMANCE:
    Uses one query per source (not one per metric) by grouping
    on metric_id. For ~500 metrics this is much more efficient
    than 500 individual MIN/MAX queries.
    The query uses partition pruning — PostgreSQL only scans
    the relevant year partitions for each metric.

ENV VARS REQUIRED:
    DATABASE_URL
"""

import argparse
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


def calculate_coverage(source_id: str = None):
    """
    Compute and update available_from and available_to for all
    metrics, or for one source only if source_id is provided.

    Args:
        source_id: If provided, only update metrics for this source.
                   If None, update all metrics.
    """
    print("=" * 50)
    print("Calculating metric coverage...")
    if source_id:
        print(f"  Filtering to source: {source_id}")
    print("=" * 50)

    with engine.connect() as conn:

        # ── Step 1: Load metrics to update ────────────────────
        # Load metric_id and source_id for all metrics that need
        # coverage calculation. Filter by source_id if provided.
        if source_id:
            metrics_df = pd.read_sql(text("""
                SELECT metric_id, source_id
                FROM metadata.metrics
                WHERE source_id = :source_id
            """), conn, params={'source_id': source_id})
        else:
            metrics_df = pd.read_sql(text("""
                SELECT metric_id, source_id
                FROM metadata.metrics
                ORDER BY source_id, metric_id
            """), conn)

        if metrics_df.empty:
            print("  No metrics found to update.")
            return

        print(f"  Found {len(metrics_df)} metrics to calculate coverage for")

        # ── Step 2: Compute MIN/MAX year per metric ────────────
        # One query groups all metrics together — far more
        # efficient than one query per metric. PostgreSQL's
        # partition pruning ensures only relevant partitions
        # are scanned for each metric_id.
        if source_id:
            coverage_df = pd.read_sql(text("""
                SELECT
                    metric_id,
                    MIN(year) AS available_from,
                    MAX(year) AS available_to,
                    COUNT(*) AS observation_count
                FROM standardized.observations
                WHERE source_id = :source_id
                GROUP BY metric_id
            """), conn, params={'source_id': source_id})
        else:
            coverage_df = pd.read_sql(text("""
                SELECT
                    metric_id,
                    MIN(year) AS available_from,
                    MAX(year) AS available_to,
                    COUNT(*) AS observation_count
                FROM standardized.observations
                GROUP BY metric_id
            """), conn)

        print(
            f"  Found coverage data for "
            f"{len(coverage_df)} metrics in standardized.observations"
        )

        # ── Step 3: Update metadata.metrics ───────────────────
        # Row-by-row update is acceptable here because:
        # - This runs once after ingestion, not in a hot loop
        # - Volume is hundreds of metrics, not millions of rows
        # - Each UPDATE touches exactly one row (PK lookup)
        updated    = 0
        no_data    = 0
        unchanged  = 0

        for _, metric_row in metrics_df.iterrows():
            metric_id = metric_row['metric_id']

            # Find coverage for this metric.
            coverage = coverage_df[
                coverage_df['metric_id'] == metric_id
            ]

            if coverage.empty:
                # No observations exist for this metric yet.
                # This is expected for metrics that were seeded
                # but whose ingestion has not run yet.
                no_data += 1
                continue

            av_from = int(coverage.iloc[0]['available_from'])
            av_to   = int(coverage.iloc[0]['available_to'])
            count   = int(coverage.iloc[0]['observation_count'])

            # Check current stored values to detect changes.
            current = conn.execute(text("""
                SELECT available_from, available_to
                FROM metadata.metrics
                WHERE metric_id = :metric_id
            """), {'metric_id': metric_id}).fetchone()

            if (current and
                current[0] == av_from and
                current[1] == av_to):
                # No change — skip the UPDATE.
                unchanged += 1
                continue

            # Apply the update.
            conn.execute(text("""
                UPDATE metadata.metrics
                SET
                    available_from = :available_from,
                    available_to   = :available_to
                WHERE metric_id = :metric_id
            """), {
                'metric_id':     metric_id,
                'available_from': av_from,
                'available_to':   av_to,
            })
            updated += 1

        conn.commit()

        # ── Step 4: Summary ────────────────────────────────────
        print(f"\n  ✓ Updated:   {updated} metrics")
        print(f"  → Unchanged: {unchanged} metrics (coverage same)")
        print(f"  → No data:   {no_data} metrics (no observations yet)")

        # Show coverage summary for the updated metrics.
        if updated > 0:
            summary = pd.read_sql(text("""
                SELECT
                    source_id,
                    COUNT(*)                                    AS metrics,
                    MIN(available_from)                         AS earliest_year,
                    MAX(available_to)                           AS latest_year,
                    COUNT(*) FILTER (WHERE available_from IS NULL) AS missing_coverage
                FROM metadata.metrics
                WHERE available_from IS NOT NULL
                GROUP BY source_id
                ORDER BY source_id
            """), conn)

            print("\n  Coverage by source:")
            print(summary.to_string(index=False))

    print("\n✓ Coverage calculation complete.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Calculate and update available_from / available_to "
            "in metadata.metrics from actual data in "
            "standardized.observations."
        )
    )
    parser.add_argument(
        '--source',
        required = False,
        default  = None,
        help     = (
            "Source id to calculate coverage for. "
            "If not provided, all sources are updated."
        ),
    )

    args = parser.parse_args()
    calculate_coverage(source_id=args.source)


if __name__ == "__main__":
    main()
