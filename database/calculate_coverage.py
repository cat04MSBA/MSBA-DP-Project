"""
database/calculate_coverage.py
==============================
Computes and stores data coverage statistics in metadata.metrics
after each ingestion/transformation cycle.

WHAT THIS COMPUTES (per metric):
    available_from      → earliest year with at least one observation
    available_to        → latest year with at least one observation
    country_count       → number of distinct countries with data
    observation_count   → total rows across all countries and years
    missing_value_rate  → % of rows where value IS NULL or ''

WHY FIVE DIMENSIONS NOT JUST YEAR RANGE:
    Year range alone tells a researcher when a metric starts and
    ends, but not whether it is useful. A metric that exists from
    1990 to 2023 but covers only 12 countries, or has 40% missing
    values, is not useful for global comparisons. Showing all five
    dimensions in the Streamlit metric picker gives researchers the
    information they need before running a query. Computing them at
    query time over millions of rows is expensive — storing them
    once here makes every UI lookup a free single-row read.

WHY ONE QUERY PER SOURCE NOT ONE QUERY PER METRIC:
    GROUP BY metric_id in one query returns all five aggregates for
    all metrics of a source in a single round trip. For World Bank
    with 439 metrics, this is one query instead of 439. PostgreSQL
    partition pruning scans only partitions with data for that
    source, keeping the query fast even as the table grows.

WHY STORED IN metadata.metrics NOT A SEPARATE TABLE:
    Coverage stats are a property of a metric, not a separate
    entity. The Streamlit app already joins metadata.metrics for
    metric_name, unit, and category — adding five more columns
    adds zero extra joins. A separate coverage table would require
    an additional join on every metric picker query.

WHEN THIS RUNS:
    Called automatically at the end of each source flow, after
    transformation completes successfully. Also callable manually:
        python3 -m database.calculate_coverage
        python3 -m database.calculate_coverage --source world_bank

    Re-run manually after any ad-hoc data backfill or after
    running add_metric.py --action yes to ingest parked rows.

PERFORMANCE:
    One GROUP BY query per source. PostgreSQL partition pruning
    ensures only relevant year partitions are scanned per metric.
    The UPDATE loop iterates over ~500 metrics maximum, each
    touching a single indexed row (PK lookup on metric_id).
    Total runtime: under 30 seconds for all sources combined.
"""

import argparse
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


def calculate_coverage(source_id: str = None) -> None:
    """
    Compute and update all five coverage fields in metadata.metrics
    for every metric of the given source, or all sources if None.

    Args:
        source_id: Limit update to this source. If None, all sources.
    """
    print("=" * 55)
    print("Calculating metric coverage...")
    if source_id:
        print(f"  Source: {source_id}")
    print("=" * 55)

    with engine.connect() as conn:

        # ── Step 1: Load metrics to update ────────────────────
        # Pull just metric_id so we know which metrics to expect
        # results for. Filtering by source avoids loading all
        # metrics when only one source changed.
        if source_id:
            rows = conn.execute(text("""
                SELECT metric_id
                FROM metadata.metrics
                WHERE source_id = :source_id
                ORDER BY metric_id
            """), {'source_id': source_id}).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT metric_id
                FROM metadata.metrics
                ORDER BY metric_id
            """)).fetchall()

        if not rows:
            print("  No metrics found.")
            return

        all_metric_ids = {r[0] for r in rows}
        print(f"  {len(all_metric_ids)} metrics to update")

        # ── Step 2: Compute all five aggregates in one query ───
        # One GROUP BY query returns all aggregates for all metrics
        # of this source in a single round trip. PostgreSQL's
        # partition pruning ensures only relevant year partitions
        # are scanned for each metric_id group.
        #
        # missing_value_rate: count rows where value is NULL or
        # empty string, divide by total, multiply by 100.
        # NULLIF avoids division by zero on metrics with no rows
        # (though such metrics would not appear in observations).
        if source_id:
            agg_rows = conn.execute(text("""
                SELECT
                    metric_id,
                    MIN(year)                                   AS available_from,
                    MAX(year)                                   AS available_to,
                    COUNT(DISTINCT country_iso3)::SMALLINT      AS country_count,
                    COUNT(*)::INTEGER                           AS observation_count,
                    ROUND(
                        100.0 *
                        COUNT(*) FILTER (
                            WHERE value IS NULL OR value = ''
                        ) /
                        NULLIF(COUNT(*), 0),
                        2
                    )                                           AS missing_value_rate
                FROM standardized.observations
                WHERE source_id = :source_id
                GROUP BY metric_id
            """), {'source_id': source_id}).fetchall()
        else:
            agg_rows = conn.execute(text("""
                SELECT
                    metric_id,
                    MIN(year)                                   AS available_from,
                    MAX(year)                                   AS available_to,
                    COUNT(DISTINCT country_iso3)::SMALLINT      AS country_count,
                    COUNT(*)::INTEGER                           AS observation_count,
                    ROUND(
                        100.0 *
                        COUNT(*) FILTER (
                            WHERE value IS NULL OR value = ''
                        ) /
                        NULLIF(COUNT(*), 0),
                        2
                    )                                           AS missing_value_rate
                FROM standardized.observations
                GROUP BY metric_id
            """)).fetchall()

        # Index results by metric_id for O(1) lookup in the
        # update loop below — avoids scanning the results list
        # once per metric (O(n²) for 439+ metrics).
        agg = {
            r[0]: {
                'available_from':     int(r[1]) if r[1] is not None else None,
                'available_to':       int(r[2]) if r[2] is not None else None,
                'country_count':      int(r[3]) if r[3] is not None else None,
                'observation_count':  int(r[4]) if r[4] is not None else None,
                'missing_value_rate': float(r[5]) if r[5] is not None else None,
            }
            for r in agg_rows
        }

        print(f"  Coverage data found for {len(agg)} metrics")

        # ── Step 3: Update metadata.metrics ───────────────────
        # Row-by-row UPDATE is acceptable here:
        #   - Runs once after ingestion, not in a hot loop
        #   - Each UPDATE touches exactly one indexed row (PK)
        #   - Volume is hundreds of metrics, never millions
        updated   = 0
        no_data   = 0
        unchanged = 0

        for metric_id in sorted(all_metric_ids):
            if metric_id not in agg:
                # No observations exist yet for this metric.
                # Expected for metrics seeded but not yet ingested.
                no_data += 1
                continue

            new = agg[metric_id]

            # Read current stored values to detect changes.
            # Skipping unchanged metrics avoids unnecessary writes
            # and keeps the UPDATE audit trail clean.
            current = conn.execute(text("""
                SELECT available_from, available_to,
                       country_count, observation_count,
                       missing_value_rate
                FROM metadata.metrics
                WHERE metric_id = :metric_id
            """), {'metric_id': metric_id}).fetchone()

            # Consider unchanged if all five fields match.
            # Use a small epsilon for the float comparison on
            # missing_value_rate to tolerate rounding differences.
            if (
                current is not None              and
                current[0] == new['available_from']    and
                current[1] == new['available_to']      and
                current[2] == new['country_count']     and
                current[3] == new['observation_count'] and
                current[4] is not None                 and
                abs(float(current[4]) - new['missing_value_rate']) < 0.01
            ):
                unchanged += 1
                continue

            conn.execute(text("""
                UPDATE metadata.metrics
                SET
                    available_from     = :available_from,
                    available_to       = :available_to,
                    country_count      = :country_count,
                    observation_count  = :observation_count,
                    missing_value_rate = :missing_value_rate
                WHERE metric_id = :metric_id
            """), {
                'metric_id':          metric_id,
                'available_from':     new['available_from'],
                'available_to':       new['available_to'],
                'country_count':      new['country_count'],
                'observation_count':  new['observation_count'],
                'missing_value_rate': new['missing_value_rate'],
            })
            updated += 1

        conn.commit()

        # ── Step 4: Print summary ──────────────────────────────
        print(f"\n  Updated:   {updated} metrics")
        print(f"  Unchanged: {unchanged} metrics")
        print(f"  No data:   {no_data} metrics (not yet ingested)")

        if updated > 0:
            summary = conn.execute(text("""
                SELECT
                    source_id,
                    COUNT(*)                          AS metrics,
                    MIN(available_from)               AS first_year,
                    MAX(available_to)                 AS last_year,
                    ROUND(AVG(country_count), 0)::INT AS avg_countries,
                    ROUND(AVG(missing_value_rate), 2) AS avg_missing_pct,
                    SUM(observation_count)            AS total_observations
                FROM metadata.metrics
                WHERE available_from IS NOT NULL
                GROUP BY source_id
                ORDER BY source_id
            """)).fetchall()

            print("\n  Coverage by source:")
            print(
                f"  {'source':<15} {'metrics':>7} {'years':>13} "
                f"{'avg_countries':>13} {'missing%':>9} {'total_obs':>12}"
            )
            print("  " + "─" * 73)
            for row in summary:
                src, metrics, first, last, avg_c, avg_m, total_obs = row
                print(
                    f"  {str(src):<15} {metrics:>7} "
                    f"{str(first)+'-'+str(last):>13} "
                    f"{str(avg_c) if avg_c else '-':>13} "
                    f"{str(avg_m) if avg_m else '-':>9} "
                    f"{str(total_obs) if total_obs else '-':>12}"
                )

    print("\n✓ Coverage calculation complete.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compute and store coverage statistics "
            "(year range, country count, missing value rate, "
            "observation count) in metadata.metrics. "
            "Run automatically after each source flow completes, "
            "or manually after ad-hoc backfills."
        )
    )
    parser.add_argument(
        '--source',
        required = False,
        default  = None,
        help     = (
            "source_id to update (e.g. world_bank). "
            "If omitted, all sources are updated."
        ),
    )
    args = parser.parse_args()
    calculate_coverage(source_id=args.source)


if __name__ == "__main__":
    main()
