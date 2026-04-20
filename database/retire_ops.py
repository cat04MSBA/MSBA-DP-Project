"""
database/retire_ops.py
======================
Archives old ops table data from Supabase to B2 and deletes
from Supabase to keep the database lean.

WHAT THIS FILE DOES:
    Migrates rows older than their retention window from four
    ops tables to B2 as JSON files, then deletes them from
    Supabase. Run monthly as a maintenance task.

RETENTION WINDOWS (from design document and our decisions):
    ops.checkpoints       → 10 days  in Supabase, then migrate to B2
    ops.quality_runs      → 2 years  in Supabase, then migrate to B2
    ops.rejection_summary → 2 years  in Supabase, then migrate to B2
    ops.metadata_changes  → 2 years  in Supabase, then migrate to B2
    ops.query_log         → 1 year   in Supabase, then migrate to B2
    ops.pipeline_runs     → NEVER migrated — kept in Supabase forever
                            At 84 rows/year it is negligible in size
                            and represents a genuinely useful permanent
                            audit trail.

WHY MIGRATE NOT DELETE:
    Every table uses "migrate to B2" not "delete." Data is never
    permanently destroyed — it moves to cheaper cold storage.
    This is consistent with the bronze layer philosophy: data
    is never deleted, only moved. B2 is 10x cheaper than
    Supabase per GB for data that is rarely queried.

WHY ops.pipeline_runs IS NEVER MIGRATED:
    At ~84 rows per year (7 sources × monthly runs × 12 months)
    the table stays tiny. It is the primary audit trail for the
    entire system — having it always queryable in Supabase is
    worth the trivial storage cost. Migrating it would mean
    losing the ability to query "when did World Bank last run
    successfully?" without fetching from B2.

B2 ARCHIVE PATH CONVENTION:
    ops/archive/{table_name}/{YYYY-MM}/{table_name}_{timestamp}.json
    Year-month subdirectory groups archives by month, making
    it easy to find data from a specific time period.

WHEN TO RUN:
    Monthly, after the master flow completes. Can be added as
    a final step in pipeline.py or run independently:
        python3 database/retire_ops.py
        python3 database/retire_ops.py --dry-run

DRY RUN MODE:
    --dry-run prints what would be migrated without touching
    Supabase or B2. Use to verify before running for real.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION
"""

import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

from database.connection import get_engine
from database.b2_upload import B2Client

engine = get_engine()


# ─────────────────────────────────────────────────────────────
# RETENTION CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Maps table name → (timestamp_column, retention_days)
# timestamp_column is the column used to determine age.
# retention_days is how long to keep in Supabase before migrating.
RETENTION_CONFIG = {
    'ops.checkpoints': (
        'checkpointed_at',
        10,     # 10 days — only needed for active restart recovery
    ),
    'ops.quality_runs': (
        'checked_at',
        730,    # 2 years — analytical value for trend detection
    ),
    'ops.rejection_summary': (
        'logged_at',
        730,    # 2 years — useful for per-source rejection rate trends
    ),
    'ops.metadata_changes': (
        'detected_at',
        730,    # 2 years — metadata governance audit trail
    ),
    'ops.query_log': (
        'requested_at',
        365,    # 1 year — researcher query patterns
    ),
    # ops.pipeline_runs intentionally omitted — never migrated
}


def retire_ops(dry_run: bool = False):
    """
    Migrate rows older than their retention window from ops tables
    to B2 and delete from Supabase.

    Args:
        dry_run: If True, print what would be migrated without
                 touching Supabase or B2.
    """
    b2  = B2Client()
    now = datetime.utcnow()

    print("=" * 60)
    print(f"retire_ops.py — {'DRY RUN' if dry_run else 'LIVE RUN'}")
    print(f"Started at: {now.isoformat()}")
    print("=" * 60)

    total_migrated = 0

    with engine.connect() as conn:

        for table, (ts_col, retention_days) in RETENTION_CONFIG.items():

            cutoff = now - timedelta(days=retention_days)

            print(f"\n── {table} ──")
            print(
                f"  Retention: {retention_days} days. "
                f"Cutoff: {cutoff.date().isoformat()}"
            )

            # ── Count rows to migrate ──────────────────────────
            count_row = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE {ts_col} < :cutoff
            """), {'cutoff': cutoff}).fetchone()

            count = count_row[0] if count_row else 0

            if count == 0:
                print(f"  → No rows to migrate.")
                continue

            print(f"  → {count:,} rows to migrate.")

            if dry_run:
                print(f"  [DRY RUN] Would migrate {count:,} rows to B2.")
                continue

            # ── Read rows to migrate ───────────────────────────
            # Read in chunks to avoid loading millions of rows
            # into memory at once. 10,000 rows per chunk.
            chunk_size = 10_000
            offset     = 0
            migrated   = 0

            while True:
                df_chunk = pd.read_sql(text(f"""
                    SELECT *
                    FROM {table}
                    WHERE {ts_col} < :cutoff
                    ORDER BY {ts_col}
                    LIMIT :limit OFFSET :offset
                """), conn, params={
                    'cutoff': cutoff,
                    'limit':  chunk_size,
                    'offset': offset,
                })

                if df_chunk.empty:
                    break

                # ── Write chunk to B2 ──────────────────────────
                # Archive path includes year-month for easy lookup.
                year_month = cutoff.strftime('%Y-%m')
                timestamp  = now.strftime('%Y%m%d_%H%M%S')
                table_name = table.replace('.', '_')
                b2_key = (
                    f"ops/archive/{table_name}/"
                    f"{year_month}/"
                    f"{table_name}_{timestamp}_"
                    f"offset{offset}.json"
                )

                # Serialize to JSON — convert datetime columns
                # to ISO strings for clean JSON output.
                json_bytes = df_chunk.to_json(
                    orient     = 'records',
                    date_format = 'iso',
                ).encode('utf-8')

                b2.upload(b2_key, json_bytes)
                print(
                    f"  ✓ Archived {len(df_chunk):,} rows → {b2_key}"
                )

                # ── Delete migrated rows from Supabase ─────────
                # Delete using the primary key of the chunk to
                # avoid a full table scan. Each ops table has a
                # SERIAL primary key — use it for precise deletion.
                pk_col = _get_pk_column(table)
                pk_values = df_chunk[pk_col].tolist()

                # Delete in sub-batches of 1000 to avoid
                # PostgreSQL IN clause limits.
                for i in range(0, len(pk_values), 1000):
                    sub_batch = pk_values[i:i + 1000]
                    placeholders = ', '.join(
                        f':pk_{j}' for j in range(len(sub_batch))
                    )
                    params = {f'pk_{j}': v for j, v in enumerate(sub_batch)}
                    conn.execute(text(f"""
                        DELETE FROM {table}
                        WHERE {pk_col} IN ({placeholders})
                    """), params)

                conn.commit()
                migrated += len(df_chunk)
                offset   += chunk_size

                if len(df_chunk) < chunk_size:
                    break

            print(f"  ✓ Migrated {migrated:,} rows total from {table}")
            total_migrated += migrated

    print(f"\n{'=' * 60}")
    print(
        f"retire_ops complete. "
        f"Total rows migrated: {total_migrated:,}. "
        f"{'DRY RUN — no changes made.' if dry_run else ''}"
    )


def _get_pk_column(table: str) -> str:
    """
    Return the primary key column name for a given ops table.
    All ops tables use SERIAL primary keys with consistent naming.
    """
    pk_map = {
        'ops.checkpoints':       'checkpoint_id',
        'ops.quality_runs':      'quality_run_id',
        'ops.rejection_summary': 'rejection_id',
        'ops.metadata_changes':  'change_id',
        'ops.query_log':         'query_id',
    }
    if table not in pk_map:
        raise ValueError(f"Unknown table for PK lookup: {table}")
    return pk_map[table]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Archive old ops table data from Supabase to B2 "
            "and delete from Supabase to keep the database lean."
        )
    )
    parser.add_argument(
        '--dry-run',
        action  = 'store_true',
        default = False,
        help    = (
            "Print what would be migrated without touching "
            "Supabase or B2. Use to verify before running live."
        ),
    )

    args = parser.parse_args()
    retire_ops(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
