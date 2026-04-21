"""
database/reset_source.py
========================
Resets a source for a clean re-run by deleting all ops table
entries and B2 files in the correct order, and setting
last_retrieved = NULL in metadata.sources.

WHEN TO USE:
    - A source ingestion or transformation failed partway through
      and you want to restart cleanly from scratch
    - You manually deleted B2 files and need to clear stale
      checkpoints that reference those files
    - You are debugging and want a fresh slate for one source

WHAT IT DELETES:
    1. ops.quality_runs         (child of pipeline_runs)
    2. ops.rejection_summary    (child of pipeline_runs)
    3. ops.checkpoints          (child of pipeline_runs)
    4. ops.pipeline_runs        (parent — deleted last)
    5. All B2 files under bronze/{source_id}/
    6. Sets metadata.sources.last_retrieved = NULL

WHAT IT DOES NOT DELETE:
    - metadata.country_codes    (seeded separately, not affected)
    - metadata.metrics          (seeded separately, not affected)
    - metadata.metric_codes     (seeded separately, not affected)
    - standardized.observations (silver layer data — use --silver
                                 flag to also clear silver data)

WHY FK ORDER MATTERS:
    ops.quality_runs and ops.rejection_summary reference
    ops.pipeline_runs via foreign key. Deleting pipeline_runs
    first raises a FK violation. Must delete children first.

SILVER FLAG:
    --silver also deletes standardized.observations rows for
    this source. Use with caution — this deletes actual data.
    Requires explicit --confirm flag when --silver is used.

USAGE:
    python3 -m database.reset_source --source openalex
    python3 -m database.reset_source --source world_bank --dry-run
    python3 -m database.reset_source --source oxford --silver --confirm

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION
"""

import argparse
from sqlalchemy import text
from database.connection import get_engine
from database.b2_upload import B2Client

engine = get_engine()


# Map source_id → B2 prefix for file deletion.
# Some sources use a different source_id in ops tables vs B2 path.
B2_PREFIX_MAP = {
    'world_bank': 'bronze/world_bank/',
    'imf':        'bronze/imf/',
    'openalex':   'bronze/openalex/',
    'oecd_msti':  'bronze/oecd_msti/',
    'oxford':     'bronze/oxford/',
    'wipo_ip':    'bronze/wipo_ip/',
    'pwt':        'bronze/pwt/',
}


def reset_source(source_id: str, dry_run: bool = False,
                 silver: bool = False, confirm: bool = False):
    """
    Reset a source for a clean re-run.

    Args:
        source_id: Source to reset. Must match metadata.sources.
        dry_run:   If True, print what would be deleted without
                   touching anything.
        silver:    If True, also delete standardized.observations
                   rows for this source.
        confirm:   Required when silver=True to prevent accidental
                   data deletion.
    """
    if source_id not in B2_PREFIX_MAP:
        print(
            f"✗ Unknown source_id: '{source_id}'. "
            f"Valid sources: {list(B2_PREFIX_MAP.keys())}"
        )
        return

    if silver and not confirm:
        print(
            f"✗ --silver requires --confirm to prevent accidental "
            f"data deletion. Re-run with both flags."
        )
        return

    print("=" * 60)
    print(f"RESET SOURCE: {source_id}")
    if dry_run:
        print("DRY RUN — no changes will be made")
    if silver:
        print("⚠ SILVER FLAG SET — will also delete observations")
    print("=" * 60)

    # ── Step 1: Count rows to delete ───────────────────────────
    with engine.connect() as conn:
        counts = {}
        for table in [
            'ops.quality_runs',
            'ops.rejection_summary',
            'ops.checkpoints',
            'ops.pipeline_runs',
        ]:
            row = conn.execute(text(
                f"SELECT COUNT(*) FROM {table} WHERE source_id = :sid"
            ), {'sid': source_id}).fetchone()
            counts[table] = row[0] if row else 0

        if silver:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM standardized.observations
                WHERE source_id = :sid
            """), {'sid': source_id}).fetchone()
            counts['standardized.observations'] = row[0] if row else 0

    # ── Step 2: Count B2 files ─────────────────────────────────
    b2     = B2Client()
    prefix = B2_PREFIX_MAP[source_id]
    try:
        b2_keys = b2.list(prefix)
    except Exception as e:
        print(f"  ⚠ Could not list B2 files: {e}")
        b2_keys = []

    # ── Step 3: Print what will be deleted ─────────────────────
    print(f"\nWill delete:")
    for table, count in counts.items():
        print(f"  {table}: {count:,} rows")
    print(f"  B2 files under {prefix}: {len(b2_keys)} files")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return

    # ── Step 4: Confirm ────────────────────────────────────────
    answer = input(f"\nProceed with reset of '{source_id}'? [y/N]: ").strip().lower()
    if answer not in ('y', 'yes'):
        print("Aborted.")
        return

    # ── Step 5: Delete ops table rows in FK order ──────────────
    with engine.connect() as conn:
        for table in [
            'ops.quality_runs',
            'ops.rejection_summary',
            'ops.checkpoints',
            'ops.pipeline_runs',
        ]:
            conn.execute(text(
                f"DELETE FROM {table} WHERE source_id = :sid"
            ), {'sid': source_id})
            print(f"  ✓ Deleted from {table}")

        # ── Step 6: Reset last_retrieved ───────────────────────
        conn.execute(text("""
            UPDATE metadata.sources
            SET last_retrieved = NULL
            WHERE source_id = :sid
        """), {'sid': source_id})
        print(f"  ✓ Reset last_retrieved = NULL")

        # ── Step 7: Delete silver layer rows (if --silver) ─────
        if silver:
            conn.execute(text("""
                DELETE FROM standardized.observations
                WHERE source_id = :sid
            """), {'sid': source_id})
            print(f"  ✓ Deleted from standardized.observations")

        conn.commit()

    # ── Step 8: Delete B2 files ────────────────────────────────
    deleted_b2 = 0
    for key in b2_keys:
        try:
            b2.client.delete_object(Bucket=b2.bucket, Key=key)
            deleted_b2 += 1
        except Exception as e:
            print(f"  ⚠ Could not delete B2 file {key}: {e}")

    print(f"  ✓ Deleted {deleted_b2} B2 files from {prefix}")

    print(f"\n✓ Reset complete for '{source_id}'.")
    print(f"  Run the flow to re-ingest from scratch:")
    print(f"  python3 -m orchestration.{source_id}_flow")


def main():
    parser = argparse.ArgumentParser(
        description="Reset a source for a clean re-run."
    )
    parser.add_argument(
        '--source', required=True,
        help="Source ID to reset (e.g. 'openalex', 'world_bank')",
    )
    parser.add_argument(
        '--dry-run', action='store_true', default=False,
        help="Print what would be deleted without making changes.",
    )
    parser.add_argument(
        '--silver', action='store_true', default=False,
        help=(
            "Also delete standardized.observations rows for this "
            "source. Requires --confirm."
        ),
    )
    parser.add_argument(
        '--confirm', action='store_true', default=False,
        help="Required when --silver is set.",
    )

    args = parser.parse_args()
    reset_source(
        source_id = args.source,
        dry_run   = args.dry_run,
        silver    = args.silver,
        confirm   = args.confirm,
    )


if __name__ == "__main__":
    main()
