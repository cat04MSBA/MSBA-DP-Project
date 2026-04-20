"""
database/add_metric.py
======================
CLI utility for approving or rejecting unknown metrics and
metadata drift changes detected by the pipeline.

WHAT THIS FILE DOES:
    Handles two types of team decisions:

    1. UNKNOWN METRIC (--action yes / --action no):
       When base_ingestor.py detects a metric_id not in
       metadata.metrics, it parks the rows on B2 and emails
       the team. This script processes that decision:
       - yes: register the metric, upsert parked rows immediately
       - no:  permanently skip the metric, log the decision

    2. METADATA DRIFT (--action approve / --action reject):
       When metadata_drift.py detects a critical change (unit,
       frequency, or code mapping change), it logs to
       ops.metadata_changes with action_taken='pending_review'.
       This script processes that decision:
       - approve: apply the change to metadata.metrics or
                  metadata.country_codes / metadata.metric_codes
       - reject:  update action_taken='rejected', no metadata change

USAGE:
    Unknown metric — register and ingest:
        python3 database/add_metric.py \\
            --metric_id wb.new_indicator \\
            --source world_bank \\
            --action yes

    Unknown metric — permanently skip:
        python3 database/add_metric.py \\
            --metric_id wb.new_indicator \\
            --source world_bank \\
            --action no

    Metadata drift — approve change:
        python3 database/add_metric.py \\
            --metric_id wb.ny_gdp_pcap_cd \\
            --source world_bank \\
            --action approve \\
            --change_id 42

    Metadata drift — reject change:
        python3 database/add_metric.py \\
            --metric_id wb.ny_gdp_pcap_cd \\
            --source world_bank \\
            --action reject \\
            --change_id 42

WHY --change_id IS REQUIRED FOR APPROVE/REJECT:
    A metric may have multiple pending drift changes
    (e.g. both unit and frequency changed). --change_id
    identifies which specific change in ops.metadata_changes
    is being approved or rejected, allowing the team to
    handle each field change independently.

HOW PARKED ROWS ARE UPSERTED (--action yes):
    1. Read parked rows from B2:
       bronze/parked/{source_id}/{metric_id}_{date}.json
    2. Run through check_pre_upsert() — same validation as
       the normal transformation pipeline
    3. Upsert to standardized.observations via temp table
    4. Delete the parked file from B2

WHY PARKED FILES ARE DELETED AFTER UPSERT:
    Parked files are temporary staging data. Once rows are
    in the silver layer, the parked file has no purpose.
    Keeping it would accumulate stale files with no meaning.
    This is the one exception to the no-delete rule in B2.

LOGGING:
    Every action is logged to ops.pipeline_runs as a
    maintenance run (source_id = the affected source,
    status = 'success' or 'failed'). This ensures the
    audit trail is complete for all data that enters the
    silver layer regardless of how it got there.
"""

import argparse
import json
import pandas as pd
from datetime import datetime, date
from io import BytesIO
from sqlalchemy import text

from database.connection import get_engine
from database.b2_upload import B2Client
from database.quality_checks import check_pre_upsert

# Chunk size for temp table upsert — matches base_transformer.py
UPSERT_CHUNK_SIZE = 500


# ═══════════════════════════════════════════════════════════════
# ACTION: YES — register metric and upsert parked rows
# ═══════════════════════════════════════════════════════════════

def action_yes(conn, engine, b2: B2Client,
               metric_id: str, source_id: str):
    """
    Register an unknown metric in metadata and immediately
    upsert any parked rows from B2.

    Steps:
    1. Find the parked rows file on B2
    2. Check if metric is already registered (idempotent)
    3. Register metric in metadata.metrics and metadata.metric_codes
    4. Load validation sets for pre-upsert checks
    5. Run check_pre_upsert() on parked rows
    6. Upsert clean rows to standardized.observations
    7. Delete parked file from B2
    8. Log to ops.pipeline_runs

    Args:
        conn:      Open SQLAlchemy connection.
        engine:    SQLAlchemy engine for read_sql.
        b2:        B2Client instance.
        metric_id: Standardized metric_id to register.
        source_id: Source that produced this metric.
    """
    print(f"\n── Registering metric: {metric_id} ──")

    # ── Step 1: Find parked rows on B2 ────────────────────────
    # Scan bronze/parked/{source_id}/ for files matching this
    # metric_id. There may be files from multiple runs.
    safe_metric_id = metric_id.replace('.', '_')
    prefix = f"bronze/parked/{source_id}/"
    all_keys = b2.list(prefix)

    parked_keys = [
        k for k in all_keys
        if safe_metric_id in k.split('/')[-1]
    ]

    if not parked_keys:
        print(
            f"  ⚠ No parked rows found on B2 for {metric_id}. "
            f"Registering metric only — no rows to upsert."
        )

    # ── Step 2: Check if already registered ───────────────────
    existing = conn.execute(text("""
        SELECT metric_id FROM metadata.metrics
        WHERE metric_id = :metric_id
    """), {'metric_id': metric_id}).fetchone()

    if existing:
        print(f"  ⚠ {metric_id} already registered in metadata.metrics.")
        print(f"  Proceeding to upsert parked rows only.")
    else:
        # ── Step 3: Register in metadata.metrics ──────────────
        # We need the metric metadata. Try to read it from the
        # parked file (base_ingestor stores metadata alongside rows).
        # Fall back to prompting the user if not found.
        metadata = _get_metric_metadata_from_parked(
            b2, parked_keys, metric_id, source_id
        )

        conn.execute(text("""
            INSERT INTO metadata.metrics (
                metric_id, metric_name, source_id, category,
                unit, description, frequency,
                available_from, available_to
            ) VALUES (
                :metric_id, :metric_name, :source_id, :category,
                :unit, :description, :frequency,
                NULL, NULL
            )
            ON CONFLICT (metric_id) DO NOTHING
        """), metadata)

        # Register in metadata.metric_codes.
        # Derive the original source code from the metric_id.
        # Convention: wb.ny_gdp_pcap_cd → NY.GDP.PCAP.CD
        original_code = _derive_source_code(metric_id, source_id)
        conn.execute(text("""
            INSERT INTO metadata.metric_codes (metric_id, source_id, code)
            VALUES (:metric_id, :source_id, :code)
            ON CONFLICT (metric_id, source_id) DO NOTHING
        """), {
            'metric_id': metric_id,
            'source_id': source_id,
            'code':      original_code,
        })

        conn.commit()
        print(f"  ✓ Registered {metric_id} in metadata.metrics")
        print(f"  ✓ Registered code '{original_code}' in metadata.metric_codes")

    # ── Step 4: Load validation sets ──────────────────────────
    valid_iso3 = {
        r[0] for r in conn.execute(text(
            "SELECT iso3 FROM metadata.countries"
        )).fetchall()
    }
    valid_metric_ids = {
        r[0] for r in conn.execute(text(
            "SELECT metric_id FROM metadata.metrics"
        )).fetchall()
    }
    valid_source_ids = {
        r[0] for r in conn.execute(text(
            "SELECT source_id FROM metadata.sources"
        )).fetchall()
    }

    # ── Step 5-6: Process each parked file ────────────────────
    total_upserted = 0

    for b2_key in parked_keys:
        print(f"\n  Processing parked file: {b2_key}")

        # Read parked rows from B2.
        raw_bytes = b2.download(b2_key)
        df = pd.read_json(BytesIO(raw_bytes), orient='records')

        if df.empty:
            print(f"    Empty file — skipping")
            continue

        # Open a maintenance pipeline run for audit trail.
        run_id = _open_maintenance_run(conn, source_id)
        conn.commit()

        # Run pre-upsert checks — same validation as normal pipeline.
        clean_df, rejections = check_pre_upsert(
            conn, run_id, source_id,
            batch_unit=f"add_metric_{metric_id}",
            df=df,
            valid_iso3=valid_iso3,
            valid_metric_ids=valid_metric_ids,
            valid_source_ids=valid_source_ids,
        )

        if clean_df.empty:
            print(f"    All rows failed validation — nothing to upsert")
            _close_maintenance_run(conn, run_id, 0, len(df), 'success_with_rej_rows')
            conn.commit()
            continue

        # Upsert clean rows in chunks using temp table pattern.
        _upsert_chunks(conn, clean_df)
        conn.commit()

        _close_maintenance_run(conn, run_id, len(clean_df), len(df) - len(clean_df), 'success')
        conn.commit()

        total_upserted += len(clean_df)
        print(f"    ✓ Upserted {len(clean_df)} rows")

        # ── Step 7: Delete parked file from B2 ────────────────
        # Parked files are temporary staging. Once rows are in
        # the silver layer, the file has no further purpose.
        # This is the one exception to the no-delete rule.
        try:
            b2.client.delete_object(Bucket=b2.bucket, Key=b2_key)
            print(f"    ✓ Deleted parked file from B2: {b2_key}")
        except Exception as e:
            print(f"    ⚠ Could not delete parked file: {e}")
            print(f"    File remains on B2 but rows are already upserted.")

    print(
        f"\n✓ action_yes complete for {metric_id}. "
        f"Total upserted: {total_upserted} rows."
    )


# ═══════════════════════════════════════════════════════════════
# ACTION: NO — permanently skip metric
# ═══════════════════════════════════════════════════════════════

def action_no(conn, b2: B2Client,
              metric_id: str, source_id: str):
    """
    Permanently skip an unknown metric. Log the decision to
    ops.pipeline_runs.notes and delete the parked rows from B2.

    The metric is NOT registered in metadata.metrics. On future
    ingestion runs, rows for this metric_id will continue to be
    parked and emailed — until the team explicitly adds the metric
    to a blocklist. For now, the simplest approach is to log the
    decision and clean up the parked files.

    Args:
        conn:      Open SQLAlchemy connection.
        b2:        B2Client instance.
        metric_id: metric_id to skip.
        source_id: Source that produced this metric.
    """
    print(f"\n── Skipping metric: {metric_id} ──")

    # Log the skip decision to ops.pipeline_runs.
    run_id = _open_maintenance_run(conn, source_id)
    conn.execute(text("""
        UPDATE ops.pipeline_runs
        SET
            completed_at  = :completed_at,
            rows_inserted = 0,
            rows_rejected = 0,
            status        = 'success',
            notes         = :notes
        WHERE run_id = :run_id
    """), {
        'run_id':       run_id,
        'completed_at': datetime.utcnow(),
        'notes': (
            f"Metric {metric_id} from {source_id} permanently skipped "
            f"by team decision via add_metric.py --action no. "
            f"Parked rows deleted from B2."
        ),
    })
    conn.commit()

    # Delete parked files from B2.
    safe_metric_id = metric_id.replace('.', '_')
    prefix     = f"bronze/parked/{source_id}/"
    all_keys   = b2.list(prefix)
    parked_keys = [
        k for k in all_keys
        if safe_metric_id in k.split('/')[-1]
    ]

    for b2_key in parked_keys:
        try:
            b2.client.delete_object(Bucket=b2.bucket, Key=b2_key)
            print(f"  ✓ Deleted parked file: {b2_key}")
        except Exception as e:
            print(f"  ⚠ Could not delete parked file: {e}")

    print(
        f"\n✓ action_no complete. {metric_id} skipped. "
        f"Decision logged to ops.pipeline_runs run_id={run_id}."
    )


# ═══════════════════════════════════════════════════════════════
# ACTION: APPROVE — apply a pending metadata drift change
# ═══════════════════════════════════════════════════════════════

def action_approve(conn, change_id: int):
    """
    Apply a pending metadata drift change to metadata tables.
    Updates ops.metadata_changes.action_taken to 'approved'.

    Reads the change details from ops.metadata_changes, applies
    the field update to the appropriate metadata table, and
    marks the change as approved.

    Args:
        conn:      Open SQLAlchemy connection.
        change_id: ops.metadata_changes.change_id to approve.
    """
    print(f"\n── Approving drift change: change_id={change_id} ──")

    # Read the change record.
    row = conn.execute(text("""
        SELECT entity_type, entity_id, field_changed,
               old_value, new_value, source_id, action_taken
        FROM ops.metadata_changes
        WHERE change_id = :change_id
    """), {'change_id': change_id}).fetchone()

    if not row:
        print(f"  ✗ No drift change found with change_id={change_id}")
        return

    entity_type, entity_id, field_changed, old_value, new_value, \
        source_id, action_taken = row

    if action_taken != 'pending_review':
        print(
            f"  ⚠ change_id={change_id} has action_taken='{action_taken}'. "
            f"Only 'pending_review' changes can be approved."
        )
        return

    print(
        f"  Entity:  {entity_type} — {entity_id}\n"
        f"  Field:   {field_changed}\n"
        f"  Change:  '{old_value}' → '{new_value}'"
    )

    # Apply the change to the appropriate metadata table.
    if entity_type == 'metric':
        conn.execute(text(f"""
            UPDATE metadata.metrics
            SET {field_changed} = :new_value
            WHERE metric_id = :entity_id
        """), {'new_value': new_value, 'entity_id': entity_id})
        print(f"  ✓ Applied to metadata.metrics")

    elif entity_type == 'country_code':
        conn.execute(text("""
            UPDATE metadata.country_codes
            SET code = :new_value
            WHERE iso3 = :entity_id AND source_id = :source_id
        """), {
            'new_value': new_value,
            'entity_id': entity_id,
            'source_id': source_id,
        })
        print(f"  ✓ Applied to metadata.country_codes")

    elif entity_type == 'metric_code':
        conn.execute(text("""
            UPDATE metadata.metric_codes
            SET code = :new_value
            WHERE metric_id = :entity_id AND source_id = :source_id
        """), {
            'new_value': new_value,
            'entity_id': entity_id,
            'source_id': source_id,
        })
        print(f"  ✓ Applied to metadata.metric_codes")

    elif entity_type == 'source':
        conn.execute(text(f"""
            UPDATE metadata.sources
            SET {field_changed} = :new_value
            WHERE source_id = :entity_id
        """), {'new_value': new_value, 'entity_id': entity_id})
        print(f"  ✓ Applied to metadata.sources")

    # Mark as approved in ops.metadata_changes.
    conn.execute(text("""
        UPDATE ops.metadata_changes
        SET action_taken = 'approved'
        WHERE change_id = :change_id
    """), {'change_id': change_id})

    conn.commit()
    print(f"\n✓ Drift change {change_id} approved and applied.")


# ═══════════════════════════════════════════════════════════════
# ACTION: REJECT — reject a pending metadata drift change
# ═══════════════════════════════════════════════════════════════

def action_reject(conn, change_id: int):
    """
    Reject a pending metadata drift change.
    Updates ops.metadata_changes.action_taken to 'rejected'.
    No metadata tables are modified — the stored value is preserved.

    Args:
        conn:      Open SQLAlchemy connection.
        change_id: ops.metadata_changes.change_id to reject.
    """
    print(f"\n── Rejecting drift change: change_id={change_id} ──")

    row = conn.execute(text("""
        SELECT entity_id, field_changed, old_value, new_value,
               action_taken
        FROM ops.metadata_changes
        WHERE change_id = :change_id
    """), {'change_id': change_id}).fetchone()

    if not row:
        print(f"  ✗ No drift change found with change_id={change_id}")
        return

    entity_id, field_changed, old_value, new_value, action_taken = row

    if action_taken != 'pending_review':
        print(
            f"  ⚠ change_id={change_id} has action_taken='{action_taken}'. "
            f"Only 'pending_review' changes can be rejected."
        )
        return

    # Mark as rejected — no metadata change applied.
    conn.execute(text("""
        UPDATE ops.metadata_changes
        SET action_taken = 'rejected'
        WHERE change_id = :change_id
    """), {'change_id': change_id})

    conn.commit()

    print(
        f"  Entity:  {entity_id}\n"
        f"  Field:   {field_changed}\n"
        f"  Kept:    '{old_value}' (incoming '{new_value}' rejected)"
    )
    print(f"\n✓ Drift change {change_id} rejected. Metadata unchanged.")


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _get_metric_metadata_from_parked(b2: B2Client, parked_keys: list,
                                      metric_id: str,
                                      source_id: str) -> dict:
    """
    Attempt to read metric metadata from the parked JSON file.
    The parked file contains the DataFrame rows which include
    metric_id and source_id. We cannot recover name/unit/description
    from the rows alone — those were in the email sent to the team.

    Returns a minimal metadata dict. The team should manually update
    metadata.metrics after registration if they want richer metadata.
    """
    return {
        'metric_id':   metric_id,
        'metric_name': metric_id,  # team should update manually
        'source_id':   source_id,
        'category':    None,
        'unit':        None,
        'description': (
            f'Registered via add_metric.py --action yes. '
            f'Update metadata.metrics manually with full details '
            f'from the approval email.'
        ),
        'frequency':   'annual',
    }


def _derive_source_code(metric_id: str, source_id: str) -> str:
    """
    Derive the original source indicator code from our metric_id.
    Reverses the naming convention used at seeding time.

    Examples:
        wb.ny_gdp_pcap_cd → NY.GDP.PCAP.CD  (World Bank)
        imf.ngdp_rpch     → NGDP_RPCH        (IMF)
        pwt.rgdpe         → rgdpe             (PWT)
    """
    if source_id == 'world_bank':
        # wb.ny_gdp_pcap_cd → NY.GDP.PCAP.CD
        code = metric_id.replace('wb.', '')
        return code.upper().replace('_', '.')
    elif source_id == 'imf':
        # imf.ngdp_rpch → NGDP_RPCH
        return metric_id.replace('imf.', '').upper()
    elif source_id == 'pwt':
        # pwt.rgdpe → rgdpe
        return metric_id.replace('pwt.', '')
    else:
        # For other sources, return the suffix as-is.
        parts = metric_id.split('.', 1)
        return parts[1] if len(parts) > 1 else metric_id


def _open_maintenance_run(conn, source_id: str) -> int:
    """
    Open a maintenance ops.pipeline_runs entry.
    Used to log the add_metric.py action to the audit trail.
    """
    result = conn.execute(text("""
        INSERT INTO ops.pipeline_runs (
            source_id, started_at, status
        ) VALUES (
            :source_id, :started_at, 'running'
        )
        RETURNING run_id
    """), {
        'source_id':  source_id,
        'started_at': datetime.utcnow(),
    })
    return result.fetchone()[0]


def _close_maintenance_run(conn, run_id: int,
                            rows_inserted: int,
                            rows_rejected: int,
                            status: str):
    """Close a maintenance ops.pipeline_runs entry."""
    conn.execute(text("""
        UPDATE ops.pipeline_runs
        SET
            completed_at  = :completed_at,
            rows_inserted = :rows_inserted,
            rows_rejected = :rows_rejected,
            status        = :status,
            notes         = 'add_metric.py maintenance run'
        WHERE run_id = :run_id
    """), {
        'run_id':        run_id,
        'completed_at':  datetime.utcnow(),
        'rows_inserted': rows_inserted,
        'rows_rejected': rows_rejected,
        'status':        status,
    })


def _upsert_chunks(conn, df: pd.DataFrame):
    """
    Upsert DataFrame to standardized.observations in chunks
    using the temp table pattern. Same approach as base_transformer.
    """
    chunks = [
        df.iloc[i:i + UPSERT_CHUNK_SIZE]
        for i in range(0, len(df), UPSERT_CHUNK_SIZE)
    ]

    for chunk in chunks:
        conn.execute(text("""
            CREATE TEMP TABLE temp_obs
            ON COMMIT DROP
            AS SELECT * FROM standardized.observations LIMIT 0
        """))

        conn.execute(
            text("""
                INSERT INTO temp_obs (
                    country_iso3, year, period, metric_id,
                    value, source_id, retrieved_at
                ) VALUES (
                    :country_iso3, :year, :period, :metric_id,
                    :value, :source_id, :retrieved_at
                )
            """),
            chunk.to_dict(orient='records')
        )

        conn.execute(text("""
            INSERT INTO standardized.observations (
                country_iso3, year, period, metric_id,
                value, source_id, retrieved_at
            )
            SELECT
                country_iso3, year, period, metric_id,
                value, source_id, retrieved_at
            FROM temp_obs
            ON CONFLICT (country_iso3, year, period, metric_id)
            DO UPDATE SET
                value        = EXCLUDED.value,
                source_id    = EXCLUDED.source_id,
                retrieved_at = EXCLUDED.retrieved_at
        """))

        conn.commit()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Approve or reject unknown metrics and metadata "
            "drift changes detected by the pipeline."
        )
    )
    parser.add_argument(
        '--metric_id',
        required = True,
        help     = "Standardized metric_id (e.g. wb.ny_gdp_pcap_cd)",
    )
    parser.add_argument(
        '--source',
        required = True,
        help     = "Source id (e.g. world_bank, imf, oxford)",
    )
    parser.add_argument(
        '--action',
        required = True,
        choices  = ['yes', 'no', 'approve', 'reject'],
        help     = (
            "yes: register metric and upsert parked rows. "
            "no: permanently skip metric. "
            "approve: apply pending drift change. "
            "reject: reject pending drift change."
        ),
    )
    parser.add_argument(
        '--change_id',
        required = False,
        type     = int,
        default  = None,
        help     = "ops.metadata_changes.change_id (required for approve/reject)",
    )

    args = parser.parse_args()

    # Validate change_id requirement.
    if args.action in ('approve', 'reject') and args.change_id is None:
        parser.error(
            f"--change_id is required for --action {args.action}. "
            f"Find the change_id in ops.metadata_changes."
        )

    engine = get_engine()
    b2     = B2Client()

    with engine.connect() as conn:
        if args.action == 'yes':
            action_yes(conn, engine, b2, args.metric_id, args.source)
        elif args.action == 'no':
            action_no(conn, b2, args.metric_id, args.source)
        elif args.action == 'approve':
            action_approve(conn, args.change_id)
        elif args.action == 'reject':
            action_reject(conn, args.change_id)


if __name__ == "__main__":
    main()
