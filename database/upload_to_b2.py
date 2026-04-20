"""
database/upload_to_b2.py
========================
Utility script for uploading manual source files to B2 and
triggering the corresponding Prefect flow.

WHAT THIS FILE DOES:
    Two things in one command:
    1. Upload the file to the correct B2 path
    2. Trigger the corresponding Prefect flow

    This is the entry point for all manual source updates —
    Oxford, WIPO, and PWT. Team members never upload files
    directly to B2 or manually trigger flows — this script
    handles both in one step.

USAGE:
    Oxford (requires --year):
        python3 database/upload_to_b2.py \\
            --source oxford \\
            --file /path/to/oxford_2025.xlsx \\
            --year 2025

    WIPO:
        python3 database/upload_to_b2.py \\
            --source wipo \\
            --file /path/to/wipo_ai_patents.csv

    PWT:
        python3 database/upload_to_b2.py \\
            --source pwt \\
            --file /path/to/pwt110.dta

B2 PATHS:
    Oxford: bronze/oxford/oxford_{year}_{date}.xlsx
    WIPO:   bronze/wipo_ip/full_file_{date}.csv
    PWT:    bronze/pwt/pwt110.dta

WHY ONE SCRIPT FOR ALL MANUAL SOURCES:
    A single entry point with --source routing is simpler than
    three separate upload scripts. The team member runs one
    command regardless of which source they are uploading.
    Adding a new manual source means adding one elif block here.

WHY THE FLOW IS TRIGGERED DIRECTLY (not via Prefect API):
    Calling the flow function directly from Python runs it as
    a Prefect flow run — Prefect tracks it, logs it, shows it
    in the UI, and applies retry logic. This works without
    requiring a separate Prefect deployment for each manual flow.

    If in future the team wants to trigger flows remotely (from
    a different machine, or via a web hook), replace the direct
    function call with:
        from prefect.deployments import run_deployment
        run_deployment(name="oxford-flow/manual", timeout=0)
    This change is isolated to this file only.

WHY --year IS REQUIRED FOR OXFORD (not for others):
    Oxford publishes one file per year. The year is part of the
    B2 filename so the ingestion script can identify which year
    each file belongs to. WIPO and PWT are single full-history
    files with no year component in the filename.

LOGGING:
    Every upload is logged to B2 at:
        logs/uploads/upload_{source}_{date}.json
    This provides a permanent audit trail of when each file
    was uploaded and by whom (hostname is captured).
    Consistent with design document Section 14.
"""

import argparse
import json
import os
import socket
from datetime import date, datetime
from pathlib import Path

from database.b2_upload import B2Client
from database.email_utils import send_email


# ═══════════════════════════════════════════════════════════════
# SUPPORTED MANUAL SOURCES
# ═══════════════════════════════════════════════════════════════

SUPPORTED_SOURCES = {
    'oxford': {
        'requires_year': True,
        'description':   'Oxford Insights GAIRI (.xlsx)',
        'extension':     '.xlsx',
    },
    'wipo': {
        'requires_year': False,
        'description':   'WIPO AI Patent Statistics (.csv)',
        'extension':     '.csv',
        'source_id':     'wipo_ip',  # maps to source_id in our system
    },
    'pwt': {
        'requires_year': False,
        'description':   'Penn World Tables (.dta)',
        'extension':     '.dta',
    },
}


# ═══════════════════════════════════════════════════════════════
# B2 KEY CONSTRUCTION
# ═══════════════════════════════════════════════════════════════

def get_b2_key(source: str, year: str = None) -> str:
    """
    Return the B2 object key for a given source and optional year.
    Must match the key expected by the ingestion script exactly.

    Args:
        source: Source name ('oxford', 'wipo', 'pwt').
        year:   4-digit year string. Required for Oxford only.

    Returns:
        Full B2 path string.
    """
    today = date.today().isoformat()

    if source == 'oxford':
        return f"bronze/oxford/oxford_{year}_{today}.xlsx"
    elif source == 'wipo':
        return f"bronze/wipo_ip/full_file_{today}.csv"
    elif source == 'pwt':
        # PWT .dta stored at a fixed path — not date-stamped.
        # The ingestion script always reads from this fixed path.
        # When a new PWT version is released, this file is
        # overwritten. The ingestion script detects the new
        # version via last_retrieved comparison.
        return "bronze/pwt/pwt110.dta"
    else:
        raise ValueError(f"Unknown source: {source}")


# ═══════════════════════════════════════════════════════════════
# FLOW TRIGGER
# ═══════════════════════════════════════════════════════════════

def trigger_flow(source: str):
    """
    Trigger the Prefect flow for the given source.

    Calls the flow function directly — Prefect tracks it as a
    flow run in the UI with full logging and retry handling.

    WHY DIRECT CALL NOT run_deployment():
        Direct call works without a Prefect deployment setup.
        The flow is still tracked by Prefect as a flow run.
        To switch to remote triggering via Prefect API, replace
        the direct call with run_deployment() here — no other
        files need to change.

    Args:
        source: Source name ('oxford', 'wipo', 'pwt').
    """
    print(f"\nTriggering {source} flow...")

    if source == 'oxford':
        from orchestration.oxford_flow import oxford_flow
        oxford_flow()
    elif source == 'wipo':
        from orchestration.wipo_flow import wipo_flow
        wipo_flow()
    elif source == 'pwt':
        from orchestration.pwt_flow import pwt_flow
        pwt_flow()
    else:
        raise ValueError(f"No flow defined for source: {source}")


# ═══════════════════════════════════════════════════════════════
# UPLOAD LOG
# ═══════════════════════════════════════════════════════════════

def log_upload(b2: B2Client, source: str, b2_key: str,
               file_path: str, year: str = None):
    """
    Write a JSON log of this upload to B2 at logs/uploads/.
    Provides a permanent audit trail of when files were uploaded.

    Log includes: source, file path, B2 key, year (if applicable),
    upload timestamp, hostname of the machine that ran the script.
    """
    log_entry = {
        'source':     source,
        'file_path':  str(file_path),
        'b2_key':     b2_key,
        'year':       year,
        'uploaded_at': datetime.utcnow().isoformat(),
        'hostname':   socket.gethostname(),
    }

    log_key = (
        f"logs/uploads/upload_{source}_"
        f"{date.today().isoformat()}.json"
    )

    try:
        b2.upload(
            log_key,
            json.dumps(log_entry, indent=2).encode('utf-8')
        )
        print(f"✓ Upload logged to B2: {log_key}")
    except Exception as e:
        # Log failure must never mask the upload success.
        print(f"⚠ Could not write upload log to B2: {e}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Upload a manual source file to B2 and trigger "
            "the corresponding Prefect ingestion flow."
        )
    )
    parser.add_argument(
        '--source',
        required = True,
        choices  = list(SUPPORTED_SOURCES.keys()),
        help     = "Source to upload: oxford, wipo, or pwt",
    )
    parser.add_argument(
        '--file',
        required = True,
        help     = "Full local path to the file to upload",
    )
    parser.add_argument(
        '--year',
        required = False,
        default  = None,
        help     = "4-digit year (required for Oxford only)",
    )

    args   = parser.parse_args()
    source = args.source
    config = SUPPORTED_SOURCES[source]

    # ── Validate arguments ─────────────────────────────────────
    if config['requires_year'] and not args.year:
        parser.error(
            f"--year is required for source '{source}'. "
            f"Example: --year 2025"
        )

    file_path = Path(args.file)
    if not file_path.exists():
        parser.error(f"File not found: {file_path}")

    # Validate file extension matches expected format.
    expected_ext = config['extension']
    if file_path.suffix.lower() != expected_ext:
        print(
            f"⚠ Warning: expected {expected_ext} file for "
            f"{source}, got {file_path.suffix}. Proceeding anyway."
        )

    # ── Read file ──────────────────────────────────────────────
    print(f"\nReading {file_path.name}...")
    file_bytes = file_path.read_bytes()
    file_size_mb = len(file_bytes) / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")

    # ── Construct B2 key ───────────────────────────────────────
    b2_key = get_b2_key(source, args.year)
    print(f"  B2 destination: {b2_key}")

    # ── Upload to B2 ───────────────────────────────────────────
    print(f"\nUploading to B2...")
    b2 = B2Client()

    try:
        b2.upload(b2_key, file_bytes)
        print(f"✓ Uploaded successfully: {b2_key}")
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        raise

    # ── Write upload log ───────────────────────────────────────
    log_upload(b2, source, b2_key, file_path, args.year)

    # ── Trigger Prefect flow ───────────────────────────────────
    print(f"\n{'─' * 50}")
    print(f"File uploaded. Triggering {source} pipeline...")
    print(f"{'─' * 50}\n")

    try:
        trigger_flow(source)
        print(f"\n{'─' * 50}")
        print(f"✓ {source} pipeline complete.")
        print(f"  Check Prefect UI and Supabase ops.pipeline_runs")
        print(f"  for detailed results.")
    except Exception as e:
        print(f"\n✗ {source} pipeline failed: {e}")
        # Send alert email so the team knows the upload
        # succeeded but the pipeline failed.
        send_email(
            subject = (
                f"[ACTION REQUIRED] {source} upload succeeded "
                f"but pipeline failed"
            ),
            body    = (
                f"File uploaded successfully to B2:\n"
                f"  {b2_key}\n\n"
                f"But the {source} pipeline failed with error:\n"
                f"  {e}\n\n"
                f"The file is safe on B2. Re-run the pipeline:\n"
                f"  python3 database/upload_to_b2.py "
                f"--source {source} --file {file_path}"
                + (f" --year {args.year}" if args.year else "")
                + f"\n\n"
                f"Note: re-running will skip the B2 upload "
                f"(file already there) and go straight to "
                f"triggering the flow."
            )
        )
        raise


if __name__ == "__main__":
    main()
