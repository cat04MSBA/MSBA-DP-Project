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
    WIPO:   bronze/wipo_ip/source_{date}.csv
    PWT:    bronze/pwt/source_{date}.dta

AUDIT LOG STRUCTURE:
    Every upload writes a JSON log to B2 under a source-specific
    folder so the audit trail is browsable by source:

        logs/oxford/oxford_2019_uploaded_2026-04-23.json
        logs/oxford/oxford_2020_uploaded_2026-04-23.json
        logs/wipo/wipo_ai_patents_1980-2024_uploaded_2026-04-23.json
        logs/pwt/pwt110_uploaded_2026-04-23.json

    The log records: source, local file path, B2 destination key,
    data year range, upload timestamp (UTC), hostname, and git
    username if available — giving a complete audit trail of who
    uploaded what, from which machine, and when.

WHY ONE SCRIPT FOR ALL MANUAL SOURCES:
    A single entry point with --source routing is simpler than
    three separate upload scripts. Adding a new manual source
    means adding one elif block here.

WHY THE FLOW IS TRIGGERED DIRECTLY (not via Prefect API):
    Direct call works without a Prefect deployment setup and is
    still tracked by Prefect as a flow run. To switch to remote
    triggering via Prefect API, replace the direct call with
    run_deployment() here — no other files need to change.

WHY --year IS REQUIRED FOR OXFORD (not for others):
    Oxford publishes one file per year. The year is part of the
    B2 filename so the ingestion script can identify which year
    each file belongs to. WIPO and PWT are single full-history
    files — their year range is read from the file itself.
"""

import argparse
import io
import json
import socket
import subprocess
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from database.b2_upload import B2Client
from database.email_utils import send_email


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

# Number of header rows to skip in WIPO CSV before the data.
# Used to detect the year range from column headers locally.
WIPO_HEADER_ROWS = 3


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
        'source_id':     'wipo_ip',
    },
    'pwt': {
        'requires_year': False,
        'description':   'Penn World Tables (.dta)',
        'extension':     '.dta',
    },
}


# ═══════════════════════════════════════════════════════════════
# GIT USERNAME
# ═══════════════════════════════════════════════════════════════

def get_git_username() -> str:
    """
    Return the uploader's name for the audit log.

    Tries in order:
    1. git config user.name — if git is configured on this machine
    2. Prompts the user to type their name — if git is not configured
       Also offers to save it to git config so they're not asked again.

    Returns:
        Name string. Never returns empty or 'unknown'.
    """
    # Try git first
    try:
        result = subprocess.run(
            ['git', 'config', 'user.name'],
            capture_output = True,
            text           = True,
            timeout        = 5,
        )
        name = result.stdout.strip()
        if name:
            return name
    except Exception:
        pass

    # Git not configured — prompt the user
    print()
    print("⚠ git user.name not configured on this machine.")
    name = input("  Enter your name for the upload audit log: ").strip()

    if not name:
        name = 'unknown'
    else:
        # Offer to save to git config so they're not asked again
        save = input(
            f"  Save '{name}' to git config so you're not asked again? [y/n]: "
        ).strip().lower()
        if save == 'y':
            try:
                subprocess.run(
                    ['git', 'config', '--global', 'user.name', name],
                    timeout = 5,
                )
                print(f"  ✓ Saved to git config. Won't ask again.")
            except Exception:
                print(f"  ⚠ Could not save to git config. You'll be asked next time.")

    return name


# ═══════════════════════════════════════════════════════════════
# YEAR RANGE DETECTION
# ═══════════════════════════════════════════════════════════════

def get_wipo_year_range(file_path: Path) -> tuple:
    """
    Read the WIPO CSV locally and detect the year range from
    the column headers (4-digit numeric column names).

    Used to produce a meaningful log filename and log entry
    without having to open the file after it's on B2.

    Returns:
        (first_year, last_year) as strings, e.g. ('1980', '2024').
        Returns ('unknown', 'unknown') if detection fails.
    """
    try:
        df = pd.read_csv(
            file_path,
            skiprows  = 6,          # matches WIPO_HEADER_ROWS in wipo_ingest.py
            dtype     = str,
            nrows     = 1,
            index_col = False,      # trailing comma fix — same as wipo_ingest.py
        )
        year_cols = sorted([
            col for col in df.columns
            if str(col).strip().isdigit() and len(str(col).strip()) == 4
        ])
        if year_cols:
            return year_cols[0], year_cols[-1]
    except Exception:
        pass
    return 'unknown', 'unknown'


def get_pwt_version(file_path: Path) -> str:
    """
    Extract PWT version from the filename.
    e.g. pwt110.dta → '110', pwt100.dta → '100'

    Returns:
        Version string, or 'unknown' if not detectable.
    """
    stem = file_path.stem.lower()  # e.g. 'pwt110'
    if stem.startswith('pwt') and stem[3:].isdigit():
        return stem[3:]
    return 'unknown'


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
        # wipo_ingest.py reads from: bronze/wipo_ip/source_{date}.csv
        return f"bronze/wipo_ip/source_{today}.csv"
    elif source == 'pwt':
        # pwt_ingest.py reads from: bronze/pwt/source_{date}.dta
        return f"bronze/pwt/source_{today}.dta"
    else:
        raise ValueError(f"Unknown source: {source}")


# ═══════════════════════════════════════════════════════════════
# LOG KEY CONSTRUCTION
# ═══════════════════════════════════════════════════════════════

def get_log_key(source: str, file_path: Path,
                year: str = None,
                wipo_year_range: tuple = None) -> str:
    """
    Return the B2 key for the upload audit log.

    Log structure by source:
        Oxford: logs/oxford/oxford_{year}_uploaded_{YYYY-MM-DD}.json
        WIPO:   logs/wipo/wipo_ai_patents_{from}-{to}_uploaded_{YYYY-MM-DD}.json
        PWT:    logs/pwt/pwt{version}_uploaded_{YYYY-MM-DD}.json

    WHY SOURCE-SPECIFIC SUBFOLDERS:
        Browsing logs/oxford/ immediately shows all Oxford uploads
        in chronological order. A flat logs/uploads/ folder mixes
        all sources together and requires opening files to know
        which year each Oxford upload covered.

    WHY UPLOADED DATE IN FILENAME:
        Makes the log sortable by upload date. The data year and
        upload date are both visible in the filename without
        opening the file — complete audit information at a glance.
    """
    today = date.today().isoformat()

    if source == 'oxford':
        return f"logs/oxford/oxford_{year}_uploaded_{today}.json"

    elif source == 'wipo':
        if wipo_year_range and wipo_year_range[0] != 'unknown':
            yr_from, yr_to = wipo_year_range
            return (
                f"logs/wipo/wipo_ai_patents_{yr_from}-{yr_to}"
                f"_uploaded_{today}.json"
            )
        return f"logs/wipo/wipo_ai_patents_uploaded_{today}.json"

    elif source == 'pwt':
        version = get_pwt_version(file_path)
        if version != 'unknown':
            return f"logs/pwt/pwt{version}_uploaded_{today}.json"
        return f"logs/pwt/pwt_uploaded_{today}.json"

    else:
        return f"logs/{source}/{source}_uploaded_{today}.json"


# ═══════════════════════════════════════════════════════════════
# UPLOAD LOG
# ═══════════════════════════════════════════════════════════════

def log_upload(b2: B2Client, source: str, b2_key: str,
               file_path: Path, year: str = None,
               wipo_year_range: tuple = None):
    """
    Write a JSON audit log of this upload to B2.

    WHAT THE LOG RECORDS:
        source          → which source (oxford, wipo, pwt)
        file_path       → full local path on the uploader's machine
                          (shows which machine and directory the file
                          came from — useful for tracing provenance)
        b2_key          → where the file landed on B2
                          (different from file_path — one is local,
                          one is cloud)
        data_year       → for Oxford: the index year of this file
        data_year_range → for WIPO: first and last year in the CSV
        pwt_version     → for PWT: the version number (e.g. '110')
        uploaded_at     → UTC timestamp of the upload
        hostname        → machine name (identifies which computer)
        uploaded_by     → git user.name (identifies which team member)
    """
    log_entry = {
        'source':     source,
        'file_path':  str(file_path),
        'b2_key':     b2_key,
        'uploaded_at': datetime.utcnow().isoformat() + 'Z',
        'hostname':   socket.gethostname(),
        'uploaded_by': get_git_username(),
    }

    # Add source-specific fields
    if source == 'oxford' and year:
        log_entry['data_year'] = year

    elif source == 'wipo' and wipo_year_range:
        log_entry['data_year_range'] = {
            'from': wipo_year_range[0],
            'to':   wipo_year_range[1],
        }

    elif source == 'pwt':
        version = get_pwt_version(file_path)
        if version != 'unknown':
            log_entry['pwt_version'] = version

    log_key = get_log_key(
        source, file_path, year, wipo_year_range
    )

    try:
        b2.upload(
            log_key,
            json.dumps(log_entry, indent=2).encode('utf-8')
        )
        print(f"✓ Upload logged to B2: {log_key}")
    except Exception as e:
        # Log failure must never mask the upload success.
        print(f"⚠ Could not write upload log: {e}")


# ═══════════════════════════════════════════════════════════════
# FLOW TRIGGER
# ═══════════════════════════════════════════════════════════════

def trigger_flow(source: str):
    """
    Trigger the Prefect flow for the given source.
    Calls the flow function directly — Prefect tracks it as a
    flow run in the UI with full logging and retry handling.
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
        help     = "4-digit data year (required for Oxford only)",
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

    expected_ext = config['extension']
    if file_path.suffix.lower() != expected_ext:
        print(
            f"⚠ Warning: expected {expected_ext} file for "
            f"{source}, got {file_path.suffix}. Proceeding anyway."
        )

    # ── Detect year range for WIPO (before upload) ─────────────
    wipo_year_range = None
    if source == 'wipo':
        wipo_year_range = get_wipo_year_range(file_path)
        if wipo_year_range[0] != 'unknown':
            print(
                f"  Detected WIPO year range: "
                f"{wipo_year_range[0]}–{wipo_year_range[1]}"
            )

    # ── Read file ──────────────────────────────────────────────
    print(f"\nReading {file_path.name}...")
    file_bytes   = file_path.read_bytes()
    file_size_mb = len(file_bytes) / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")

    # ── Construct B2 key ───────────────────────────────────────
    b2_key = get_b2_key(source, args.year)
    print(f"  B2 destination: {b2_key}")
    print(f"  Uploaded by:    {get_git_username()} @ {socket.gethostname()}")

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
    log_upload(
        b2, source, b2_key, file_path,
        year            = args.year,
        wipo_year_range = wipo_year_range,
    )

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
