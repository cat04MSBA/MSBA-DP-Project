"""
run_seeds.py
============
Runs all seed scripts in the correct order.

Usage:
    python3 database/run_seeds.py

Order is mandatory due to foreign key dependencies:
    1. seed_sources.py       → no dependencies
    2. seed_countries.py     → depends on: nothing
    3. seed_metrics.py       → depends on: sources
    4. seed_country_codes.py → depends on: sources + countries
    5. seed_metric_codes.py  → depends on: sources + metrics

Each script is safe to re-run independently.
This runner is also safe to re-run.

Estimated runtime: 10-20 minutes
Most time is spent on World Bank API (7 topics × ~1000 indicators).
PWT Stata file download takes ~1-2 minutes.
"""

import subprocess
import sys
import os
import time

scripts = [
    ('seed_sources.py',       'Populating metadata.sources (7 sources)'),
    ('seed_countries.py',     'Populating metadata.countries (~266 countries)'),
    ('seed_metrics.py',       'Populating metadata.metrics (all sources)'),
    ('seed_country_codes.py', 'Populating metadata.country_codes (crosswalk)'),
    ('seed_metric_codes.py',  'Populating metadata.metric_codes (crosswalk)'),
]
print("=" * 60)
print("RUNNING ALL SEED SCRIPTS")
print("=" * 60)

total_start = time.time()

for i, (script, description) in enumerate(scripts, 1):
    print(f"\n[{i}/{len(scripts)}] {description}")
    print("-" * 60)

    start = time.time()

    result = subprocess.run(
        [sys.executable, f"database/{script}"],
        capture_output=False,
        env={**os.environ, 'PYTHONPATH': '.'}
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n✗ {script} FAILED after {elapsed:.1f}s")
        print("Stopping — fix the error above before continuing.")
        sys.exit(1)
    else:
        print(f"\n✓ {script} completed in {elapsed:.1f}s")

total_elapsed = time.time() - total_start
print("\n" + "=" * 60)
print(f"ALL SEED SCRIPTS COMPLETED in {total_elapsed:.1f}s")
print("=" * 60)

# ── Final step: validate seed integrity ───────────────────────
# Verifies that all metadata tables are consistent and complete
# before any pipeline run is attempted. Exits with code 1 if
# any critical check fails so CI/CD or manual runs can catch it.
print("\n[6/6] Validating seed integrity...")
print("-" * 60)
import subprocess
val_result = subprocess.run(
    [sys.executable, "database/validate_seeds.py"],
    capture_output=False,
    env={**os.environ, 'PYTHONPATH': '.'}
)
if val_result.returncode != 0:
    print(
        "\n✗ SEED VALIDATION FAILED — do not run the pipeline.\n"
        "  Fix the issues reported above, then re-run: "
        "python3 -m database.run_seeds"
    )
    sys.exit(1)

print("\nNext steps:")
print("  1. Run ingestion scripts to pull actual data")
print("  2. After ingestion, run calculate_coverage.py to")
print("     populate available_from and available_to in metadata.metrics")
