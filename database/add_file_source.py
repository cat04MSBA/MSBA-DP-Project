"""
database/add_file_source.py
===========================
Interactive CLI utility for adding a new manual file-based
data source to the pipeline end to end.

WHAT THIS FILE DOES:
    Same as add_api_source.py but for manual file sources
    (like Oxford, WIPO, PWT). Key differences:

    - No automatic metric fetching — files have no API to query
      for metadata. Team member enters each metric manually.
    - Generates a different set of skeleton files — the ingestion
      script reads from B2 (uploaded via upload_to_b2.py) and
      the flow is triggered manually, not by a cron schedule.
    - Prints instructions for adding to upload_to_b2.py instead
      of adding to api_flow().

USAGE:
    python3 database/add_file_source.py

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION
"""

import json
import pycountry
from datetime import date, datetime
from pathlib import Path
from sqlalchemy import text

from database.connection import get_engine
from database.b2_upload import B2Client

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# PROMPTS (shared with add_api_source.py pattern)
# ═══════════════════════════════════════════════════════════════

def prompt(question: str, default: str = None) -> str:
    if default:
        answer = input(f"{question} [{default}]: ").strip()
        return answer if answer else default
    else:
        while True:
            answer = input(f"{question}: ").strip()
            if answer:
                return answer
            print("  This field is required.")


def prompt_choice(question: str, choices: list) -> str:
    print(f"\n{question}")
    for i, choice in enumerate(choices, 1):
        print(f"  {i}. {choice}")
    while True:
        answer = input("Enter number: ").strip()
        try:
            idx = int(answer) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(choices)}.")


def prompt_yes_no(question: str, default: bool = True) -> bool:
    default_str = 'Y/n' if default else 'y/N'
    answer = input(f"{question} [{default_str}]: ").strip().lower()
    if not answer:
        return default
    return answer in ('y', 'yes')


# ═══════════════════════════════════════════════════════════════
# SKELETON FILE GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_ingest_skeleton(source_id: str, source_name: str,
                              file_format: str) -> str:
    """Generate skeleton ingestion script for a manual file source."""
    class_name = ''.join(
        w.capitalize() for w in source_id.replace('_', ' ').split()
    ) + 'Ingestor'

    deserialize_hint = {
        'csv':   "pd.read_csv(BytesIO(data), dtype=str)",
        'xlsx':  "pd.read_excel(BytesIO(data), dtype=str)",
        'json':  "pd.read_json(BytesIO(data), orient='records')",
        'stata': "pd.read_stata(BytesIO(data), convert_categoricals=False)",
    }.get(file_format.lower(), "# TODO: implement deserialize")

    return f'''"""
ingestion/{source_id}_ingest.py
{'=' * (len(source_id) + 15)}
Ingestion script for {source_name}.

Manual source — triggered by upload_to_b2.py when a new file
is uploaded. Never called by api_flow().

TODO: Implement fetch() and fetch_metric_metadata().
"""

import pandas as pd
from datetime import date
from io import BytesIO

from database.base_ingestor import BaseIngestor

RETRY_DELAYS = [60, 300, 600]


class {class_name}(BaseIngestor):

    def __init__(self):
        super().__init__(source_id=\'{source_id}\')
        self.run_date = date.today().isoformat()

    def get_batch_units(self, since_date: date) -> list:
        """
        TODO: Return batch units. For single-file sources: [\'full_file\'].
        For year-based sources: scan B2 for uploaded files.
        """
        return [\'full_file\']

    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        """
        TODO: Read file from B2, parse to canonical DataFrame.
        Must return (raw_row_count, df).
        """
        raise NotImplementedError("Implement fetch()")

    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        TODO: Return metadata for unknown metric.
        For file sources, this is usually hardcoded since
        there is no API to query.
        """
        return {{
            \'metric_id\':   metric_id,
            \'metric_name\': metric_id,
            \'source_id\':   \'{source_id}\',
            \'category\':    None,
            \'unit\':        None,
            \'description\': \'Auto-detected metric\',
            \'frequency\':   \'annual\',
        }}

    def get_b2_key(self, batch_unit: str) -> str:
        return f"bronze/{source_id}/{{batch_unit}}_{{self.run_date}}.{file_format}"

    def serialize(self, df: pd.DataFrame) -> bytes:
        return df.to_json(orient=\\'records\\', date_format=\\'iso\\').encode(\\'utf-8\\')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return {deserialize_hint}


if __name__ == "__main__":
    {class_name}().run()
'''


def generate_transform_skeleton(source_id: str, source_name: str) -> str:
    """Generate skeleton transformation script for a manual file source."""
    class_name = ''.join(
        w.capitalize() for w in source_id.replace('_', ' ').split()
    ) + 'Transformer'

    return f'''"""
transformation/{source_id}_transform.py
{'=' * (len(source_id) + 20)}
Transformation script for {source_name}.

TODO: Implement parse().
"""

import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_transformer import BaseTransformer

EXPECTED_COLUMNS = {{
    \\'country_iso3\\', \\'year\\', \\'period\\', \\'metric_id\\',
    \\'value\\', \\'source_id\\', \\'retrieved_at\\'
}}


class {class_name}(BaseTransformer):

    def __init__(self):
        super().__init__(source_id=\'{source_id}\')
        self.run_date = None

    def get_batch_units(self) -> list:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = \\'{source_id}\\'
                  AND stage     = \\'ingestion_batch\\'
                  AND status    = \\'complete\\'
                  AND checkpointed_at >= NOW() - INTERVAL \\'10 days\\'
                ORDER BY checkpointed_at DESC
                LIMIT 1
            """)).fetchall()
        if not rows:
            raise ValueError("No completed ingestion checkpoint found.")
        self.run_date = rows[0][1].date().isoformat()
        return [rows[0][0]]

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        TODO: Standardize df_raw into canonical silver layer shape.
        """
        raise NotImplementedError("Implement parse()")

    def get_b2_key(self, batch_unit: str) -> str:
        if self.run_date is None:
            raise RuntimeError("Call get_batch_units() first.")
        return f"bronze/{source_id}/{{batch_unit}}_{{self.run_date}}.json"

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(BytesIO(data), orient=\\'records\\')


if __name__ == "__main__":
    {class_name}().run()
'''


def generate_flow_skeleton(source_id: str, source_name: str) -> str:
    """Generate skeleton Prefect flow for a manual file source."""
    class_ingest    = ''.join(w.capitalize() for w in source_id.replace('_', ' ').split()) + 'Ingestor'
    class_transform = ''.join(w.capitalize() for w in source_id.replace('_', ' ').split()) + 'Transformer'

    return f'''"""
orchestration/{source_id}_flow.py
{'=' * (len(source_id) + 15)}
Prefect flow for {source_name}.
Triggered by upload_to_b2.py — NOT part of api_flow().
"""

from prefect import flow, task, get_run_logger
from ingestion.{source_id}_ingest import {class_ingest}
from transformation.{source_id}_transform import {class_transform}


@task(name="{source_id}-ingestion", retries=3, retry_delay_seconds=[60, 300, 600])
def {source_id}_ingest_task():
    logger = get_run_logger()
    logger.info("Starting {source_name} ingestion")
    {class_ingest}().run()
    logger.info("{source_name} ingestion complete")


@task(name="{source_id}-transformation", retries=3, retry_delay_seconds=[60, 300, 600])
def {source_id}_transform_task():
    logger = get_run_logger()
    logger.info("Starting {source_name} transformation")
    {class_transform}().run()
    logger.info("{source_name} transformation complete")


@flow(name="{source_id}-flow", timeout_seconds=3600)
def {source_id}_flow():
    logger = get_run_logger()
    logger.info("{source_id}_flow started")
    {source_id}_ingest_task()
    {source_id}_transform_task()
    logger.info("{source_id}_flow complete")


if __name__ == "__main__":
    {source_id}_flow()
'''


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 60)
    print("ADD NEW FILE SOURCE")
    print("=" * 60)
    print(
        "\nThis script adds a new manual file-based data source.\n"
        "You will be prompted for the source details.\n"
    )

    # ── Collect source metadata ────────────────────────────────
    source_id  = prompt("Source ID (lowercase_underscore)")
    full_name  = prompt("Full source name")
    url        = prompt("Source download URL")
    license_   = prompt("Data license (e.g. 'CC-BY 4.0')")
    notes      = prompt("Notes", default="")

    update_frequency = prompt_choice(
        "Update frequency:",
        ['annual', 'biannual', 'every few years', 'irregular']
    )

    file_format = prompt_choice(
        "File format:",
        ['csv', 'xlsx', 'json', 'stata', 'other']
    )

    country_convention = prompt_choice(
        "Country code convention:",
        ['iso3', 'iso2', 'name', 'custom']
    )

    # ── Confirm ────────────────────────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  Source ID:   {source_id}")
    print(f"  Name:        {full_name}")
    print(f"  Format:      {file_format}")
    print(f"  Frequency:   {update_frequency}")
    print(f"  Codes:       {country_convention}")
    print(f"{'─' * 60}")

    if not prompt_yes_no("\nProceed?"):
        print("Aborted.")
        return

    b2 = B2Client()

    with engine.connect() as conn:
        import pandas as pd

        # ── Step 1: Insert into metadata.sources ──────────────
        conn.execute(text("""
            INSERT INTO metadata.sources (
                source_id, full_name, url, access_method,
                update_frequency, license, last_retrieved, notes
            ) VALUES (
                :source_id, :full_name, :url,
                :access_method,
                :update_frequency, :license, NULL, :notes
            )
            ON CONFLICT (source_id) DO UPDATE SET
                full_name        = EXCLUDED.full_name,
                url              = EXCLUDED.url,
                update_frequency = EXCLUDED.update_frequency,
                license          = EXCLUDED.license,
                notes            = EXCLUDED.notes
        """), {
            'source_id':        source_id,
            'full_name':        full_name,
            'url':              url,
            'access_method':    f'{file_format} download',
            'update_frequency': update_frequency,
            'license':          license_,
            'notes':            notes,
        })
        conn.commit()
        print(f"\n  ✓ Inserted into metadata.sources")

        # ── Step 2: Seed country codes ─────────────────────────
        if country_convention != 'custom':
            countries_df = pd.read_sql(
                "SELECT iso3, country_name FROM metadata.countries",
                conn
            )
            count = 0
            for _, row in countries_df.iterrows():
                iso3 = row['iso3']
                if country_convention == 'iso3':
                    code = iso3
                elif country_convention == 'iso2':
                    try:
                        c = pycountry.countries.get(alpha_3=iso3)
                        code = c.alpha_2 if c and hasattr(c, 'alpha_2') else None
                    except Exception:
                        code = None
                else:
                    code = row['country_name']

                if code:
                    conn.execute(text("""
                        INSERT INTO metadata.country_codes
                            (iso3, source_id, code)
                        VALUES (:iso3, :source_id, :code)
                        ON CONFLICT (iso3, source_id) DO NOTHING
                    """), {'iso3': iso3, 'source_id': source_id, 'code': code})
                    count += 1

            conn.commit()
            print(f"  ✓ Seeded {count} country codes ({country_convention})")
        else:
            print(
                f"  ⚠ Custom codes — add manually to "
                f"metadata.country_codes before ingestion."
            )

        # ── Step 3: Prompt for metrics ─────────────────────────
        print(f"\n{'─' * 60}")
        print("METRIC REGISTRATION")
        print(f"{'─' * 60}")

        metrics_added = 0
        while True:
            print(f"\nMetric {metrics_added + 1}:")
            metric_id = input("  metric_id (empty to finish): ").strip()
            if not metric_id:
                break

            metric_name = prompt("  metric_name")
            category    = prompt("  category", default="")
            unit        = prompt("  unit", default="")
            description = prompt("  description", default="")
            frequency   = prompt("  frequency", default="annual")
            source_code = prompt("  original code in the file")

            conn.execute(text("""
                INSERT INTO metadata.metrics (
                    metric_id, metric_name, source_id, category,
                    unit, description, frequency,
                    available_from, available_to
                ) VALUES (
                    :metric_id, :metric_name, :source_id, :category,
                    :unit, :description, :frequency, NULL, NULL
                )
                ON CONFLICT (metric_id) DO NOTHING
            """), {
                'metric_id':   metric_id,
                'metric_name': metric_name,
                'source_id':   source_id,
                'category':    category or None,
                'unit':        unit or None,
                'description': description or None,
                'frequency':   frequency,
            })

            conn.execute(text("""
                INSERT INTO metadata.metric_codes
                    (metric_id, source_id, code)
                VALUES (:metric_id, :source_id, :code)
                ON CONFLICT (metric_id, source_id) DO NOTHING
            """), {
                'metric_id': metric_id,
                'source_id': source_id,
                'code':      source_code,
            })

            conn.commit()
            metrics_added += 1
            print(f"  ✓ Registered {metric_id}")

        print(f"\n  ✓ {metrics_added} metrics registered")

    # ── Step 4: Generate skeleton files ───────────────────────
    ingest_path    = Path(f"ingestion/{source_id}_ingest.py")
    transform_path = Path(f"transformation/{source_id}_transform.py")
    flow_path      = Path(f"orchestration/{source_id}_flow.py")

    for path, content in [
        (ingest_path,    generate_ingest_skeleton(source_id, full_name, file_format)),
        (transform_path, generate_transform_skeleton(source_id, full_name)),
        (flow_path,      generate_flow_skeleton(source_id, full_name)),
    ]:
        if path.exists():
            print(f"  ⚠ {path} exists — skipping.")
        else:
            path.write_text(content)
            print(f"  ✓ Generated {path}")

    # ── Step 5: Log to B2 ─────────────────────────────────────
    log_entry = {
        'source_id':         source_id,
        'full_name':         full_name,
        'url':               url,
        'file_format':       file_format,
        'update_frequency':  update_frequency,
        'country_convention': country_convention,
        'metrics_added':     metrics_added,
        'added_at':          datetime.utcnow().isoformat(),
    }

    b2_log_key = f"logs/add_source_{source_id}_{date.today().isoformat()}.json"
    try:
        b2.upload(
            b2_log_key,
            json.dumps(log_entry, indent=2).encode('utf-8')
        )
        print(f"\n  ✓ Logged to B2: {b2_log_key}")
    except Exception as e:
        print(f"\n  ⚠ Could not write log to B2: {e}")

    # ── Step 6: Print next steps ───────────────────────────────
    print(f"\n{'=' * 60}")
    print("NEXT STEPS")
    print(f"{'=' * 60}")
    print(f"""
1. Implement fetch() in:
   {ingest_path}

2. Implement parse() in:
   {transform_path}

3. Add {source_id} to upload_to_b2.py:
   In SUPPORTED_SOURCES dict, add:
   '{source_id}': {{
       'requires_year': False,  # or True if year-based
       'description':   '{full_name}',
       'extension':     '.{file_format}',
   }}

   In trigger_flow(), add:
   elif source == '{source_id}':
       from orchestration.{source_id}_flow import {source_id}_flow
       {source_id}_flow()

4. Test by uploading a file:
   python3 database/upload_to_b2.py \\
       --source {source_id} \\
       --file /path/to/file.{file_format}

5. After first successful ingestion:
   python3 database/calculate_coverage.py --source {source_id}
""")


if __name__ == "__main__":
    main()
