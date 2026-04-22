"""
database/add_api_source.py
==========================
Interactive CLI utility for adding a new API-based data source
to the pipeline end to end.

WHAT THIS FILE DOES:
    Guides the team through adding a new API source by:
    1. Prompting for source metadata (name, URL, format, etc.)
    2. Inserting into metadata.sources
    3. Fetching and inserting country codes into metadata.country_codes
    4. Fetching and inserting metrics into metadata.metrics and
       metadata.metric_codes
    5. Generating skeleton ingestion and transformation scripts
    6. Generating a skeleton Prefect flow file
    7. Printing instructions for adding the flow to api_flow()
    8. Logging everything to B2

WHY INTERACTIVE:
    Adding a source requires decisions that cannot be automated —
    what is the API URL, what country code format does it use,
    which metrics are relevant. Interactive prompts ensure the
    team member provides all required information while the script
    handles the database writes and file generation.

WHY THIS SCRIPT GENERATES SKELETON FILES:
    The skeleton ingestion and transformation scripts pre-fill
    all the boilerplate (imports, class structure, B2 paths,
    serialize/deserialize) that is identical for every source.
    The team member only writes fetch() and parse() — the
    source-specific logic. This prevents copy-paste errors and
    ensures every source follows the same structure.

WHAT THE TEAM MEMBER STILL DOES MANUALLY:
    - Implement fetch() in the ingestion script
    - Implement parse() in the transformation script
    - Add the new flow to api_flow() in orchestration/pipeline.py
    - Test the full pipeline end to end

LOGGING:
    Every add_api_source run is logged to B2 at:
        logs/add_source_{source_id}_{date}.json
    Includes all inputs and generated file paths.

USAGE:
    python3 database/add_api_source.py

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
# INTERACTIVE PROMPTS
# ═══════════════════════════════════════════════════════════════

def prompt(question: str, default: str = None) -> str:
    """Prompt the user for input with an optional default."""
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
    """Prompt the user to choose from a list of options."""
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
    """Prompt for a yes/no answer."""
    default_str = 'Y/n' if default else 'y/N'
    answer = input(f"{question} [{default_str}]: ").strip().lower()
    if not answer:
        return default
    return answer in ('y', 'yes')


# ═══════════════════════════════════════════════════════════════
# COUNTRY CODE SEEDING
# ═══════════════════════════════════════════════════════════════

def seed_country_codes(conn, source_id: str,
                       convention: str,
                       countries_df) -> int:
    """
    Seed country codes for a new source based on its convention.

    Args:
        conn:        Open SQLAlchemy connection.
        source_id:   New source identifier.
        convention:  'iso3', 'iso2', or 'name'
        countries_df: DataFrame of metadata.countries.

    Returns:
        Number of country codes inserted.
    """
    count     = 0
    unmatched = []

    for _, row in countries_df.iterrows():
        iso3 = row['iso3']

        if convention == 'iso3':
            code = iso3
        elif convention == 'iso2':
            try:
                country = pycountry.countries.get(alpha_3=iso3)
                code = country.alpha_2 if country and hasattr(country, 'alpha_2') else None
            except Exception:
                code = None
        else:
            # 'name' — use World Bank name as placeholder.
            # Team member should verify name variants manually.
            code = row['country_name']

        if code:
            conn.execute(text("""
                INSERT INTO metadata.country_codes (iso3, source_id, code)
                VALUES (:iso3, :source_id, :code)
                ON CONFLICT (iso3, source_id) DO NOTHING
            """), {'iso3': iso3, 'source_id': source_id, 'code': code})
            count += 1
        else:
            unmatched.append(iso3)

    if unmatched:
        print(
            f"  ⚠ {len(unmatched)} countries could not be mapped "
            f"to {convention} codes. Add manually to "
            f"metadata.country_codes if needed."
        )

    return count


# ═══════════════════════════════════════════════════════════════
# SKELETON FILE GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_ingest_skeleton(source_id: str, source_name: str) -> str:
    """Generate a skeleton ingestion script for a new API source."""
    class_name = ''.join(
        word.capitalize()
        for word in source_id.replace('_', ' ').split()
    ) + 'Ingestor'

    return f'''"""
ingestion/{source_id}_ingest.py
{'=' * (len(source_id) + 15)}
Ingestion script for {source_name}.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

TODO: Implement fetch() and fetch_metric_metadata().
      All other pipeline logic is inherited from BaseIngestor.
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
        TODO: Return the list of batch units to process.
        Example: list of years, indicator codes, or [\\'full_file\\'].
        """
        raise NotImplementedError("Implement get_batch_units()")

    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        """
        TODO: Fetch data from the {source_name} API.
        Must return (raw_row_count, df) where df has columns:
            country_iso3, year, period, metric_id,
            value, source_id, retrieved_at
        """
        raise NotImplementedError("Implement fetch()")

    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        TODO: Fetch metadata for an unknown metric from the API.
        Must return dict with keys:
            metric_id, metric_name, source_id, category,
            unit, description, frequency
        """
        raise NotImplementedError("Implement fetch_metric_metadata()")

    def get_b2_key(self, batch_unit: str) -> str:
        return f"bronze/{source_id}/{{batch_unit}}_{{self.run_date}}.json"

    def serialize(self, df: pd.DataFrame) -> bytes:
        return df.to_json(orient=\\'records\\', date_format=\\'iso\\').encode(\\'utf-8\\')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(BytesIO(data), orient=\\'records\\')


if __name__ == "__main__":
    {class_name}().run()
'''


def generate_transform_skeleton(source_id: str, source_name: str) -> str:
    """Generate a skeleton transformation script for a new API source."""
    class_name = ''.join(
        word.capitalize()
        for word in source_id.replace('_', ' ').split()
    ) + 'Transformer'

    return f'''"""
transformation/{source_id}_transform.py
{'=' * (len(source_id) + 20)}
Transformation script for {source_name}.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

TODO: Implement parse().
      All other pipeline logic is inherited from BaseTransformer.
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
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = \\'{source_id}\\'
                  AND stage     = \\'ingestion_batch\\'
                  AND status    = \\'complete\\'
                  AND checkpointed_at >= NOW() - INTERVAL \\'10 days\\'
                ORDER BY checkpointed_at DESC
            """)).fetchall()
        if not rows:
            raise ValueError("No completed ingestion checkpoints found.")
        self.run_date = rows[0][1].date().isoformat()
        return sorted({{row[0] for row in rows}})

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        TODO: Standardize df_raw into canonical silver layer shape.
        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. Country code mapping (if source does not use ISO3)
        4. Value casting
        5. Reset index
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
    """Generate a skeleton Prefect flow file for a new API source."""
    class_ingest   = ''.join(w.capitalize() for w in source_id.replace('_', ' ').split()) + 'Ingestor'
    class_transform = ''.join(w.capitalize() for w in source_id.replace('_', ' ').split()) + 'Transformer'

    return f'''"""
orchestration/{source_id}_flow.py
{'=' * (len(source_id) + 15)}
Prefect flow for {source_name}.
Add {source_id}_flow() to api_flow() in orchestration/pipeline.py.
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
    print("ADD NEW API SOURCE")
    print("=" * 60)
    print(
        "\nThis script adds a new API-based data source to the "
        "pipeline end to end.\nYou will be prompted for the "
        "source details.\n"
    )

    # ── Collect source metadata ────────────────────────────────
    source_id  = prompt("Source ID (lowercase_underscore, e.g. 'world_bank')")
    full_name  = prompt("Full source name")
    url        = prompt("Source URL or API documentation URL")
    license_   = prompt("Data license (e.g. 'CC-BY 4.0')")
    notes      = prompt("Notes (quirks, format info, etc.)", default="")

    update_frequency = prompt_choice(
        "Update frequency:",
        ['annual', 'biannual', 'quarterly', 'monthly', 'continuous']
    )

    country_convention = prompt_choice(
        "Country code convention used by this source:",
        ['iso3', 'iso2', 'name', 'custom']
    )

    # ── Confirm before proceeding ──────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  Source ID:   {source_id}")
    print(f"  Name:        {full_name}")
    print(f"  URL:         {url}")
    print(f"  License:     {license_}")
    print(f"  Frequency:   {update_frequency}")
    print(f"  Codes:       {country_convention}")
    print(f"{'─' * 60}")

    if not prompt_yes_no("\nProceed with these settings?"):
        print("Aborted.")
        return

    b2 = B2Client()

    with engine.connect() as conn:

        # ── Step 1: Insert into metadata.sources ──────────────
        existing = conn.execute(text("""
            SELECT source_id FROM metadata.sources
            WHERE source_id = :source_id
        """), {'source_id': source_id}).fetchone()

        if existing:
            print(f"\n⚠ Source '{source_id}' already exists in metadata.sources.")
            if not prompt_yes_no("Overwrite?", default=False):
                print("Aborted.")
                return

        conn.execute(text("""
            INSERT INTO metadata.sources (
                source_id, full_name, url, access_method,
                update_frequency, license, last_retrieved, notes
            ) VALUES (
                :source_id, :full_name, :url, 'REST API',
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
            'update_frequency': update_frequency,
            'license':          license_,
            'notes':            notes,
        })
        conn.commit()
        print(f"\n  ✓ Inserted into metadata.sources")

        # ── Step 2: Seed country codes ─────────────────────────
        if country_convention != 'custom':
            import pandas as pd
            countries_df = pd.read_sql(
                "SELECT iso3, country_name FROM metadata.countries",
                conn
            )
            count = seed_country_codes(
                conn, source_id, country_convention, countries_df
            )
            conn.commit()
            print(f"  ✓ Seeded {count} country codes ({country_convention})")
        else:
            print(
                f"  ⚠ Custom country codes selected. "
                f"Add mappings manually to metadata.country_codes "
                f"before running ingestion."
            )

        # ── Step 3: Prompt for metrics ─────────────────────────
        print(f"\n{'─' * 60}")
        print("METRIC REGISTRATION")
        print(
            "Enter each metric this source provides. "
            "Press Enter with empty metric_id when done."
        )
        print(f"{'─' * 60}")

        metrics_added = 0
        while True:
            print(f"\nMetric {metrics_added + 1}:")
            metric_id = input("  metric_id (e.g. 'new_source.indicator_name'): ").strip()
            if not metric_id:
                break

            metric_name = prompt("  metric_name")
            category    = prompt("  category", default="")
            unit        = prompt("  unit", default="")
            description = prompt("  description", default="")
            frequency   = prompt("  frequency", default="annual")
            source_code = prompt("  original source code (e.g. 'INDICATOR_CODE')")

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
                INSERT INTO metadata.metric_codes (metric_id, source_id, code)
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
    print(f"\n{'─' * 60}")
    print("GENERATING SKELETON FILES")
    print(f"{'─' * 60}")

    ingest_path    = Path(f"ingestion/{source_id}_ingest.py")
    transform_path = Path(f"transformation/{source_id}_transform.py")
    flow_path      = Path(f"orchestration/{source_id}_flow.py")

    for path, content in [
        (ingest_path,    generate_ingest_skeleton(source_id, full_name)),
        (transform_path, generate_transform_skeleton(source_id, full_name)),
        (flow_path,      generate_flow_skeleton(source_id, full_name)),
    ]:
        if path.exists():
            print(f"  ⚠ {path} already exists — skipping generation.")
        else:
            path.write_text(content)
            print(f"  ✓ Generated {path}")

    # ── Step 5: Log to B2 ─────────────────────────────────────
    log_entry = {
        'source_id':         source_id,
        'full_name':         full_name,
        'url':               url,
        'license':           license_,
        'update_frequency':  update_frequency,
        'country_convention': country_convention,
        'metrics_added':     metrics_added,
        'files_generated': [
            str(ingest_path),
            str(transform_path),
            str(flow_path),
        ],
        'added_at': datetime.utcnow().isoformat(),
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

3. Add {source_id}_flow() to api_flow() in:
   orchestration/pipeline.py

   Import at top:
   from orchestration.{source_id}_flow import {source_id}_ingest_task, {source_id}_transform_task

   Add to Phase 1 (concurrent ingestion):
   {source_id}_ingest_future = {source_id}_ingest_task.submit()

   Add to Phase 2 (sequential transformation):
   results['{source_id}_ingestion'] = _check_future({source_id}_ingest_future, '{source_id} ingestion')
   # Then add to transformation loop

4. Test independently:
   python3 ingestion/{source_id}_ingest.py
   python3 transformation/{source_id}_transform.py

5. After first successful ingestion, calculate coverage:
   python3 database/calculate_coverage.py --source {source_id}
""")


if __name__ == "__main__":
    main()
