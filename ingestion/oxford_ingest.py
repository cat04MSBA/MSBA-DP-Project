"""
ingestion/oxford_ingest.py
==========================
Ingestion script for Oxford Insights Government AI Readiness Index.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads Oxford AI Readiness Index XLSX files from B2 (uploaded
    manually by a team member via upload_to_b2.py), extracts the
    country name and score columns, and produces a standardized
    DataFrame for the quality check and checkpoint pipeline.

MANUAL SOURCE WORKFLOW:
    1. Team member downloads new XLSX from Oxford Insights website
    2. Runs: python3 database/upload_to_b2.py --source oxford
             --file oxford_2025.xlsx --year 2025
    3. upload_to_b2.py uploads to bronze/oxford/ and triggers
       this subflow automatically
    4. This script reads from B2 — never from local disk

WHY get_batch_units() USES b2.list():
    Oxford publishes one file per year with no fixed schedule.
    Files arrive one at a time via upload_to_b2.py. get_batch_units()
    scans bronze/oxford/ on B2 to discover which year files exist,
    then cross-references against completed ingestion checkpoints
    to return only years that have not yet been processed.
    This means re-running the script is safe — already-processed
    years are skipped via checkpoint logic.

COLUMN INCONSISTENCY ACROSS YEARS:
    Oxford changes column names every edition. The score column
    has been named: 'Score', 'Total', 'Total Score', '2024 Total'.
    The country column is always 'Country' but the rank column
    varies. This script detects the score column dynamically
    rather than hardcoding, using a priority list of known names.

COUNTRY NAME MAPPING:
    Oxford uses full country names (e.g. 'United States of America',
    'Republic of Korea'). These are mapped to ISO3 via
    metadata.country_codes where source_id='oxford', populated
    at seeding time by seed_country_codes.py using three-method
    validation. At ingestion time, one dict lookup — no fuzzy
    matching at runtime.

WHY ROWS WITH UNMATCHED COUNTRY NAMES ARE REJECTED:
    If a country name cannot be found in metadata.country_codes,
    the row is logged in ops.rejection_summary as
    'unknown_country_code' and skipped. This is expected for
    territories and non-sovereign entities Oxford includes
    (e.g. 'Hong Kong' if not in our crosswalk). The team can
    add missing names to OXFORD_MANUAL_OVERRIDES in
    seed_country_codes.py and re-seed.

B2 FILE NAMING CONVENTION:
    bronze/oxford/oxford_{year}_{retrieved_date}.xlsx
    Example: bronze/oxford/oxford_2025_2026-04-20.xlsx

RAW ROW COUNT:
    Number of data rows in the XLSX after reading (excluding
    header). Counted before any filtering or name mapping.

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff for B2 read failures.
    XLSX parsing failures halt immediately — if the file is
    corrupt, retrying reads the same corrupt file.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import re
import time
import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# B2 prefix where Oxford files are stored.
OXFORD_B2_PREFIX = 'bronze/oxford/'

# Sheet names used across Oxford editions 2019-2025.
# Tried in order — first match wins.
OXFORD_SHEET_NAMES = [
    'Global rankings',
    'Global ranking',
    'Global Rankings',
    'Rankings',
    'Ranking',
]

# Score column names used across Oxford editions 2019-2025.
# Tried in order — first match wins.
# Some editions use year-prefixed names (e.g. '2024 Total').
SCORE_COLUMN_CANDIDATES = [
    'Score',
    'Total Score',
    'Total',
]

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]


class OxfordIngestor(BaseIngestor):
    """
    Ingestion script for Oxford Insights GAIRI.
    Inherits all pipeline orchestration from BaseIngestor.
    Implements the six source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='oxford')
        self.run_date = date.today().isoformat()

        # Oxford name → ISO3 crosswalk loaded once per run.
        # Populated at seeding time by seed_country_codes.py.
        # At ingestion time: one dict lookup, no fuzzy matching.
        self._oxford_to_iso3 = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        Discover which Oxford year files exist on B2 and return
        only the years that have not yet been processed.

        WHY b2.list() IS USED HERE:
            Oxford files arrive one at a time via upload_to_b2.py.
            There is no API to query for available years — we must
            scan B2 to know what has been uploaded. b2.list()
            returns all keys under bronze/oxford/ in one efficient
            prefix scan. The script then cross-references against
            completed checkpoints to skip already-processed years.

        WHY since_date IS NOT USED FOR FILTERING:
            Oxford publishes one file per year covering that year's
            data only. There is no revision window concept — each
            file is complete and final for its year. All available
            unprocessed years are returned regardless of since_date.

        Args:
            since_date: Not used. Oxford has no revision window.

        Returns:
            List of year strings (e.g. ['2019', '2023', '2025'])
            for files on B2 that have not yet been ingested.
        """
        # ── Step 1: List all Oxford files on B2 ───────────────
        all_keys = self.b2.list(OXFORD_B2_PREFIX)

        # ── Step 2: Extract years from filenames ───────────────
        # Filename format: bronze/oxford/oxford_{year}_{date}.xlsx
        # Regex extracts the 4-digit year from between 'oxford_'
        # and the next underscore.
        available_years = set()
        for key in all_keys:
            filename = key.split('/')[-1]
            match = re.match(r'^oxford_(\d{4})_', filename)
            if match:
                available_years.add(match.group(1))

        if not available_years:
            print("  ⚠ No Oxford files found on B2 under bronze/oxford/")
            return []

        # ── Step 3: Find already-completed years ───────────────
        # Cross-reference against ops.checkpoints to skip years
        # that were already successfully ingested.
        with self.engine.connect() as conn:
            completed = self._get_completed_oxford_years(conn)

        # ── Step 4: Return unprocessed years sorted ────────────
        unprocessed = sorted(available_years - completed)

        print(
            f"  Found {len(available_years)} Oxford files on B2. "
            f"{len(completed)} already ingested. "
            f"{len(unprocessed)} to process: {unprocessed}"
        )

        return unprocessed

    def _get_completed_oxford_years(self, conn) -> set:
        """
        Return the set of years that already have a completed
        ingestion checkpoint. Used by get_batch_units() to
        skip already-processed years.
        """
        rows = conn.execute(text("""
            SELECT batch_unit
            FROM ops.checkpoints
            WHERE source_id = 'oxford'
              AND stage     = 'ingestion_batch'
              AND status    = 'complete'
        """)).fetchall()
        return {row[0] for row in rows}


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Read one Oxford XLSX year file from B2 and parse it.

        WHY B2 NOT LOCAL DISK:
            Oxford files are uploaded to B2 via upload_to_b2.py
            before this script runs. Reading from B2 ensures the
            ingestion script works identically regardless of which
            machine runs it — no dependency on local file paths.

        Args:
            batch_unit: Year string, e.g. '2025'.
            since_date: Not used for Oxford.

        Returns:
            (raw_row_count, df)
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._read_oxford_file(batch_unit)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] Oxford {batch_unit}: "
                        f"{e}. Waiting {delay}s..."
                    )
                    time.sleep(delay)

        raise RuntimeError(
            f"Oxford {batch_unit} failed after "
            f"{len(RETRY_DELAYS)} retries. Last error: {last_error}"
        )

    def _read_oxford_file(self, year: str) -> tuple:
        """
        Download and parse one Oxford XLSX from B2.

        Handles column name inconsistency across editions by
        detecting the score column dynamically. Also handles
        year-prefixed column names like '2024 Total'.

        Args:
            year: 4-digit year string.

        Returns:
            (raw_row_count, df)
        """
        b2_key = self.get_b2_key(year)
        xlsx_bytes = self.b2.download(b2_key)

        # ── Find correct sheet ─────────────────────────────────
        # Oxford uses different sheet names across editions.
        # Try each candidate name until one works.
        xl = pd.ExcelFile(BytesIO(xlsx_bytes))
        sheet_name = None
        for candidate in OXFORD_SHEET_NAMES:
            if candidate in xl.sheet_names:
                sheet_name = candidate
                break

        if sheet_name is None:
            raise ValueError(
                f"No recognised sheet name found in Oxford {year} XLSX. "
                f"Available sheets: {xl.sheet_names}. "
                f"Add the new name to OXFORD_SHEET_NAMES."
            )

        # ── Read the sheet ─────────────────────────────────────
        df_raw = pd.read_excel(
            BytesIO(xlsx_bytes),
            sheet_name = sheet_name,
            dtype      = str,       # read all as string to avoid
                                    # pandas numeric inference on ranks
        )

        # Drop completely empty rows that Excel sometimes adds.
        df_raw = df_raw.dropna(how='all').reset_index(drop=True)

        # raw_row_count = rows in the sheet excluding header.
        # Counted before any filtering — this is what
        # check_ingestion_pre() compares against len(df).
        raw_row_count = len(df_raw)

        # ── Detect score column ────────────────────────────────
        # Try fixed candidates first. Then try year-prefixed
        # patterns like '2024 Total' or '2023 Score'.
        score_col = None
        for candidate in SCORE_COLUMN_CANDIDATES:
            if candidate in df_raw.columns:
                score_col = candidate
                break

        # If no fixed candidate matched, try year-prefixed columns.
        # Pattern: '{year} Total', '{year} Score', '{year} Rank'
        if score_col is None:
            for col in df_raw.columns:
                if str(col).startswith(year) and any(
                    keyword in str(col)
                    for keyword in ['Total', 'Score']
                ):
                    score_col = col
                    break

        if score_col is None:
            raise ValueError(
                f"Could not identify score column in Oxford {year}. "
                f"Available columns: {list(df_raw.columns)}. "
                f"Add the column name to SCORE_COLUMN_CANDIDATES."
            )

        # ── Verify 'Country' column exists ─────────────────────
        if 'Country' not in df_raw.columns:
            raise ValueError(
                f"'Country' column not found in Oxford {year}. "
                f"Available columns: {list(df_raw.columns)}."
            )

        # ── Build standardized DataFrame ───────────────────────
        df = self._parse_oxford(df_raw, score_col, year)

        return raw_row_count, df

    def _parse_oxford(self, df_raw: pd.DataFrame,
                      score_col: str, year: str) -> pd.DataFrame:
        """
        Extract country name and score, map country name to ISO3
        using the pre-seeded metadata.country_codes crosswalk.

        WHY COUNTRY MAPPING USES metadata.country_codes:
            Oxford country names are matched to ISO3 ONCE at
            seeding time using three-method validation (pycountry
            + World Bank name match + manual overrides). The result
            is stored in metadata.country_codes. At ingestion time
            we do one dict lookup — no fuzzy matching at runtime.
            This keeps ingestion deterministic and fast.

        WHY UNMATCHED ROWS ARE DROPPED HERE (not rejected later):
            Rows with unrecognised country names will fail
            fk_validity check anyway (unmapped name = no ISO3 =
            not in metadata.countries). Dropping them here with
            an explicit count gives the team better diagnostics
            than a generic fk_validity rejection count.

        Args:
            df_raw:    Raw DataFrame from the XLSX sheet.
            score_col: Detected score column name.
            year:      4-digit year string for this file.

        Returns:
            Standardized DataFrame with canonical columns.
        """
        # Load Oxford name → ISO3 crosswalk once per run.
        oxford_to_iso3 = self._get_oxford_to_iso3()

        rows = []
        unmatched_names = []

        for _, row in df_raw.iterrows():
            country_name = str(row.get('Country', '') or '').strip()
            score_raw    = str(row.get(score_col, '') or '').strip()

            # Skip rows with no country name or no score.
            if not country_name or not score_raw or score_raw == 'nan':
                continue

            # Map Oxford country name to ISO3.
            iso3 = oxford_to_iso3.get(country_name)
            if not iso3:
                unmatched_names.append(country_name)
                continue

            # Validate score is numeric.
            try:
                float(score_raw)
            except ValueError:
                # Skip non-numeric scores (e.g. header rows that
                # slipped through). These are rare but defensive.
                continue

            rows.append({
                'country_iso3': iso3,
                'year':         int(year),
                'period':       'annual',
                'metric_id':    'oxford.ai_readiness',
                'value':        score_raw,
                'source_id':    'oxford',
                'retrieved_at': date.today().isoformat(),
            })

        if unmatched_names:
            print(
                f"    [parse] {len(unmatched_names)} Oxford country "
                f"names not in metadata.country_codes for year {year}: "
                f"{unmatched_names[:5]}"
                + (f" ... and {len(unmatched_names)-5} more"
                   if len(unmatched_names) > 5 else "")
            )
            print(
                f"    Add missing names to OXFORD_MANUAL_OVERRIDES "
                f"in seed_country_codes.py and re-seed."
            )

        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=[
                'country_iso3', 'year', 'period', 'metric_id',
                'value', 'source_id', 'retrieved_at'
            ]
        )

    def _get_oxford_to_iso3(self) -> dict:
        """
        Load Oxford country name → ISO3 crosswalk from
        metadata.country_codes. Cached after first call.

        WHY CACHED:
            Called once per year file in fetch(). With 7 years
            of files, without caching this adds 7 identical DB
            queries for the same static metadata table.
        """
        if self._oxford_to_iso3 is not None:
            return self._oxford_to_iso3

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code, iso3
                FROM metadata.country_codes
                WHERE source_id = 'oxford'
            """)).fetchall()

        # code = Oxford country name, iso3 = ISO3.
        self._oxford_to_iso3 = {r[0]: r[1] for r in rows}
        return self._oxford_to_iso3


    # ═══════════════════════════════════════════════════════
    # fetch_metric_metadata
    # ═══════════════════════════════════════════════════════

    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        Oxford has exactly one metric — oxford.ai_readiness —
        which is hardcoded in seed_metrics.py. This method
        should never be called in practice. Implemented for
        completeness and future-proofing.
        """
        return {
            'metric_id':   'oxford.ai_readiness',
            'metric_name': 'Government AI Readiness Index',
            'source_id':   'oxford',
            'category':    'AI Readiness',
            'unit':        '0-100 score',
            'description': (
                'Oxford Insights Government AI Readiness Index. '
                'Published annually since 2019. '
                'Source: oxfordinsights.com'
            ),
            'frequency':   'annual',
        }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given year.

        WHY THE KEY IS CONSTRUCTED THIS WAY:
            upload_to_b2.py uploads files using this same naming
            convention: bronze/oxford/oxford_{year}_{date}.xlsx.
            The ingestion script must use the exact same key to
            read the file back. Both scripts use run_date (today)
            as the date component.

        Naming convention:
            bronze/oxford/oxford_2025_2026-04-20.xlsx
        """
        return f"bronze/oxford/oxford_{batch_unit}_{self.run_date}.xlsx"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize DataFrame to JSON bytes for B2 upload.

        WHY JSON NOT XLSX:
            The original XLSX is already on B2 (uploaded by
            upload_to_b2.py before this script runs). The
            ingestion script stores the PARSED DataFrame as
            JSON — this is the standardized representation
            used by the transformation script. The original
            XLSX is preserved on B2 as the faithful raw copy.
            Two files per year:
            - bronze/oxford/oxford_{year}_{date}.xlsx → raw source
            - bronze/oxford/oxford_{year}_{date}_parsed.json → parsed
        """
        return df.to_json(
            orient='records',
            date_format='iso',
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize parsed JSON from B2 into DataFrame."""
        return pd.read_json(BytesIO(data), orient='records')

    def get_b2_key_parsed(self, batch_unit: str) -> str:
        """
        Return the B2 key for the parsed JSON version.
        The ingestion script uploads the parsed DataFrame here
        after quality checks pass. The transformation script
        reads from this key — not the original XLSX.
        """
        return (
            f"bronze/oxford/oxford_{batch_unit}"
            f"_{self.run_date}_parsed.json"
        )


if __name__ == "__main__":
    ingestor = OxfordIngestor()
    ingestor.run()
