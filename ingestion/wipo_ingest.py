"""
ingestion/wipo_ingest.py
========================
Ingestion script for WIPO IP Statistics AI Patent Applications.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the WIPO AI patents CSV from B2 (uploaded manually via
    upload_to_b2.py), melts it from wide format (years as columns)
    to tall format (one row per country per year), and produces a
    standardized DataFrame for the quality check pipeline.

MANUAL SOURCE WORKFLOW:
    1. Team member downloads ZIP from wipo.int/en/web/ip-statistics
    2. Extracts the AI patents CSV (indicator 4c)
    3. Runs: python3 database/upload_to_b2.py --source wipo
             --file wipo_ai_patents.csv
    4. upload_to_b2.py uploads to bronze/wipo_ip/ and triggers
       this subflow

CSV STRUCTURE (from actual file inspection):
    - 6 header rows to skip (metadata, source info, blank lines)
    - Data starts at row 7
    - Columns: Origin (country name), Origin (Code) (ISO2),
               Office (total patents at that office — not used),
               then one column per year: '1980', '1981', ..., '2024'
    - Wide format: one row per country, years as columns
    - Empty cells = no patents (treated as 0 or absent)
    - Country identifier: 'Origin (Code)' = ISO2

WHY WIDE FORMAT IS MELTED TO TALL:
    The silver layer stores one row per country per year.
    The WIPO CSV stores one row per country with years as columns.
    pd.melt() converts wide to tall efficiently in one operation.

WHY ISO2 CODES ARE STORED AS-IS AT INGESTION TIME:
    The transformation script maps ISO2 → ISO3 using
    metadata.country_codes where source_id='wipo_ip'.
    Storing the raw ISO2 code at ingestion keeps the bronze
    file faithful to the source.

WHY EMPTY CELLS ARE DROPPED (not stored as 0):
    Absence of a row = absence of data. A country with no AI
    patents in a year simply has no observation. Storing 0
    would conflate "zero patents" with "no data reported" —
    WIPO does not distinguish between these in the CSV.

RAW ROW COUNT:
    Number of (country, year) pairs where the value is not
    empty, counted after melting. This is what
    check_ingestion_pre() compares against len(df).

B2 FILE NAMING:
    bronze/wipo_ip/full_file_{retrieved_date}.csv

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff for B2 read failures.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import io
import time
import pandas as pd
from datetime import date
from sqlalchemy import text

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Number of header rows to skip in the WIPO CSV.
# Rows 1-6 contain: title, blank, indicator description,
# blank, source line, blank. Data starts at row 7.
WIPO_HEADER_ROWS = 6

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]


class WIPOIngestor(BaseIngestor):
    """
    Ingestion script for WIPO IP Statistics AI Patents.
    Inherits all pipeline orchestration from BaseIngestor.
    """

    def __init__(self):
        super().__init__(source_id='wipo_ip')
        self.run_date = date.today().isoformat()


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        WIPO is a single full-file source. Always returns
        ['full_file'] — one batch unit, one B2 file.

        WHY SINGLE BATCH UNIT:
            WIPO publishes one complete CSV covering all years
            (1980-2024) and all countries in one file. There is
            no meaningful sub-unit to split on. The entire file
            is downloaded once, processed once, and checkpointed
            once per run.

        Args:
            since_date: Not used. WIPO file covers full history.
        """
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Read the WIPO CSV from B2, melt from wide to tall,
        and return the standardized DataFrame.

        Args:
            batch_unit: Always 'full_file' for WIPO.
            since_date: Used to filter years after melting.

        Returns:
            (raw_row_count, df)
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._read_wipo_file(since_date)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] WIPO: {e}. "
                        f"Waiting {delay}s..."
                    )
                    time.sleep(delay)

        raise RuntimeError(
            f"WIPO failed after {len(RETRY_DELAYS)} retries. "
            f"Last error: {last_error}"
        )

    def _read_wipo_file(self, since_date: date) -> tuple:
        """
        Download WIPO CSV from B2 and parse to canonical shape.

        Wide → tall transformation:
            Before melt (wide):
                Origin | Origin (Code) | Office | 1980 | 1981 | ...
                France |      FR       | 628.0  |  2   |  5   | ...

            After melt (tall):
                country_iso3 | year | value
                    FR       | 1980 |   2
                    FR       | 1981 |   5

        Args:
            since_date: Filter years from since_date.year.

        Returns:
            (raw_row_count, df)
        """
        b2_key     = self.get_b2_key('full_file')
        csv_bytes  = self.b2.download(b2_key)

        # ── Read CSV skipping header rows ──────────────────────
        # WIPO CSV has 6 header rows of metadata before the data.
        # dtype=str reads everything as string to prevent pandas
        # from misinterpreting ISO2 codes or year numbers.
        df_wide = pd.read_csv(
            io.BytesIO(csv_bytes),
            skiprows = WIPO_HEADER_ROWS,
            dtype    = str,
        )

        # ── Identify year columns ──────────────────────────────
        # Year columns are 4-digit numeric strings (1980-2024).
        # Other columns: 'Origin', 'Origin (Code)', 'Office'.
        year_cols = [
            col for col in df_wide.columns
            if str(col).strip().isdigit() and len(str(col).strip()) == 4
        ]

        if not year_cols:
            raise ValueError(
                "No year columns found in WIPO CSV. "
                "File format may have changed."
            )

        # ── Melt wide → tall ───────────────────────────────────
        # pd.melt() is the most efficient way to convert wide
        # format to tall format in pandas. One vectorized
        # operation instead of a nested loop.
        df_tall = df_wide.melt(
            id_vars    = ['Origin', 'Origin (Code)'],
            value_vars = year_cols,
            var_name   = 'year_str',
            value_name = 'patent_count',
        )

        # ── Drop empty cells ───────────────────────────────────
        # Empty cells in the wide CSV become NaN after melt.
        # Absence of a row = absence of data (design principle).
        df_tall = df_tall[
            df_tall['patent_count'].notna() &
            (df_tall['patent_count'].str.strip() != '')
        ].copy()

        # raw_row_count = non-empty (country, year) pairs.
        raw_row_count = len(df_tall)

        # ── Apply since_date year filter ───────────────────────
        # WIPO file covers 1980-2024. We only upsert rows within
        # the 5-year revision window. Earlier rows are already
        # in the silver layer from previous runs.
        since_year = since_date.year
        df_tall = df_tall[
            df_tall['year_str'].astype(int) >= since_year
        ].copy()

        # ── Build canonical DataFrame ──────────────────────────
        df = self._parse_wipo(df_tall)

        return raw_row_count, df

    def _parse_wipo(self, df_tall: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the melted DataFrame to canonical silver layer shape.

        WHY ISO2 IS STORED AS-IS:
            'Origin (Code)' contains ISO2 codes (e.g. 'LB', 'US').
            The transformation script maps ISO2 → ISO3 using
            metadata.country_codes where source_id='wipo_ip'.
            Storing ISO2 faithfully keeps the bronze file matching
            the source exactly.

        WHY PATENT COUNT IS CAST TO INT THEN STRING:
            WIPO counts are integers. Pandas reads them as floats
            after the wide CSV read (e.g. '2.0'). Casting to int
            first gives '2' not '2.0' in the silver layer, which
            is correct for a count metric.

        Args:
            df_tall: Melted DataFrame with year_str and patent_count.

        Returns:
            Canonical DataFrame.
        """
        rows = []
        for _, row in df_tall.iterrows():
            iso2          = str(row.get('Origin (Code)', '') or '').strip()
            year_str      = str(row.get('year_str', '') or '').strip()
            patent_count  = str(row.get('patent_count', '') or '').strip()

            if not iso2 or not year_str or not patent_count:
                continue

            try:
                year  = int(year_str)
                count = int(float(patent_count))
            except (ValueError, TypeError):
                continue

            # Skip zero counts — absence of data, not zero patents.
            if count == 0:
                continue

            rows.append({
                'country_iso3': iso2,       # ISO2 stored as-is
                'year':         year,
                'period':       'annual',
                'metric_id':    'wipo.ai_patent_count',
                'value':        str(count),
                'source_id':    'wipo_ip',
                'retrieved_at': date.today().isoformat(),
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=[
                'country_iso3', 'year', 'period', 'metric_id',
                'value', 'source_id', 'retrieved_at'
            ]
        )


    # ═══════════════════════════════════════════════════════
    # fetch_metric_metadata
    # ═══════════════════════════════════════════════════════

    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        WIPO has exactly one metric — wipo.ai_patent_count —
        hardcoded in seed_metrics.py. Should never be called.
        """
        return {
            'metric_id':   'wipo.ai_patent_count',
            'metric_name': 'AI Patent Applications',
            'source_id':   'wipo_ip',
            'category':    'AI Production',
            'unit':        'count',
            'description': (
                'AI patent applications (IPC class G06N) by '
                'country of origin per year. '
                'Source: wipo.int/en/web/ip-statistics'
            ),
            'frequency':   'annual',
        }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 key for the WIPO CSV.

        Naming convention:
            bronze/wipo_ip/full_file_2026-04-20.csv
        """
        return f"bronze/wipo_ip/{batch_unit}_{self.run_date}.csv"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """Serialize DataFrame to CSV bytes for B2 upload."""
        return df.to_csv(index=False).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize CSV bytes from B2 into DataFrame."""
        return pd.read_csv(io.BytesIO(data), dtype=str)


if __name__ == "__main__":
    ingestor = WIPOIngestor()
    ingestor.run()
