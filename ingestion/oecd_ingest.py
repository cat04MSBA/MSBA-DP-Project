"""
ingestion/oecd_ingest.py
========================
Ingestion script for OECD Main Science and Technology Indicators.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Downloads the full OECD MSTI dataset as a single CSV file
    from the OECD SDMX REST API. All three metrics, all OECD
    countries, and full historical range come back in one call.
    Saves the raw CSV bytes to B2.

API ENDPOINT:
    https://sdmx.oecd.org/public/rest/data/
    OECD.STI.STP,DSD_MSTI@DF_MSTI,/
    .A.B+GV+T_RS.10P3EMP+PT_B1GQ..
    ?startPeriod={since_year}&endPeriod={current_year}
    &dimensionAtObservation=AllDimensions
    &format=csvfilewithlabels

WHY ONE BATCH UNIT (full file):
    All three metrics × all OECD countries × full history arrive
    in one CSV response. The dataset is small (~6,750 rows
    at most: 3 metrics × ~50 countries × ~45 years). There is
    no meaningful sub-unit to split on. One API call, one B2
    file, one checkpoint matches the design document's "per full
    file" granularity for sources like this.

WHY CSV FORMAT (not SDMX XML or JSON):
    format=csvfilewithlabels returns a flat CSV with human-readable
    labels alongside codes. Far simpler to parse than SDMX XML
    which requires an XML parser and namespace handling. The CSV
    contains all dimension codes needed for standardization.

WHY since_date IS RESPECTED AT THE API LEVEL:
    Unlike IMF which returns full history regardless, the OECD
    SDMX API supports startPeriod filtering. We use since_date
    (last_retrieved - 5 years) as the startPeriod to respect
    the 5-year revision window. This keeps the downloaded file
    small on incremental runs while still catching retroactive
    revisions. The full history from before since_date is already
    in the silver layer from previous runs.

WHY AGGREGATES ARE EXCLUDED AT INGESTION TIME:
    OECD returns aggregate rows for 'EU27_2020' and 'OECD' which
    are not real countries and have no ISO3 equivalent. Excluding
    them at ingestion (before saving to B2) keeps the bronze file
    clean. These are structural constants of the OECD dataset —
    they will always be present and always need to be excluded.
    Transformation would reject them via fk_validity anyway but
    filtering early avoids storing meaningless rows on B2.

RAW ROW COUNT:
    Number of data rows in the CSV after excluding aggregates
    but before any further filtering. Counted from the raw
    CSV before parsing into a DataFrame.

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff. No splitting —
    the dataset is too small to warrant it and the API
    does not support meaningful sub-unit requests.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import io
import time
import requests
import pandas as pd
from datetime import date
from sqlalchemy import text

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

OECD_API_BASE = "https://sdmx.oecd.org/public/rest/data"

# OECD SDMX dataflow identifier — confirmed from actual CSV.
OECD_DATAFLOW = "OECD.STI.STP,DSD_MSTI@DF_MSTI,"

# Dimension path — all frequencies, three measures, two units.
# B=BERD%GDP, GV=GOVERD%GDP, T_RS=Researchers per thousand.
# 10P3EMP=per thousand employed, PT_B1GQ=% of GDP.
OECD_DIMENSION_PATH = ".A.B+GV+T_RS.10P3EMP+PT_B1GQ.."

# Aggregate rows returned by OECD that are not real countries.
# Always excluded — they have no ISO3 equivalent.
AGGREGATES_TO_EXCLUDE = {'EU27_2020', 'OECD'}

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]  # 1 min, 5 min, 10 min

# Map OECD measure codes to our standardized metric_ids.
# Keys must match what is in metadata.metric_codes exactly.
MEASURE_TO_METRIC = {
    'B':    'oecd.berd_gdp',
    'GV':   'oecd.goverd_gdp',
    'T_RS': 'oecd.researchers_per_thousand',
}


class OECDIngestor(BaseIngestor):
    """
    Ingestion script for OECD MSTI.
    Inherits all pipeline orchestration from BaseIngestor.
    Implements the six source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='oecd_msti')

        self.run_date = date.today().isoformat()

        # Session reused across requests.
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/csv',
        })


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        Return a single batch unit — the full OECD MSTI dataset.

        WHY A SINGLE BATCH UNIT:
            All three metrics, all countries, and the full
            date range arrive in one CSV response. There is no
            meaningful sub-unit. One batch unit = one API call
            = one B2 file = one checkpoint.

        WHY NO NEW INDICATOR DETECTION:
            OECD MSTI has exactly three metrics in our system,
            all hardcoded. New OECD metrics would require a
            deliberate schema and metric_codes update — not
            automatic detection. MEASURE_TO_METRIC is the
            authoritative mapping.

        Args:
            since_date: Used to construct startPeriod in fetch().
                        Stored as instance variable for access
                        in fetch() without passing through the
                        batch unit string.

        Returns:
            List with one element: 'full_file'.
        """
        # Store since_date for use in fetch() — the batch unit
        # string 'full_file' does not encode the date range,
        # so we keep it here.
        self._since_date = since_date
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Download the full OECD MSTI CSV for the since_date window.
        Retries 3 times with backoff before raising.

        Args:
            batch_unit: Always 'full_file' for OECD.
            since_date: Start of the 5-year revision window.

        Returns:
            (raw_row_count, df)
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._fetch_csv(since_date)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] OECD MSTI: {e}. "
                        f"Waiting {delay}s..."
                    )
                    time.sleep(delay)

        raise RuntimeError(
            f"OECD MSTI failed after {len(RETRY_DELAYS)} retries. "
            f"Last error: {last_error}"
        )


    def _fetch_csv(self, since_date: date) -> tuple:
        """
        Make one API call and return the parsed DataFrame.

        WHY startPeriod USES since_date.year:
            The OECD SDMX API supports year-level filtering via
            startPeriod. Using since_date.year (last_retrieved - 5)
            keeps the download small on incremental runs while
            still covering the 5-year revision window. Historical
            data before this window is already in the silver layer.

        Args:
            since_date: Start of the data window.

        Returns:
            (raw_row_count, df)
        """
        since_year   = since_date.year
        current_year = date.today().year

        url = (
            f"{OECD_API_BASE}/"
            f"{OECD_DATAFLOW}/"
            f"{OECD_DIMENSION_PATH}"
        )
        params = {
            'startPeriod':              since_year,
            'endPeriod':                current_year,
            'dimensionAtObservation':   'AllDimensions',
            'format':                   'csvfilewithlabels',
        }

        response = self.session.get(url, params=params, timeout=120)
        response.raise_for_status()

        # ── Parse CSV to count raw rows ────────────────────────
        # Read into DataFrame immediately — CSV is small enough
        # to hold in memory. We count rows after excluding
        # aggregates as raw_row_count because aggregates are
        # structural noise, not data, and we never intend to
        # store them.
        df_raw = pd.read_csv(
            io.BytesIO(response.content),
            dtype=str,         # read everything as string —
                               # avoids pandas numeric inference
                               # on codes that look like numbers
            low_memory=False,
        )

        # ── Exclude aggregate rows ─────────────────────────────
        # 'REF_AREA' is the OECD column name for country code.
        # Aggregates like 'EU27_2020' and 'OECD' are not countries.
        # Excluded here — before counting raw_row_count — because
        # they are never intended to enter the pipeline.
        if 'REF_AREA' in df_raw.columns:
            df_raw = df_raw[
                ~df_raw['REF_AREA'].isin(AGGREGATES_TO_EXCLUDE)
            ].copy()

        raw_row_count = len(df_raw)

        # ── Standardize into canonical shape ──────────────────
        df = self._parse_csv(df_raw)

        return raw_row_count, df


    def _parse_csv(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Parse the raw OECD CSV DataFrame into the canonical shape.

        OECD CSV column names (from csvfilewithlabels format):
            REF_AREA         → country code (ISO3 directly)
            MEASURE          → variable code (B, GV, T_RS)
            TIME_PERIOD      → year
            OBS_VALUE        → value
            (plus label columns like REF_AREA_LABEL, etc.)

        WHY OECD CODES ARE ISO3 DIRECTLY:
            Unlike IMF (mixed codes) or OpenAlex (ISO2), OECD
            uses ISO3 country codes natively. No crosswalk lookup
            needed — country_iso3 is used as-is.

        WHY ONLY ROWS WITH KNOWN MEASURE CODES ARE KEPT:
            MEASURE_TO_METRIC defines the three metrics we ingest.
            Any other measure code in the CSV is outside our scope.
            Filtering here avoids unknown metric detection emails
            for OECD measures we deliberately chose not to ingest.

        Args:
            df_raw: Raw CSV DataFrame with all columns as strings.

        Returns:
            DataFrame with canonical columns.
        """
        # Verify required columns exist.
        required = {'REF_AREA', 'MEASURE', 'TIME_PERIOD', 'OBS_VALUE'}
        missing  = required - set(df_raw.columns)
        if missing:
            raise ValueError(
                f"OECD CSV missing expected columns: {missing}. "
                f"API response format may have changed."
            )

        rows = []
        for _, row in df_raw.iterrows():
            measure     = str(row.get('MEASURE', '')).strip()
            metric_id   = MEASURE_TO_METRIC.get(measure)

            # Skip measures outside our scope silently.
            # These are not unknown metrics — they are
            # deliberately excluded OECD measures.
            if not metric_id:
                continue

            obs_value = str(row.get('OBS_VALUE', '')).strip()

            # Skip missing or empty values.
            if not obs_value or obs_value.lower() in ('nan', ''):
                continue

            try:
                year = int(str(row.get('TIME_PERIOD', '')).strip())
            except ValueError:
                continue

            rows.append({
                'country_iso3': str(row['REF_AREA']).strip(),
                'year':         year,
                'period':       'annual',
                'metric_id':    metric_id,
                'value':        obs_value,
                'source_id':    'oecd_msti',
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
        Fetch metadata for an unknown OECD metric.
        In practice this should never be called — all three OECD
        metrics are hardcoded and unknown measures are silently
        skipped in _parse_csv(). Implemented for completeness.

        Args:
            metric_id: e.g. 'oecd.berd_gdp'

        Returns:
            Dict with metric metadata.
        """
        # Reverse lookup from metric_id to OECD measure code.
        metric_to_measure = {v: k for k, v in MEASURE_TO_METRIC.items()}
        measure = metric_to_measure.get(metric_id, metric_id)

        return {
            'metric_id':   metric_id,
            'metric_name': metric_id,
            'source_id':   'oecd_msti',
            'category':    'Research & Development',
            'unit':        None,
            'description': (
                f'OECD MSTI measure code: {measure}. '
                f'Source: OECD Main Science and Technology Indicators.'
            ),
            'frequency':   'biannual',
        }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key.

        Naming convention:
            bronze/oecd_msti/full_file_2026-04-20.csv

        WHY CSV EXTENSION:
            The bronze file is saved as raw CSV bytes exactly
            as returned by the OECD API. Using .csv makes the
            format explicit and inspectable without tooling.
        """
        return f"bronze/oecd_msti/{batch_unit}_{self.run_date}.csv"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize DataFrame to CSV bytes for B2 upload.

        WHY CSV NOT JSON:
            The OECD source returns CSV. Saving as CSV keeps
            the bronze file faithful to the raw source format.
            CSV is also directly inspectable in any spreadsheet
            tool without parsing.
        """
        return df.to_csv(index=False).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize CSV bytes from B2 into a DataFrame.
        Reads all columns as strings — same as ingestion.
        """
        return pd.read_csv(
            io.BytesIO(data),
            dtype=str,
            low_memory=False,
        )


if __name__ == "__main__":
    ingestor = OECDIngestor()
    ingestor.run()
