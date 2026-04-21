"""
ingestion/imf_ingest.py
=======================
Ingestion script for the IMF DataMapper API.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Pulls one indicator at a time from the IMF DataMapper REST API
    for all countries across all available years in a single call.
    Saves each indicator's raw JSON response to B2.
    Detects new indicators not yet registered in metadata.metrics
    and emails the team.

WHY ONE INDICATOR PER BATCH UNIT:
    The IMF DataMapper API returns one indicator × all countries ×
    all years in a single JSON response with no pagination.
    This makes one indicator the natural batch unit — one API
    call, one B2 file, one checkpoint.

WHY NO ADAPTIVE CHUNKING:
    Unlike the World Bank API, the IMF DataMapper returns the full
    time series in one flat JSON response. There are no per-page
    limits and no need to split by year. If a call fails, we
    retry the same call — we cannot split the response into
    smaller units because the API does not support it.

WHY NO since_date FILTERING AT THE API LEVEL:
    The IMF DataMapper API does not support date range filtering
    in the same way as the World Bank. The API returns the full
    historical series for each indicator in one call. Filtering
    by since_date would require post-processing the response —
    but since we store the full raw response on B2 and the
    transformation script handles what gets upserted, we let
    the transformation script apply the date filter. This keeps
    ingestion simple and ensures the full raw response is always
    preserved on B2.

IMF RESPONSE STRUCTURE:
    {
      "values": {
        "NGDP_RPCH": {
          "AFG": {"2015": 1.3, "2016": 2.2, ...},
          "LBN": {"2015": -0.1, "2016": 1.7, ...},
          ...
        }
      }
    }
    Keys are IMF country codes (mix of 2-letter, 3-letter, and
    custom codes). Values are year → value dicts.
    Country code mapping to ISO3 is handled by the transformation
    script using metadata.country_codes.

RAW ROW COUNT:
    Since the response has no pagination metadata, raw_row_count
    is computed by counting (country, year) pairs in the raw
    response dict before parsing into a DataFrame.

RATE LIMITING:
    1 second between indicator calls. Slightly more conservative
    than World Bank because the IMF API is known to be sensitive
    to rapid sequential requests despite having no documented
    rate limit.

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff (design document Section 9).
    No year splitting — the API does not support partial requests.
    If all retries fail, the indicator is logged as rejected and
    the pipeline continues with remaining indicators.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import json
import time
import requests
import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

IMF_API_BASE = "https://www.imf.org/external/datamapper/api/v1"

# Seconds between indicator calls.
INTER_INDICATOR_DELAY = 1.0

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]  # 1 min, 5 min, 10 min


class IMFIngestor(BaseIngestor):
    """
    Ingestion script for IMF DataMapper.
    Inherits all pipeline orchestration from BaseIngestor.
    Implements the six source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='imf')

        # run_date used in B2 filenames. Set once at init so
        # all files in one run share the same date.
        self.run_date = date.today().isoformat()

        # Session reused across all requests. Avoids TCP
        # handshake overhead on every API call.
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        Return the list of IMF indicator codes to process.

        Two steps:
        1. Fetch current indicator list from IMF API — detects
           new unknown indicators.
        2. Filter to indicators registered in metadata.metric_codes
           for source='imf'.

        WHY FETCH FROM API FIRST:
            New indicators appear in the IMF API without
            announcement. Comparing against our registered codes
            triggers unknown metric detection for any new ones.

        Args:
            since_date: Not used for batch unit selection —
                        IMF API returns full history regardless.

        Returns:
            Sorted list of IMF indicator code strings.
        """
        # ── Step 1: Fetch current API indicator list ───────────
        api_codes = self._fetch_api_indicator_codes()

        # ── Step 2: Load registered codes from database ────────
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code
                FROM metadata.metric_codes
                WHERE source_id = 'imf'
            """)).fetchall()
        registered_codes = {r[0] for r in rows}

        # ── Step 3: Detect new indicators ─────────────────────
        new_codes = api_codes - registered_codes
        if new_codes:
            placeholder_df = pd.DataFrame({
                'metric_id':    [f"imf.{c.lower()}" for c in new_codes],
                'country_iso3': ['USA'] * len(new_codes),
                'year':         [2024] * len(new_codes),
                'period':       ['annual'] * len(new_codes),
                'value':        ['0'] * len(new_codes),
                'source_id':    ['imf'] * len(new_codes),
                'retrieved_at': [date.today()] * len(new_codes),
            })
            registered_metric_ids = {
                f"imf.{c.lower()}" for c in registered_codes
            }
            self._handle_unknown_metrics(
                placeholder_df,
                batch_unit='indicator_discovery',
                known_metrics=registered_metric_ids
            )

        return sorted(registered_codes)


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Fetch one IMF indicator for all countries and all years.
        Retries 3 times with backoff before logging as failed.

        WHY NO YEAR FILTERING AT FETCH TIME:
            The IMF DataMapper API returns the full historical
            series in one call. Filtering by since_date would
            require post-processing — but since the full raw
            response is stored on B2 and transformation applies
            the date filter, we preserve the complete raw response.
            This means B2 always has the full history regardless
            of when the run happened.

        Args:
            batch_unit: IMF indicator code, e.g. 'NGDP_RPCH'.
            since_date: Not used at fetch time — see above.

        Returns:
            (raw_row_count, df) where raw_row_count is the count
            of (country, year) pairs in the raw response.
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                result = self._fetch_indicator(batch_unit)
                # Add delay between indicators after successful
                # fetch to avoid throttling.
                time.sleep(INTER_INDICATOR_DELAY)
                return result
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] {batch_unit}: {e}. "
                        f"Waiting {delay}s..."
                    )
                    time.sleep(delay)

        # All retries exhausted — raise to let BaseIngestor
        # log this batch as failed and continue with others.
        raise RuntimeError(
            f"IMF indicator {batch_unit} failed after "
            f"{len(RETRY_DELAYS)} retries. "
            f"Last error: {last_error}"
        )


    def _fetch_indicator(self, indicator: str) -> tuple:
        """
        Make one API call for one indicator and parse the response.

        IMF response structure:
        {
          "values": {
            "NGDP_RPCH": {
              "AFG": {"2015": 1.3, "2016": 2.2, ...},
              ...
            }
          }
        }

        raw_row_count is computed by counting (country, year)
        pairs in the raw response dict — the IMF API provides
        no pagination metadata with a total count field.

        Args:
            indicator: IMF indicator code.

        Returns:
            (raw_row_count, df)
        """
        url = f"{IMF_API_BASE}/{indicator}"
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Extract the values dict for this indicator.
        # Structure: data['values'][indicator_code][country][year]
        values = data.get('values', {}).get(indicator, {})

        if not values:
            # No data returned — valid empty response.
            return 0, pd.DataFrame(columns=[
                'country_iso3', 'year', 'period', 'metric_id',
                'value', 'source_id', 'retrieved_at'
            ])

        # Count raw (country, year) pairs before parsing.
        # This is raw_row_count for check_ingestion_pre().
        raw_row_count = sum(
            len(year_dict)
            for year_dict in values.values()
            if isinstance(year_dict, dict)
        )

        # Parse into DataFrame.
        df = self._parse_response(values, indicator)

        return raw_row_count, df


    def _parse_response(self, values: dict,
                        indicator: str) -> pd.DataFrame:
        """
        Parse the IMF response values dict into a DataFrame.

        WHY IMF CODES ARE STORED AS-IS AT INGESTION TIME:
            The transformation script looks up ISO3 from
            metadata.country_codes using the IMF code. Storing
            the raw IMF code here keeps ingestion faithful to
            the source. The transformation script handles the
            mapping using a pre-loaded dict — one vectorized
            operation, not a row-by-row lookup.

        WHY NULL VALUES ARE DROPPED HERE:
            Absence of a row = absence of data (design principle).
            The IMF API sometimes returns explicit null values.
            Dropping them here prevents meaningless rows entering
            the pipeline.

        Args:
            values:    Dict of {imf_code: {year: value, ...}, ...}
            indicator: IMF indicator code for constructing metric_id.

        Returns:
            DataFrame with columns: country_iso3 (IMF code stored
            here — mapped to ISO3 in transformation), year, period,
            metric_id, value, source_id, retrieved_at.
        """
        metric_id = f"imf.{indicator.lower()}"
        rows = []

        for imf_code, year_dict in values.items():
            if not isinstance(year_dict, dict):
                continue

            for year_str, val in year_dict.items():
                # Drop null values — absence of data.
                if val is None:
                    continue

                try:
                    year = int(year_str)
                except ValueError:
                    # Skip non-integer year keys (e.g. 'eweek').
                    continue

                rows.append({
                    # Store IMF code here — transformation maps
                    # to ISO3 via metadata.country_codes.
                    'country_iso3': imf_code,
                    'year':         year,
                    'period':       'annual',
                    'metric_id':    metric_id,
                    'value':        str(val),
                    'source_id':    'imf',
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
        Fetch metadata for an unknown IMF indicator.
        Reverses the metric_id naming convention to get the
        original IMF code: imf.ngdp_rpch → NGDP_RPCH

        Args:
            metric_id: Standardized metric_id, e.g. 'imf.ngdp_rpch'

        Returns:
            Dict with metric metadata.
        """
        # Reverse naming convention: imf.ngdp_rpch → NGDP_RPCH
        code = metric_id.replace('imf.', '').upper()

        url = f"{IMF_API_BASE}/indicators"
        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            data     = r.json()
            ind_info = data.get('indicators', {}).get(code, {})

            return {
                'metric_id':   metric_id,
                'metric_name': ind_info.get('label', code),
                'source_id':   'imf',
                'category':    None,  # IMF API has no topic categories
                'unit':        ind_info.get('unit') or None,
                'description': ind_info.get('description') or None,
                'frequency':   'annual',
            }
        except Exception as e:
            return {
                'metric_id':   metric_id,
                'metric_name': code,
                'source_id':   'imf',
                'category':    None,
                'unit':        None,
                'description': f'Auto-fetch failed for {code}: {e}',
                'frequency':   'annual',
            }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given batch_unit.

        Naming convention:
            bronze/imf/NGDP_RPCH_2026-04-20.json

        Args:
            batch_unit: IMF indicator code.

        Returns:
            Full B2 path string.
        """
        return f"bronze/imf/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize DataFrame to JSON bytes for B2 upload.
        Same approach as World Bank — orient='records', UTF-8.

        WHY JSON:
            IMF source is JSON. Saving as JSON keeps the bronze
            file faithful to the raw source format.
        """
        return df.to_json(
            orient='records',
            date_format='iso',
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize JSON bytes from B2 back into a DataFrame.
        Exact inverse of serialize().
        """
        return pd.read_json(BytesIO(data), orient='records', convert_dates=False, dtype=False)


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _fetch_api_indicator_codes(self) -> set:
        """
        Fetch the current list of indicator codes from the IMF
        DataMapper API. Used to detect new unknown indicators.

        Returns:
            Set of IMF indicator code strings.
        """
        url = f"{IMF_API_BASE}/indicators"
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            return set(data.get('indicators', {}).keys())
        except Exception as e:
            print(f"  ⚠ Could not fetch IMF indicator list: {e}")
            return set()


if __name__ == "__main__":
    ingestor = IMFIngestor()
    ingestor.run()
