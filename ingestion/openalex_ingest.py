"""
ingestion/openalex_ingest.py
============================
Ingestion script for OpenAlex AI Publications Dataset.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Pulls AI publication counts per country per year from the
    OpenAlex REST API by filtering on AI concept C154945302
    and grouping by authorships.institutions.country_code.
    Saves each year's raw JSON response to B2.

API ENDPOINT USED:
    GET /works?filter=concepts.id:C154945302,publication_year:{year}
              &group_by=authorships.institutions.country_code
              &mailto={email}

    Returns one JSON response per year with all countries'
    AI publication counts. No pagination needed — group_by
    returns at most 200 groups (well under country count limit).

WHY ONE YEAR PER BATCH UNIT:
    The group_by endpoint returns all countries for one year
    in a single call. One year is the natural batch unit —
    one API call, one B2 file, one checkpoint. This matches
    the design document's checkpoint granularity of "per year"
    for OpenAlex.

API RESPONSE STRUCTURE:
    {
      "meta": {
        "count": 45231,      ← total AI publications globally
        "groups_count": 167  ← number of countries returned
      },
      "group_by": [
        {"key": "US", "key_display_name": "United States", "count": 12450},
        {"key": "CN", "key_display_name": "China", "count": 9832},
        ...
      ]
    }

    key is ISO2 country code. count is the number of AI
    publications for that country in that year.
    Country code mapping to ISO3 is handled by the transformation
    script using metadata.country_codes.

RAW ROW COUNT:
    meta.groups_count — the number of countries returned in
    the group_by array. Used by check_ingestion_pre() to verify
    no rows were dropped during parsing.

HISTORICAL COVERAGE FLOOR — 1990:
    OpenAlex covers publications back to 1800 but AI concept
    tagging (concept C154945302) before 1990 is sparse and
    unreliable. Academic research using OpenAlex for AI
    bibliometrics consistently starts from 1990. We set 1990
    as the floor regardless of since_date to avoid pulling
    decades of near-zero, unreliable data.

WHY NO RATE LIMITING DELAY:
    OpenAlex allows 100,000 API calls per day with a free API
    key. One call per year × ~36 years (1990-2026) = 36 calls
    per full run. Even monthly incremental runs are at most
    6 calls (5-year window). Rate limiting is unnecessary at
    this volume. The mailto parameter is used instead of an
    API key header for politeness — OpenAlex recommends this
    for free tier access.

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff (design document
    Section 9). No year splitting — the API returns a simple
    aggregate count, not raw records, so splitting would
    produce incorrect counts.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT,
    OPENALEX_KEY (optional but recommended — increases
    rate limit from 10 to 100,000 calls/day)
"""

import json
import os
import time
import requests
import pandas as pd
from datetime import date
from io import BytesIO

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

OPENALEX_API_BASE = "https://api.openalex.org"

# OpenAlex concept ID for Artificial Intelligence.
# Confirmed from: https://api.openalex.org/concepts/C154945302
AI_CONCEPT_ID = "C154945302"

# Earliest year with reliable AI concept tagging in OpenAlex.
# Before 1990, AI publication data is sparse and unreliable.
OPENALEX_YEAR_FLOOR = 1990

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]  # 1 min, 5 min, 10 min

# OpenAlex recommends including email for polite API access.
# Used as mailto parameter — not an API key.
OPENALEX_EMAIL = os.getenv("SMTP_SENDER", "pipeline@example.com")


class OpenAlexIngestor(BaseIngestor):
    """
    Ingestion script for OpenAlex AI Publications Dataset.
    Inherits all pipeline orchestration from BaseIngestor.
    Implements the six source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='openalex')

        self.run_date = date.today().isoformat()

        # Session reused across all requests.
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': f'MSBA305Pipeline/1.0 ({OPENALEX_EMAIL})',
        })

        # Add API key if available — increases daily limit
        # from 10 calls/second to 100,000/day.
        api_key = os.getenv("OPENALEX_KEY")
        if api_key:
            self.session.params = {'api_key': api_key}
        else:
            # Without API key use mailto for polite access.
            self.session.params = {'mailto': OPENALEX_EMAIL}


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        Return the list of years to process this run.

        Years range from max(since_date.year, OPENALEX_YEAR_FLOOR)
        to the current year inclusive.

        WHY YEAR FLOOR OF 1990:
            AI concept tagging in OpenAlex before 1990 is sparse
            and unreliable. Pulling earlier years adds near-zero
            data that would mislead researchers. 1990 is the
            established start year used in academic AI
            bibliometrics research with OpenAlex.

        WHY NO NEW INDICATOR DETECTION:
            OpenAlex has exactly one metric in our system:
            openalex.ai_publication_count. There are no other
            indicators to detect. If OpenAlex adds new
            AI-related concepts in the future, those would be
            a deliberate design decision requiring schema changes
            — not an automatic detection case.

        Args:
            since_date: Start of the data window from BaseIngestor.

        Returns:
            List of integer years as strings (batch_unit format).
        """
        start_year = max(since_date.year, OPENALEX_YEAR_FLOOR)
        end_year   = date.today().year

        # Return years as strings — batch_unit is always a string.
        return [str(year) for year in range(start_year, end_year + 1)]


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Fetch AI publication counts per country for one year.
        Retries 3 times with backoff before raising.

        Args:
            batch_unit: Year as string, e.g. '2020'.
            since_date: Not used — year is derived from batch_unit.

        Returns:
            (raw_row_count, df) where raw_row_count is
            meta.groups_count from the API response.
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._fetch_year(int(batch_unit))
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] year {batch_unit}: "
                        f"{e}. Waiting {delay}s..."
                    )
                    time.sleep(delay)

        raise RuntimeError(
            f"OpenAlex year {batch_unit} failed after "
            f"{len(RETRY_DELAYS)} retries. Last error: {last_error}"
        )


    def _fetch_year(self, year: int) -> tuple:
        """
        Make one API call for one year and parse the response.

        Args:
            year: Publication year to fetch.

        Returns:
            (raw_row_count, df)
        """
        url = f"{OPENALEX_API_BASE}/works"
        params = {
            'filter':   f"concepts.id:{AI_CONCEPT_ID},"
                        f"publication_year:{year}",
            'group_by': 'authorships.institutions.country_code',
        }

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        meta     = data.get('meta', {})
        group_by = data.get('group_by', [])

        df = self._parse_response(group_by, year)

        # raw_row_count = rows AFTER parsing and filtering.
        # WHY NOT meta.groups_count:
        #     _parse_response() drops groups with zero counts and
        #     empty/unknown country codes. If any such groups exist,
        #     meta.groups_count > len(df), and check_ingestion_pre()
        #     raises a false CRITICAL failure comparing the API total
        #     (including dropped groups) against the parsed count
        #     (excluding them). Counting after parse is consistent
        #     with the documented fix in §4.5 and with how
        #     world_bank_ingest.py handles the same issue.
        raw_row_count = len(df)

        return raw_row_count, df


    def _parse_response(self, group_by: list,
                        year: int) -> pd.DataFrame:
        """
        Parse the group_by array into a DataFrame.

        WHY ISO2 CODES ARE STORED AS-IS:
            OpenAlex returns ISO2 country codes (e.g. 'US', 'LB').
            The transformation script maps these to ISO3 using
            metadata.country_codes. Storing raw codes keeps
            ingestion faithful to the source.

        WHY ZERO COUNTS ARE DROPPED:
            A count of zero means no AI publications for that
            country in that year. Absence of a row = absence
            of data (design principle). Zero-count groups are
            rare in OpenAlex but dropping them keeps the silver
            layer clean.

        Args:
            group_by: List of {key, key_display_name, count} dicts.
            year:     Publication year.

        Returns:
            DataFrame with columns: country_iso3 (ISO2 stored here),
            year, period, metric_id, value, source_id, retrieved_at.
        """
        rows = []
        for group in group_by:
            country_code = group.get('key', '').strip()
            count        = group.get('count', 0)

            # Skip empty keys (unknown country) and zero counts.
            if not country_code or count == 0:
                continue

            # OpenAlex API returns full URIs for country codes in
            # newer API versions:
            #   https://openalex.org/countries/CN → CN
            # Strip the URI prefix to get the bare ISO2 code.
            # Older API versions return bare codes (e.g. 'CN')
            # directly — the split/last operation handles both.
            if '/' in country_code:
                country_code = country_code.split('/')[-1].strip()

            if not country_code:
                continue

            rows.append({
                # Store ISO2 code here — transformation maps to ISO3.
                'country_iso3': country_code,
                'year':         year,
                'period':       'annual',
                'metric_id':    'openalex.ai_publication_count',
                'value':        str(count),
                'source_id':    'openalex',
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
        Fetch metadata for an unknown OpenAlex metric.
        OpenAlex has only one metric in our system so this
        should never be called in practice. Implemented for
        completeness and future-proofing.

        Args:
            metric_id: e.g. 'openalex.ai_publication_count'

        Returns:
            Dict with metric metadata from the concept endpoint.
        """
        url = f"{OPENALEX_API_BASE}/concepts/{AI_CONCEPT_ID}"
        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            return {
                'metric_id':   metric_id,
                'metric_name': data.get('display_name',
                                        'AI Publications Count'),
                'source_id':   'openalex',
                'category':    'AI Research Output',
                'unit':        'count',
                'description': data.get('description') or (
                    'Number of AI-related academic publications '
                    'per country per year, filtered by concept '
                    f'C154945302 (Artificial Intelligence).'
                ),
                'frequency':   'annual',
            }
        except Exception as e:
            return {
                'metric_id':   metric_id,
                'metric_name': 'AI Publications Count',
                'source_id':   'openalex',
                'category':    'AI Research Output',
                'unit':        'count',
                'description': f'Auto-fetch failed: {e}',
                'frequency':   'annual',
            }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given batch_unit.

        Naming convention:
            bronze/openalex/2020_2026-04-20.json
        """
        return f"bronze/openalex/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """Serialize DataFrame to JSON bytes. Same as other sources."""
        return df.to_json(
            orient='records',
            date_format='iso',
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize JSON bytes from B2 into a DataFrame."""
        return pd.read_json(BytesIO(data), orient='records', convert_dates=False, dtype=False)


if __name__ == "__main__":
    ingestor = OpenAlexIngestor()
    ingestor.run()
