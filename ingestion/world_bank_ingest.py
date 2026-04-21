"""
ingestion/world_bank_ingest.py
==============================
Ingestion script for the World Bank World Development Indicators.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Pulls one indicator at a time from the World Bank REST API
    for all countries across the full date range defined by
    since_date. Saves each indicator's raw JSON response to B2.
    Detects new indicators not yet registered in metadata.metrics
    and emails the team.

WHY ONE INDICATOR PER BATCH UNIT:
    The World Bank API does not support pulling multiple indicators
    in a single data call reliably. One indicator × all countries
    × full date range is the natural unit the API supports and
    the simplest reliable approach. One batch unit = one indicator
    = one checkpoint = one B2 file. Clean and restartable.

ADAPTIVE CHUNKING STRATEGY:
    Try: 1 indicator × all countries × full date range (one call)
    If that fails after 3 retries:
        Split date range in half, try each half independently
        Each half also gets 3 retries before splitting further
        Minimum chunk size: 1 year
        If a single year still fails: log rejection, skip, continue
    This catches transient API failures with retries and handles
    genuine large-response failures by reducing request size.
    Best case: 439 API calls for a full run.
    Worst case: a few indicators need splitting — still far fewer
    than one call per year per indicator.

WHY SEPARATE BATCH UNITS FOR YEAR CHUNKS:
    If an indicator is split into year chunks and the script
    crashes mid-split, restart logic needs to know which chunks
    completed. Giving each chunk its own batch_unit and checkpoint
    means restart skips completed chunks and retries only the
    failed ones. Combining chunks into one batch_unit would force
    a full retry of all chunks on restart.

NEW INDICATOR DETECTION:
    At run start, the script fetches the current indicator list
    from the World Bank API for all 7 topics and compares against
    metadata.metric_codes. Any indicator in the API but not in
    our database triggers the unknown metric email via
    _handle_unknown_metrics() in BaseIngestor.

WHY countryiso3code IS USED DIRECTLY:
    The World Bank API returns both a 2-letter country code
    (country.id) and a 3-letter ISO3 code (countryiso3code)
    in every observation. Using countryiso3code directly avoids
    any country code lookup — no join against metadata.country_codes
    needed at ingestion time. The transformation script uses it
    as-is for the country_iso3 column.

WHY per_page=1000:
    The World Bank API supports up to 1000 records per page.
    Using the maximum page size minimizes the number of HTTP
    requests needed to collect all pages for one indicator.
    Fewer requests = faster ingestion = less chance of hitting
    rate limits.

WHY 0.5s DELAY BETWEEN INDICATORS:
    The World Bank API has no documented rate limit but throttles
    heavy usage in practice. A 0.5 second delay between indicators
    adds ~3.5 minutes to a full 439-indicator run — negligible —
    but prevents throttling that would trigger retries and slow
    the run down far more.

RATE LIMITING:
    0.5 seconds between indicator calls (not between page calls
    within one indicator — pages for the same indicator are
    fetched as fast as possible since they are part of one
    logical request).

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import json
import time
import requests
import pandas as pd
from datetime import date, datetime
from io import BytesIO

from database.base_ingestor import BaseIngestor
from database.email_utils import send_critical_alert

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# World Bank REST API base URL.
WB_API_BASE = "https://api.worldbank.org/v2"

# Maximum records per API page. 1000 is the API maximum —
# using the max minimizes HTTP requests per indicator.
WB_PER_PAGE = 1000

# Topic IDs matching seed_metrics.py exactly.
# These determine which indicators we pull.
WB_TOPIC_IDS = [1, 2, 4, 5, 9, 14, 21]

# Seconds to wait between indicator calls.
# Prevents throttling without meaningfully slowing the run.
INTER_INDICATOR_DELAY = 0.5

# Retry backoff delays in seconds (design document Section 9).
RETRY_DELAYS = [60, 300, 600]  # 1 min, 5 min, 10 min

# Minimum year chunk size for adaptive splitting.
# If a single year still fails after 3 retries, we reject and
# log — we cannot split smaller than one year.
MIN_CHUNK_YEARS = 1

# Education indicator allowlist — exactly the 41 featured indicators
# from the World Bank Education topic page (worldbank.org/en/topic/education).
# Topic 4 returns thousands of indicators across all WB databases.
# This allowlist restricts new indicator detection to the same 41
# indicators we registered at seeding time — prevents emails for
# every unregistered education indicator in the full topic 4 list.
# Must match EDUCATION_ALLOWLIST in seed_metrics.py and
# seed_metric_codes.py exactly.
EDUCATION_ALLOWLIST = {
    'SE.PRM.UNER.FE', 'SE.PRM.UNER.MA',
    'SE.XPD.TOTL.GD.ZS', 'SE.XPD.TOTL.GB.ZS',
    'SE.XPD.PRIM.PC.ZS', 'SE.XPD.SECO.PC.ZS', 'SE.XPD.TERT.PC.ZS',
    'SE.PRM.GINT.FE.ZS', 'SE.PRM.GINT.MA.ZS',
    'SL.TLF.TOTL.FE.ZS', 'SL.TLF.TOTL.IN',
    'SE.ADT.LITR.FE.ZS', 'SE.ADT.LITR.MA.ZS', 'SE.ADT.LITR.ZS',
    'SE.ADT.1524.LT.FE.ZS', 'SE.ADT.1524.LT.MA.ZS', 'SE.ADT.1524.LT.ZS',
    'SE.PRM.PRSL.FE.ZS', 'SE.PRM.PRSL.MA.ZS',
    'SP.POP.0014.TO.ZS', 'SP.POP.1564.TO.ZS',
    'SE.PRM.CMPT.FE.ZS', 'SE.PRM.CMPT.MA.ZS', 'SE.PRM.CMPT.ZS',
    'SE.SEC.PROG.FE.ZS', 'SE.SEC.PROG.MA.ZS',
    'SE.PRM.ENRL.TC.ZS',
    'SE.PRM.REPT.FE.ZS', 'SE.PRM.REPT.MA.ZS',
    'SE.PRE.ENRR',
    'SE.PRM.ENRR', 'SE.PRM.NENR',
    'SE.ENR.PRIM.FM.ZS', 'SE.ENR.PRSC.FM.ZS',
    'SE.SEC.ENRR', 'SE.SEC.NENR',
    'SE.TER.ENRR',
    'SE.PRM.TCAQ.ZS',
    'SL.UEM.TOTL.FE.ZS', 'SL.UEM.TOTL.MA.ZS', 'SL.UEM.TOTL.ZS',
}


class WorldBankIngestor(BaseIngestor):
    """
    Ingestion script for World Bank WDI.
    Inherits all pipeline orchestration from BaseIngestor.
    Implements the six source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='world_bank')

        # Today's date used in B2 filenames and retrieved_at.
        # Set once at init so all files in one run share the
        # same date even if the run crosses midnight.
        self.run_date = date.today().isoformat()

        # Session reused across all requests in one run.
        # Avoids TCP handshake overhead on every API call.
        # Headers set once — applied to all requests.
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
        })


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        Return the list of indicator codes to process this run.

        Two steps:
        1. Fetch current indicator list from World Bank API
           for all 7 topics — detects new unknown indicators.
        2. Filter to only indicators registered in
           metadata.metric_codes for source='world_bank'.

        WHY FETCH FROM API FIRST:
            If a new indicator appeared in the World Bank API
            since our last run, it will be in the API response
            but not in metadata.metric_codes. Comparing the two
            triggers unknown metric detection for new indicators.
            If we only read from metadata.metric_codes, we would
            miss new indicators entirely.

        WHY since_date IS NOT USED HERE:
            since_date determines the date range passed to fetch(),
            not which indicators to pull. All registered indicators
            are always checked on every run — the date range
            controls how far back we pull data for each one.

        Args:
            since_date: Passed through to fetch() — not used here.

        Returns:
            List of World Bank indicator code strings.
            Example: ['NY.GDP.PCAP.CD', 'FP.CPI.TOTL.ZG', ...]
        """
        # ── Step 1: Fetch current API indicator list ───────────
        # Used to detect new indicators not yet in our database.
        api_codes = self._fetch_api_indicator_codes()

        # ── Step 2: Load registered codes from database ────────
        # These are the indicators we will actually pull data for.
        with self.engine.connect() as conn:
            from sqlalchemy import text
            rows = conn.execute(text("""
                SELECT code
                FROM metadata.metric_codes
                WHERE source_id = 'world_bank'
            """)).fetchall()
        registered_codes = {r[0] for r in rows}

        # ── Step 3: Detect new indicators ─────────────────────
        # Any code in the API but not in our database is unknown.
        # _handle_unknown_metrics() in BaseIngestor emails the
        # team and parks the relevant rows.
        # We build a minimal DataFrame to pass to the detection
        # logic — just the metric_ids, no actual data yet.
        new_codes = api_codes - registered_codes
        if new_codes:
            # Build a placeholder DataFrame so BaseIngestor's
            # unknown metric detection can identify and email
            # the team about each new indicator.
            placeholder_df = pd.DataFrame({
                'metric_id':    [f"wb.{c.lower().replace('.','_')}"
                                 for c in new_codes],
                'country_iso3': ['USA'] * len(new_codes),
                'year':         [2024] * len(new_codes),
                'period':       ['annual'] * len(new_codes),
                'value':        ['0'] * len(new_codes),
                'source_id':    ['world_bank'] * len(new_codes),
                'retrieved_at': [date.today()] * len(new_codes),
            })
            # known_metrics is passed as registered metric_ids.
            # Anything not in registered = unknown = email sent.
            registered_metric_ids = {
                f"wb.{c.lower().replace('.','_')}"
                for c in registered_codes
            }
            self._handle_unknown_metrics(
                placeholder_df,
                batch_unit='indicator_discovery',
                known_metrics=registered_metric_ids
            )

        # Return registered codes as the ordered batch unit list.
        # Sort for deterministic order — makes restart predictable.
        return sorted(registered_codes)


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Fetch one indicator for all countries across the full
        date range using adaptive chunking.

        Tries the full date range first. Falls back to year
        splitting if the full range fails after 3 retries.

        batch_unit format:
            Normal:     'NY.GDP.PCAP.CD'
            Year chunk: 'NY.GDP.PCAP.CD_1950-1987'

        Args:
            batch_unit: Indicator code, optionally with year range.
            since_date: Start of the data window.

        Returns:
            (raw_row_count, df) where raw_row_count is the total
            records reported by the API (data[0]['total']),
            and df is the parsed DataFrame.
        """
        # ── Parse batch_unit ───────────────────────────────────
        # Normal batch unit: just the indicator code.
        # Year chunk batch unit: code_YYYY-YYYY.
        if '_' in batch_unit and batch_unit.split('_')[-1][0].isdigit():
            parts         = batch_unit.rsplit('_', 1)
            indicator     = parts[0]
            year_start, year_end = map(int, parts[1].split('-'))
        else:
            indicator  = batch_unit
            year_start = since_date.year
            year_end   = date.today().year

        # ── Fetch with adaptive chunking ───────────────────────
        return self._fetch_with_chunking(
            indicator, year_start, year_end
        )


    def _fetch_with_chunking(self, indicator: str,
                             year_start: int,
                             year_end: int) -> tuple:
        """
        Try to fetch indicator data for the given year range.
        Retries 3 times with backoff then splits the year range
        in half and tries each half independently.
        """
        last_error = None

        for attempt in range(1, len(RETRY_DELAYS) + 1):
            try:
                return self._fetch_indicator(indicator, year_start, year_end)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    delay = RETRY_DELAYS[attempt - 1]
                    print(
                        f"    [retry {attempt}] {indicator} "
                        f"{year_start}-{year_end}: {e}. "
                        f"Waiting {delay}s..."
                    )
                    time.sleep(delay)

        # All retries exhausted — split or give up.
        chunk_size = year_end - year_start + 1

        if chunk_size <= MIN_CHUNK_YEARS:
            raise RuntimeError(
                f"Single year {year_start} for {indicator} "
                f"failed after {len(RETRY_DELAYS)} retries: {last_error}"
            )

        mid = (year_start + year_end) // 2
        print(
            f"    [split] {indicator} {year_start}-{year_end} → "
            f"{year_start}-{mid} + {mid+1}-{year_end}"
        )

        count1, df1 = self._fetch_with_chunking(indicator, year_start, mid)
        count2, df2 = self._fetch_with_chunking(indicator, mid + 1, year_end)

        combined_df    = pd.concat([df1, df2], ignore_index=True)
        combined_count = count1 + count2

        return combined_count, combined_df


    def _fetch_indicator(self, indicator: str,
                         year_start: int,
                         year_end: int) -> tuple:
        """
        Make the actual API call(s) for one indicator + year range.
        Handles pagination automatically — collects all pages
        before returning.

        WHY ALL PAGES IN ONE CALL:
            A single page of 1000 records is an API implementation
            detail, not a meaningful data boundary. Collecting all
            pages inside this function means the caller always gets
            a complete, consistent dataset. The checkpoint is
            written only after all pages are collected and verified.

        Args:
            indicator:  World Bank indicator code.
            year_start: First year (inclusive).
            year_end:   Last year (inclusive).

        Returns:
            (raw_row_count, df) where raw_row_count is the total
            from the API metadata (data[0]['total']).
        """
        url = f"{WB_API_BASE}/country/all/indicator/{indicator}"

        # ── Page 1: get total count and first batch ────────────
        params = {
            'format':   'json',
            'date':     f"{year_start}:{year_end}",
            'per_page': WB_PER_PAGE,
            'page':     1,
        }

        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Validate response structure.
        # The API returns a list of two elements:
        #   [0] pagination metadata dict
        #   [1] list of observations, or None if no data exists
        #
        # BUT the API also returns error responses as a 1-element list:
        #   [{'message': [{'id': '120', 'key': 'Parameter errors', ...}]}]
        # These indicate deprecated indicators, invalid codes, or
        # parameter errors. Treat as empty data — do not retry.
        #
        # Only raise ValueError if the structure is completely
        # unrecognisable (not a list at all).
        if not isinstance(data, list):
            raise ValueError(
                f"Non-list API response for {indicator}: {type(data)}"
            )

        # 1-element list = API error response (deprecated indicator etc.)
        # Return empty DataFrame — no data for this indicator.
        if len(data) < 2:
            print(
                f"    [no data] {indicator}: API returned "
                f"error/empty response — skipping."
            )
            empty_df = pd.DataFrame(columns=[
                'country_iso3', 'year', 'period', 'metric_id',
                'value', 'source_id', 'retrieved_at'
            ])
            return 0, empty_df

        metadata = data[0]
        records  = data[1] or []  # None → empty list (no data)

        # If metadata is not a dict something is genuinely wrong —
        # raise to trigger retry.
        if not isinstance(metadata, dict):
            raise ValueError(
                f"Unexpected metadata structure for {indicator}: "
                f"expected dict, got {type(metadata)}"
            )

        total_pages = metadata.get('pages', 1)

        # ── Collect remaining pages ────────────────────────────
        # Page 1 already collected above. Loop from page 2.
        for page in range(2, total_pages + 1):
            params['page'] = page
            r = self.session.get(url, params=params, timeout=30)
            r.raise_for_status()
            page_data = r.json()
            if isinstance(page_data, list) and len(page_data) > 1:
                page_records = page_data[1] or []
                records.extend(page_records)

        # ── Parse records into DataFrame ───────────────────────
        df = self._parse_records(records, indicator)

        # raw_row_count is the count AFTER null-dropping — the
        # number of rows we actually intend to store. The API's
        # metadata.total includes NULL rows which _parse_records()
        # drops (absence of data = no row). Using the API total
        # would cause check_ingestion_pre() to fail because it
        # would compare the API total (including NULLs) against
        # the parsed count (excluding NULLs).
        raw_row_count = len(df)

        return raw_row_count, df


    def _parse_records(self, records: list,
                       indicator: str) -> pd.DataFrame:
        """
        Parse raw API observation dicts into a standardized
        DataFrame ready for B2 upload and quality checks.

        WHY countryiso3code IS USED DIRECTLY:
            The World Bank API returns both country.id (ISO2)
            and countryiso3code (ISO3) in every observation.
            Using countryiso3code directly means no lookup against
            metadata.country_codes is needed at ingestion time.
            The transformation script uses it as-is.

        WHY NULL VALUES ARE DROPPED HERE:
            A NULL value in the World Bank API means "no data
            for this country-year combination." The design
            principle is: absence of a row = absence of data.
            Storing NULL rows would add millions of meaningless
            rows to the silver layer. We drop them here rather
            than inserting and immediately rejecting them.

        Args:
            records:   List of raw API observation dicts.
            indicator: Indicator code for constructing metric_id.

        Returns:
            DataFrame with columns: country_iso3, year, period,
            metric_id, value, source_id, retrieved_at.
        """
        if not records:
            return pd.DataFrame(columns=[
                'country_iso3', 'year', 'period', 'metric_id',
                'value', 'source_id', 'retrieved_at'
            ])

        # Build metric_id from indicator code.
        # Convention: wb. + lowercase indicator with . replaced by _
        metric_id = f"wb.{indicator.lower().replace('.', '_')}"

        rows = []
        for rec in records:
            # Skip records with NULL values — absence of data.
            if rec.get('value') is None:
                continue

            # countryiso3code is directly usable as country_iso3.
            iso3 = rec.get('countryiso3code', '').strip()
            if not iso3:
                # Skip records with no ISO3 code —
                # these are World Bank aggregates (e.g. 'World',
                # 'Sub-Saharan Africa') which have no ISO3 code.
                continue

            rows.append({
                'country_iso3': iso3,
                'year':         int(rec['date']),
                'period':       'annual',
                'metric_id':    metric_id,
                'value':        str(rec['value']),
                'source_id':    'world_bank',
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
        Fetch metadata for an unknown World Bank indicator from
        the API. Called by BaseIngestor._handle_unknown_metrics()
        when a new indicator is detected.

        Converts metric_id back to the original indicator code
        by reversing the naming convention:
            wb.ny_gdp_pcap_cd → NY.GDP.PCAP.CD

        Args:
            metric_id: Standardized metric_id, e.g. 'wb.ny_gdp_pcap_cd'

        Returns:
            Dict with keys: metric_id, metric_name, source_id,
            category, unit, description, frequency.
        """
        # Reverse the metric_id naming convention to get the
        # original World Bank indicator code.
        # wb.ny_gdp_pcap_cd → NY.GDP.PCAP.CD
        code = metric_id.replace('wb.', '').upper().replace('_', '.')

        url = f"{WB_API_BASE}/indicator/{code}?format=json"

        try:
            r = self.session.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()

            # API returns [metadata, [indicator_dict]]
            if isinstance(data, list) and len(data) > 1 and data[1]:
                ind = data[1][0]
                return {
                    'metric_id':   metric_id,
                    'metric_name': ind.get('name', code),
                    'source_id':   'world_bank',
                    'category':    ind.get('topics', [{}])[0].get(
                                       'value', None
                                   ) if ind.get('topics') else None,
                    'unit':        ind.get('unit') or None,
                    'description': ind.get('sourceNote') or None,
                    'frequency':   'annual',
                }
        except Exception as e:
            # Return a minimal dict if the fetch fails.
            # BaseIngestor handles the None values gracefully.
            pass

        return {
            'metric_id':   metric_id,
            'metric_name': code,
            'source_id':   'world_bank',
            'category':    None,
            'unit':        None,
            'description': f'Auto-fetch failed for {code}',
            'frequency':   'annual',
        }


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given batch_unit.

        Naming convention:
            Normal:     bronze/world_bank/NY.GDP.PCAP.CD_2026-04-20.json
            Year chunk: bronze/world_bank/NY.GDP.PCAP.CD_1950-1987_2026-04-20.json

        The run_date suffix ensures files from different runs
        do not overwrite each other, preserving the full
        history of pulls on B2.

        Args:
            batch_unit: Indicator code, optionally with year range.

        Returns:
            Full B2 path string.
        """
        return f"bronze/world_bank/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize DataFrame to JSON bytes for B2 upload.

        WHY JSON:
            The World Bank API returns JSON. Saving as JSON keeps
            the bronze file as close as possible to the raw source
            format, consistent with the bronze layer philosophy
            of faithful raw storage. Human-readable and inspectable
            on B2 without any tooling.

        WHY orient='records':
            Each row becomes one JSON object. Simple, flat
            structure that maps directly back to a DataFrame
            without any index reconstruction.

        Args:
            df: DataFrame to serialize.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        return df.to_json(
            orient='records',
            date_format='iso',
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize JSON bytes from B2 back into a DataFrame.
        Exact inverse of serialize().

        WHY convert_dates=False:
            pd.read_json() auto-detects date-like strings and
            converts them to int64 timestamps (milliseconds since
            epoch). This means 'retrieved_at' comes back as
            1745193600000 instead of '2026-04-21', breaking the
            checksum comparison between df_before and df_after.
            convert_dates=False preserves all values as-is,
            matching the original DataFrame exactly.

        WHY dtype=False:
            Prevents pandas from inferring types differently on
            read-back vs the original DataFrame. Keeps all values
            as strings/numbers matching what serialize() wrote.
        """
        return pd.read_json(
            BytesIO(data),
            orient        = 'records',
            convert_dates = False,
            dtype         = False,
        )


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _fetch_api_indicator_codes(self) -> set:
        """
        Fetch the current list of indicator codes from the World
        Bank API for all 7 topics. Used to detect new indicators
        not yet registered in our database.

        WHY FETCH AT RUN START:
            Sources add new indicators without announcement.
            Comparing the API's current list against our registered
            codes ensures we never miss a new indicator. Without
            this check, new indicators would be silently skipped
            until someone noticed manually.

        Returns:
            Set of indicator code strings from the API.
        """
        api_codes = set()

        for topic_id in WB_TOPIC_IDS:
            page = 1
            while True:
                url = (
                    f"{WB_API_BASE}/topic/{topic_id}/indicator"
                    f"?format=json&per_page=500&page={page}"
                )
                try:
                    r = self.session.get(url, timeout=20)
                    r.raise_for_status()
                    data = r.json()

                    if not data or len(data) < 2 or not data[1]:
                        break

                    for ind in data[1]:
                        code = ind.get('id')
                        if code:
                            # Education topic (4) returns thousands of
                            # indicators across all WB databases.
                            # Apply the same allowlist used at seeding
                            # time so new indicator detection only flags
                            # indicators we actually care about.
                            if topic_id == 4 and code not in EDUCATION_ALLOWLIST:
                                continue
                            api_codes.add(code)

                    total_pages = data[0].get('pages', 1)
                    if page >= total_pages:
                        break
                    page += 1

                except Exception as e:
                    print(
                        f"  ⚠ Could not fetch topic {topic_id} "
                        f"page {page}: {e}"
                    )
                    break

        return api_codes


if __name__ == "__main__":
    # Allow running directly for development and testing:
    # python3 ingestion/world_bank_ingest.py
    ingestor = WorldBankIngestor()
    ingestor.run()
