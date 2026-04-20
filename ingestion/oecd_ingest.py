"""
ingestion/oecd_ingest.py
========================
Ingestion script for OECD Main Science and Technology Indicators.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

BATCH UNIT DESIGN — PER METRIC:
    The OECD API returns all three metrics in one CSV call.
    We split the response by metric_id and process each as a
    separate batch unit: 'berd_gdp', 'goverd_gdp',
    'researchers_per_thousand'.

    WHY PER METRIC (not full_file):
        1. Restart recovery: if metric 2 fails, restart skips
           metric 1 and resumes from metric 2. Fail on metric 2
           with full_file = reprocess everything.
        2. Auditability: ops.checkpoints shows exactly which
           metric failed, not just "something failed."
        3. Consistency: every source checkpoints at the smallest
           meaningful unit. For OECD that is one metric.
        4. Direct inspectability: per-metric B2 files can be
           opened and examined for exactly one metric without
           loading the full dataset.

    THE API IS CALLED ONCE regardless of metric count.
    The full response is cached in memory and split by metric.
    Subsequent fetch() calls read from cache — zero extra API calls.

B2 FILE NAMING (per metric):
    bronze/oecd_msti/berd_gdp_2026-04-21.csv
    bronze/oecd_msti/goverd_gdp_2026-04-21.csv
    bronze/oecd_msti/researchers_per_thousand_2026-04-21.csv

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

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

OECD_API_BASE       = "https://sdmx.oecd.org/public/rest/data"
OECD_DATAFLOW       = "OECD.STI.STP,DSD_MSTI@DF_MSTI,"
OECD_DIMENSION_PATH = ".A.B+GV+T_RS.10P3EMP+PT_B1GQ.."
AGGREGATES_TO_EXCLUDE = {'EU27_2020', 'OECD'}
RETRY_DELAYS = [60, 300, 600]

# OECD measure code → our metric_id.
MEASURE_TO_METRIC = {
    'B':    'oecd.berd_gdp',
    'GV':   'oecd.goverd_gdp',
    'T_RS': 'oecd.researchers_per_thousand',
}

# Batch unit names = metric_id suffix after 'oecd.'
BATCH_UNITS = ['berd_gdp', 'goverd_gdp', 'researchers_per_thousand']

# Batch unit → metric_id
BATCH_TO_METRIC = {
    'berd_gdp':                 'oecd.berd_gdp',
    'goverd_gdp':               'oecd.goverd_gdp',
    'researchers_per_thousand': 'oecd.researchers_per_thousand',
}


class OECDIngestor(BaseIngestor):

    def __init__(self):
        super().__init__(source_id='oecd_msti')
        self.run_date    = date.today().isoformat()
        self._full_df    = None  # cached after first API call
        self._since_date = None
        self.session     = requests.Session()
        self.session.headers.update({'Accept': 'application/csv'})


    def get_batch_units(self, since_date: date) -> list:
        """
        Return ['berd_gdp', 'goverd_gdp', 'researchers_per_thousand'].
        Stores since_date for use in fetch() without encoding it
        in the batch_unit string.
        """
        self._since_date = since_date
        return BATCH_UNITS


    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        """
        Return rows for one metric from the cached full dataset.
        Downloads the full CSV on the first call only — subsequent
        calls read from self._full_df cache. One API call total.

        Args:
            batch_unit: One of BATCH_UNITS.
            since_date: Used for API call on first fetch only.

        Returns:
            (raw_row_count, df) — rows for this metric only.
        """
        # ── Download full dataset on first call only ───────────
        if self._full_df is None:
            last_error = None
            for attempt, delay in enumerate(RETRY_DELAYS, 1):
                try:
                    self._full_df = self._fetch_full_csv(since_date)
                    break
                except Exception as e:
                    last_error = e
                    if attempt < len(RETRY_DELAYS):
                        print(f"    [retry {attempt}] OECD: {e}. Waiting {delay}s...")
                        time.sleep(delay)
            else:
                raise RuntimeError(
                    f"OECD failed after {len(RETRY_DELAYS)} retries. "
                    f"Last error: {last_error}"
                )

        # ── Extract this metric's rows ─────────────────────────
        metric_id = BATCH_TO_METRIC[batch_unit]
        df_metric = self._full_df[
            self._full_df['metric_id'] == metric_id
        ].copy().reset_index(drop=True)

        return len(df_metric), df_metric


    def _fetch_full_csv(self, since_date: date) -> pd.DataFrame:
        """One API call — returns full standardized DataFrame."""
        url = f"{OECD_API_BASE}/{OECD_DATAFLOW}/{OECD_DIMENSION_PATH}"
        params = {
            'startPeriod':            since_date.year,
            'endPeriod':              date.today().year,
            'dimensionAtObservation': 'AllDimensions',
            'format':                 'csvfilewithlabels',
        }
        response = self.session.get(url, params=params, timeout=120)
        response.raise_for_status()

        df_raw = pd.read_csv(
            io.BytesIO(response.content),
            dtype=str, low_memory=False,
        )

        if 'REF_AREA' in df_raw.columns:
            df_raw = df_raw[
                ~df_raw['REF_AREA'].isin(AGGREGATES_TO_EXCLUDE)
            ].copy()

        return self._parse_csv(df_raw)


    def _parse_csv(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        required = {'REF_AREA', 'MEASURE', 'TIME_PERIOD', 'OBS_VALUE'}
        missing  = required - set(df_raw.columns)
        if missing:
            raise ValueError(f"OECD CSV missing columns: {missing}")

        rows = []
        for _, row in df_raw.iterrows():
            measure   = str(row.get('MEASURE', '')).strip()
            metric_id = MEASURE_TO_METRIC.get(measure)
            if not metric_id:
                continue

            obs_value = str(row.get('OBS_VALUE', '')).strip()
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
            columns=['country_iso3', 'year', 'period', 'metric_id',
                     'value', 'source_id', 'retrieved_at']
        )


    def fetch_metric_metadata(self, metric_id: str) -> dict:
        metric_to_measure = {v: k for k, v in MEASURE_TO_METRIC.items()}
        measure = metric_to_measure.get(metric_id, metric_id)
        return {
            'metric_id':   metric_id,
            'metric_name': metric_id,
            'source_id':   'oecd_msti',
            'category':    'Research & Development',
            'unit':        None,
            'description': f'OECD MSTI measure code: {measure}.',
            'frequency':   'biannual',
        }


    def get_b2_key(self, batch_unit: str) -> str:
        """
        bronze/oecd_msti/{metric_name}_{date}.csv
        e.g. bronze/oecd_msti/berd_gdp_2026-04-21.csv
        """
        return f"bronze/oecd_msti/{batch_unit}_{self.run_date}.csv"


    def serialize(self, df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False)


if __name__ == "__main__":
    OECDIngestor().run()
