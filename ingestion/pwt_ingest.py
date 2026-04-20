"""
ingestion/pwt_ingest.py
=======================
Ingestion script for Penn World Tables 11.0.

BATCH UNIT DESIGN — PER METRIC:
    PWT has 47 metric variables. Each is processed as a separate
    batch unit — batch_unit = variable name (e.g. 'rgdpe', 'pop').

    WHY PER METRIC IS CRITICAL FOR PWT:
        PWT is the largest source — up to 640,000 rows (185
        countries × 74 years × 47 metrics before NaN dropping).
        If the script crashes on metric 40, per-metric checkpointing
        means restart skips metrics 1-39 and resumes from metric 40.
        Without per-metric, a crash on metric 40 forces reprocessing
        all 39 completed metrics — potentially an hour of wasted work.

    THE STATA FILE IS READ ONCE. The full DataFrame is cached in
    memory and each fetch() call extracts one metric's rows from
    the cache. No re-reading the file per metric.

B2 FILE NAMING (per metric):
    bronze/pwt/rgdpe_2026-04-21.json
    bronze/pwt/pop_2026-04-21.json
    ... one per variable

FILE PATH:
    The original .dta is uploaded by upload_to_b2.py to:
        bronze/pwt/source_{date}.dta
    Ingestion reads from there, processes per metric, saves
    per-metric JSON files.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import time
import pandas as pd
from datetime import date
from io import BytesIO

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# All 47 PWT metric variables — matches seed_metrics.py exactly.
PWT_METRIC_VARIABLES = [
    'rgdpe', 'rgdpo', 'pop', 'emp', 'avh', 'hc', 'ccon', 'cda',
    'cgdpe', 'cgdpo', 'cn', 'ck', 'ctfp', 'cwtfp', 'rgdpna',
    'rconna', 'rdana', 'rnna', 'rkna', 'rtfpna', 'rwtfpna',
    'labsh', 'irr', 'delta', 'xr', 'pl_con', 'pl_da', 'pl_gdpo',
    'i_cig', 'i_xm', 'i_xr', 'i_outlier', 'i_irr', 'cor_exp',
    'csh_c', 'csh_i', 'csh_g', 'csh_x', 'csh_m', 'csh_r',
    'pl_c', 'pl_i', 'pl_g', 'pl_x', 'pl_m', 'pl_n', 'pl_k',
]

RETRY_DELAYS = [60, 300, 600]


class PWTIngestor(BaseIngestor):

    def __init__(self):
        super().__init__(source_id='pwt')
        self.run_date    = date.today().isoformat()
        self._pwt_df     = None   # full DataFrame cached after first read
        self._since_date = None


    def get_batch_units(self, since_date: date) -> list:
        """
        Return one batch unit per PWT metric variable.
        47 batch units: ['rgdpe', 'rgdpo', 'pop', ...].
        Stores since_date for use in fetch().
        """
        self._since_date = since_date
        return PWT_METRIC_VARIABLES


    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        """
        Return rows for one PWT variable from the cached DataFrame.
        Reads the Stata file once on first call, caches it.
        All subsequent calls read from the cache — no re-reading.

        Args:
            batch_unit: PWT variable name e.g. 'rgdpe'.

        Returns:
            (raw_row_count, df) for this variable only.
        """
        # ── Read Stata file once, cache for all 47 metrics ────
        if self._pwt_df is None:
            last_error = None
            for attempt, delay in enumerate(RETRY_DELAYS, 1):
                try:
                    self._pwt_df = self._read_stata_file(since_date)
                    break
                except Exception as e:
                    last_error = e
                    if attempt < len(RETRY_DELAYS):
                        print(f"    [retry {attempt}] PWT: {e}. Waiting {delay}s...")
                        time.sleep(delay)
            else:
                raise RuntimeError(
                    f"PWT failed after {len(RETRY_DELAYS)} retries. "
                    f"Last error: {last_error}"
                )

        # ── Extract this metric's rows ─────────────────────────
        metric_id  = f"pwt.{batch_unit}"
        df_metric  = self._pwt_df[
            self._pwt_df['metric_id'] == metric_id
        ].copy().reset_index(drop=True)

        raw_row_count = len(df_metric)
        return raw_row_count, df_metric


    def _read_stata_file(self, since_date: date) -> pd.DataFrame:
        """
        Download .dta from B2, melt all 47 variables wide→tall,
        apply since_date filter, return full tall DataFrame.
        The full DataFrame is cached — not split by metric yet.
        """
        source_key = f"bronze/pwt/source_{self.run_date}.dta"
        dta_bytes  = self.b2.download(source_key)

        # Read Stata file — convert_categoricals=False returns
        # raw numeric values not Stata label strings.
        df_wide = pd.read_stata(
            BytesIO(dta_bytes),
            convert_categoricals=False,
        )

        # Only melt variables that exist in this file AND are
        # in our variable list — handles future PWT version changes.
        metric_cols = [
            col for col in df_wide.columns
            if col.lower() in set(PWT_METRIC_VARIABLES)
        ]

        if not metric_cols:
            raise ValueError("No recognised metric columns in PWT file.")

        # Melt wide → tall. countrycode and year identify each row.
        df_tall = df_wide.melt(
            id_vars    = ['countrycode', 'year'],
            value_vars = metric_cols,
            var_name   = 'variable',
            value_name = 'value',
        )

        # Drop NaN values — absence of data = no row.
        df_tall = df_tall[df_tall['value'].notna()].copy()

        # Apply since_date filter.
        df_tall = df_tall[
            df_tall['year'] >= since_date.year
        ].copy()

        # Build canonical shape with metric_id column.
        df_tall['metric_id']    = 'pwt.' + df_tall['variable'].str.lower()
        df_tall['country_iso3'] = df_tall['countrycode']
        df_tall['year']         = df_tall['year'].astype(int)
        df_tall['period']       = 'annual'
        df_tall['source_id']    = 'pwt'
        df_tall['retrieved_at'] = date.today().isoformat()
        df_tall['value']        = df_tall['value'].apply(
            lambda v: f"{float(v):.10g}" if pd.notna(v) else ''
        )
        df_tall = df_tall[df_tall['value'] != ''].copy()

        return df_tall[[
            'country_iso3', 'year', 'period', 'metric_id',
            'value', 'source_id', 'retrieved_at'
        ]].reset_index(drop=True)


    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        Attempt to read variable label from the .dta file on B2.
        Falls back to variable name if file is unavailable.
        """
        var_name = metric_id.replace('pwt.', '')
        try:
            source_key = f"bronze/pwt/source_{self.run_date}.dta"
            dta_bytes  = self.b2.download(source_key)
            reader     = pd.io.stata.StataReader(BytesIO(dta_bytes))
            labels     = reader.variable_labels()
            label      = labels.get(var_name, var_name)
        except Exception:
            label = var_name

        return {
            'metric_id':   metric_id,
            'metric_name': label,
            'source_id':   'pwt',
            'category':    'Productivity',
            'unit':        None,
            'description': label,
            'frequency':   'annual',
        }


    def get_b2_key(self, batch_unit: str) -> str:
        """
        bronze/pwt/{variable_name}_{date}.json
        e.g. bronze/pwt/rgdpe_2026-04-21.json
        """
        return f"bronze/pwt/{batch_unit}_{self.run_date}.json"


    def serialize(self, df: pd.DataFrame) -> bytes:
        return df.to_json(orient='records', date_format='iso').encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(BytesIO(data), orient='records')


if __name__ == "__main__":
    PWTIngestor().run()
