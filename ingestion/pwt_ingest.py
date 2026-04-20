"""
ingestion/pwt_ingest.py
=======================
Ingestion script for Penn World Tables 11.0.

INHERITS FROM: BaseIngestor
IMPLEMENTS: fetch(), fetch_metric_metadata(), get_batch_units(),
            get_b2_key(), serialize(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the PWT 11.0 Stata .dta file from B2 (uploaded manually
    via upload_to_b2.py), melts all 47 metric variables from wide
    format to tall format, and produces a standardized DataFrame
    for the quality check pipeline.

MANUAL SOURCE WORKFLOW:
    1. Team member downloads pwt110.dta from:
       https://www.rug.nl/ggdc/productivity/pwt/
    2. Runs: python3 database/upload_to_b2.py --source pwt
             --file pwt110.dta
    3. upload_to_b2.py uploads to bronze/pwt/ and triggers
       this subflow

PWT FILE STRUCTURE (confirmed from actual pwt110.dta):
    - 13,690 rows: 185 countries × 74 years (1950-2023)
    - 51 columns: countrycode, country, currency_unit, year,
      then 47 metric variables (rgdpe, rgdpo, pop, etc.)
    - countrycode = ISO3 directly — no crosswalk needed
    - country = country name (used for PWT crosswalk in
      metadata.country_codes, not for standardization)
    - All metric columns are float64 with NaN for missing values

WHY WIDE → TALL TRANSFORMATION AT INGESTION:
    PWT is wide format — one row per country-year, one column
    per metric. The silver layer requires tall format — one row
    per country-year-metric. pd.melt() converts efficiently.
    The melted DataFrame is what gets saved to B2 as JSON.

WHY NaN ROWS ARE DROPPED AT INGESTION:
    PWT has many NaN values — not all metrics are available for
    all countries and years. Absence of a row = absence of data.
    Storing NaN would add millions of meaningless rows.

WHY countrycode IS USED DIRECTLY (no crosswalk):
    PWT uses ISO3 codes (e.g. 'LBN', 'USA') in the countrycode
    column. No mapping needed — countrycode goes directly into
    country_iso3. This was confirmed by inspecting the actual
    pwt110.dta file.

SCALE:
    Raw file: 13,690 rows × 47 metrics = up to 643,430 observations.
    After dropping NaN: significantly fewer (PWT is sparse).
    This is the largest source in the pipeline by row count.

B2 FILE NAMING:
    bronze/pwt/full_file_{retrieved_date}.json

RETRY LOGIC:
    3 retries with 1/5/10 minute backoff for B2 read failures.
    Stata file parsing failures halt immediately — if corrupt,
    retrying reads the same corrupt file.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import time
import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_ingestor import BaseIngestor

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Columns that identify a row — not metric variables.
# These are excluded from the melt operation.
PWT_ID_COLUMNS = ['countrycode', 'country', 'currency_unit', 'year']

# Metric variables to ingest — matches PWT_VARIABLE_LABELS
# in seed_metrics.py exactly. Hardcoded to avoid ingesting
# identifier columns (countrycode, country, etc.) as metrics.
PWT_METRIC_VARIABLES = {
    'rgdpe', 'rgdpo', 'pop', 'emp', 'avh', 'hc', 'ccon', 'cda',
    'cgdpe', 'cgdpo', 'cn', 'ck', 'ctfp', 'cwtfp', 'rgdpna',
    'rconna', 'rdana', 'rnna', 'rkna', 'rtfpna', 'rwtfpna',
    'labsh', 'irr', 'delta', 'xr', 'pl_con', 'pl_da', 'pl_gdpo',
    'i_cig', 'i_xm', 'i_xr', 'i_outlier', 'i_irr', 'cor_exp',
    'csh_c', 'csh_i', 'csh_g', 'csh_x', 'csh_m', 'csh_r',
    'pl_c', 'pl_i', 'pl_g', 'pl_x', 'pl_m', 'pl_n', 'pl_k',
}

# Retry backoff delays (design document Section 9).
RETRY_DELAYS = [60, 300, 600]


class PWTIngestor(BaseIngestor):
    """
    Ingestion script for Penn World Tables 11.0.
    Inherits all pipeline orchestration from BaseIngestor.
    """

    def __init__(self):
        super().__init__(source_id='pwt')
        self.run_date = date.today().isoformat()


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self, since_date: date) -> list:
        """
        PWT is a single full-file source. Always returns
        ['full_file'] — one batch unit, one B2 file, one
        checkpoint per run.

        WHY SINGLE BATCH UNIT:
            PWT releases one complete .dta file covering all
            185 countries and all years (1950-2023). There is
            no meaningful sub-unit. The entire file is processed
            in one pass.

        WHY since_date IS NOT USED AT BATCH UNIT LEVEL:
            since_date filtering is applied at ingestion time
            after melting — only rows within the revision window
            are saved to B2. The batch unit is always 'full_file'
            regardless of since_date.
        """
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # fetch
    # ═══════════════════════════════════════════════════════

    def fetch(self, batch_unit: str,
              since_date: date) -> tuple:
        """
        Read PWT .dta from B2, melt wide → tall, filter by
        since_date, return standardized DataFrame.

        Args:
            batch_unit: Always 'full_file'.
            since_date: Start of the 5-year revision window.

        Returns:
            (raw_row_count, df)
        """
        last_error = None

        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._read_pwt_file(since_date)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(
                        f"    [retry {attempt}] PWT: {e}. "
                        f"Waiting {delay}s..."
                    )
                    time.sleep(delay)

        raise RuntimeError(
            f"PWT failed after {len(RETRY_DELAYS)} retries. "
            f"Last error: {last_error}"
        )

    def _read_pwt_file(self, since_date: date) -> tuple:
        """
        Download PWT .dta from B2, melt to tall format,
        apply since_date filter, return canonical DataFrame.

        WHY pd.read_stata WITH convert_categoricals=False:
            PWT uses Stata categorical labels for some columns.
            convert_categoricals=False returns raw numeric values
            instead of Stata label strings, which is what we want
            for the metric variables. The countrycode column is
            a plain string and unaffected.

        WHY MELT IS DONE BEFORE FILTERING:
            Melting first then filtering is more efficient than
            filtering first then melting. After melt, a simple
            year >= since_year comparison on one column removes
            all out-of-window rows in one vectorized operation.

        Args:
            since_date: Filter rows from since_date.year.

        Returns:
            (raw_row_count, df)
        """
        b2_key   = self.get_b2_key('full_file')
        dta_bytes = self.b2.download(b2_key)

        # ── Read Stata file ────────────────────────────────────
        # convert_categoricals=False: return raw values not labels.
        # PWT .dta is ~20MB — fully loaded into memory.
        # At 185 countries × 74 years = 13,690 rows this is fine.
        df_wide = pd.read_stata(
            BytesIO(dta_bytes),
            convert_categoricals = False,
        )

        # ── Identify metric columns present in this file ───────
        # Intersection of PWT_METRIC_VARIABLES and actual columns.
        # Protects against PWT adding or removing variables in
        # future releases without breaking the pipeline.
        metric_cols = [
            col for col in df_wide.columns
            if col.lower() in PWT_METRIC_VARIABLES
        ]

        if not metric_cols:
            raise ValueError(
                "No recognised metric columns found in PWT file. "
                "File format may have changed."
            )

        # ── Melt wide → tall ───────────────────────────────────
        # id_vars: countrycode and year identify each row.
        # country and currency_unit are excluded — not needed
        # for standardization (countrycode is already ISO3).
        # value_vars: the 47 metric variables.
        df_tall = df_wide.melt(
            id_vars    = ['countrycode', 'year'],
            value_vars = metric_cols,
            var_name   = 'variable',
            value_name = 'value',
        )

        # ── Drop NaN values ────────────────────────────────────
        # PWT is sparse — many country-year-metric combinations
        # have no data. Dropping NaN before counting raw_row_count
        # because NaN rows are never intended to enter the pipeline.
        df_tall = df_tall[df_tall['value'].notna()].copy()

        # raw_row_count = non-null (country, year, metric) triples.
        # This is what check_ingestion_pre() compares against len(df).
        raw_row_count = len(df_tall)

        # ── Apply since_date filter ────────────────────────────
        since_year = since_date.year
        df_tall = df_tall[
            df_tall['year'] >= since_year
        ].copy()

        # ── Build canonical DataFrame ──────────────────────────
        df = self._parse_pwt(df_tall)

        return raw_row_count, df

    def _parse_pwt(self, df_tall: pd.DataFrame) -> pd.DataFrame:
        """
        Convert melted DataFrame to canonical silver layer shape.

        metric_id convention: pwt.{variable_name}
        Examples: rgdpe → pwt.rgdpe, rtfpna → pwt.rtfpna

        WHY countrycode IS USED DIRECTLY AS country_iso3:
            PWT uses ISO3 codes (confirmed from actual pwt110.dta:
            'LBN', 'USA', 'FRA'). No mapping needed. Direct use
            avoids an unnecessary join and keeps transformation fast.

        WHY VALUES ARE CAST TO str(float) NOT str(int):
            PWT values are floats (GDP in millions, TFP indices,
            shares, etc.). Unlike WIPO counts, these are genuinely
            decimal. We use :.10g same as World Bank and IMF.

        Args:
            df_tall: Melted DataFrame with countrycode, year,
                     variable, value columns.

        Returns:
            Canonical DataFrame.
        """
        # Vectorized operations are faster than row-by-row loops
        # for a DataFrame that can have hundreds of thousands of rows.
        df = df_tall.copy()

        # Construct metric_id: pwt.{variable_name}
        df['metric_id'] = 'pwt.' + df['variable'].str.lower()

        # country_iso3 directly from countrycode (already ISO3).
        df = df.rename(columns={'countrycode': 'country_iso3'})

        # Cast year to int (may be float after Stata read).
        df['year'] = df['year'].astype(int)

        # Cast value to controlled string representation.
        # :.10g: up to 10 significant figures without scientific
        # notation for normal economic values.
        df['value'] = df['value'].apply(
            lambda v: f"{float(v):.10g}"
            if pd.notna(v) else ''
        )

        # Drop any empty values produced by the cast.
        df = df[df['value'] != ''].copy()

        # Add remaining required columns.
        df['period']       = 'annual'
        df['source_id']    = 'pwt'
        df['retrieved_at'] = date.today().isoformat()

        # Return only canonical columns in the correct order.
        return df[[
            'country_iso3', 'year', 'period', 'metric_id',
            'value', 'source_id', 'retrieved_at'
        ]].reset_index(drop=True)


    # ═══════════════════════════════════════════════════════
    # fetch_metric_metadata
    # ═══════════════════════════════════════════════════════

    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        Fetch metadata for an unknown PWT variable.
        PWT variables are hardcoded in seed_metrics.py.
        This method handles the rare case of a new variable
        appearing in a future PWT release.

        PWT embeds variable labels in the .dta file — we
        read the label directly from the Stata file on B2.
        """
        # Reverse metric_id to variable name: pwt.rgdpe → rgdpe
        var_name = metric_id.replace('pwt.', '')

        # Attempt to read label from the .dta file on B2.
        try:
            b2_key    = self.get_b2_key('full_file')
            dta_bytes = self.b2.download(b2_key)
            reader    = pd.io.stata.StataReader(BytesIO(dta_bytes))
            labels    = reader.variable_labels()
            label     = labels.get(var_name, var_name)
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


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 key for the PWT file.

        WHY JSON NOT .dta:
            The ingestion script saves the melted tall-format
            DataFrame as JSON — not the original .dta. The
            original .dta is uploaded by upload_to_b2.py to
            a separate path (bronze/pwt/pwt110.dta). The
            ingestion script reads the .dta, melts it, and
            saves the result as JSON for the transformation
            script to consume. Two files on B2:
            - bronze/pwt/pwt110.dta → original file (faithful)
            - bronze/pwt/full_file_{date}.json → parsed result

        Naming convention:
            bronze/pwt/full_file_2026-04-20.json
        """
        return f"bronze/pwt/{batch_unit}_{self.run_date}.json"

    def get_b2_key_source(self) -> str:
        """
        Return the B2 key where upload_to_b2.py stored the
        original .dta file. Used by _read_pwt_file() to
        download the source file before processing.
        """
        return "bronze/pwt/pwt110.dta"


    # ═══════════════════════════════════════════════════════
    # serialize / deserialize
    # ═══════════════════════════════════════════════════════

    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize the parsed tall DataFrame to JSON bytes.
        Saves the canonical shape, not the original .dta.
        """
        return df.to_json(
            orient='records',
            date_format='iso',
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize JSON bytes from B2 into DataFrame."""
        return pd.read_json(BytesIO(data), orient='records')


if __name__ == "__main__":
    ingestor = PWTIngestor()
    ingestor.run()
