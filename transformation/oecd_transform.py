"""
ingestion/oecd_transform.py
============================
Transformation script for OECD Main Science and Technology Indicators.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the raw OECD MSTI CSV from B2, validates columns,
    enforces dtypes, filters to the since_date window, casts
    values, and upserts into standardized.observations.

KEY DIFFERENCES FROM OTHER SOURCES:
    1. CSV format — deserialize uses pd.read_csv not pd.read_json
    2. OECD uses ISO3 directly — no country code mapping needed
    3. metric_id mapping already done at ingestion via
       MEASURE_TO_METRIC — transformation just validates
    4. since_date filtering applied here because ingestion
       already used startPeriod at the API level, so rows in
       the B2 file are already within the window. The filter
       here is a defensive double-check.
    5. BIANNUAL frequency — OECD publishes twice a year.
       All observations use period='annual' since data points
       represent annual aggregates, not half-year periods.

WHY NO COUNTRY CODE MAPPING:
    OECD uses ISO3 country codes natively (e.g. 'LBN', 'USA').
    The ingestion script stores them directly as country_iso3.
    No lookup against metadata.country_codes needed.
    The pre-upsert fk_validity check validates them against
    metadata.countries as usual.

WHY SINCE_DATE FILTERING IS DEFENSIVE HERE:
    The ingestion script already applied startPeriod at the
    API level. Every row in the B2 file should already be
    within the since_date window. The filter here guards
    against any edge case where the API returned data outside
    the requested range — which can happen with OECD's
    preliminary estimate rows.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import io
import pandas as pd
from datetime import date
from sqlalchemy import text

from database.base_transformer import BaseTransformer

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

EXPECTED_COLUMNS = {
    'country_iso3', 'year', 'period', 'metric_id',
    'value', 'source_id', 'retrieved_at'
}

EXPECTED_DTYPES = {
    'country_iso3': str,
    'year':         'Int64',
    'period':       str,
    'metric_id':    str,
    'source_id':    str,
    'retrieved_at': str,
}

# Valid metric_ids for OECD — used for defensive validation.
VALID_OECD_METRIC_IDS = {
    'oecd.berd_gdp',
    'oecd.goverd_gdp',
    'oecd.researchers_per_thousand',
}


class OECDTransformer(BaseTransformer):
    """
    Transformation script for OECD MSTI.
    Inherits all pipeline orchestration from BaseTransformer.
    Implements the four source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='oecd_msti')
        self.run_date = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        Return ['full_file'] — OECD is always one batch unit.
        Reads run_date from the most recent ingestion checkpoint.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'oecd_msti'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
                LIMIT 1
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed OECD ingestion checkpoint found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = rows[0][1].date().isoformat()
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate, enforce dtypes, apply defensive date filter,
        and cast values.

        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. Defensive since_date filter
        4. Metric_id validation
        5. Null value drop
        6. Value casting

        Args:
            df_raw: DataFrame deserialized from B2 CSV.

        Returns:
            Standardized DataFrame ready for pre-upsert checks.
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"OECD B2 file missing expected columns: {missing_cols}"
            )

        df = df_raw.copy()

        # ── Step 2: Dtype enforcement ──────────────────────────
        for col, dtype in EXPECTED_DTYPES.items():
            try:
                df[col] = df[col].astype(
                    'Int64' if dtype == 'Int64' else dtype
                )
            except Exception as e:
                raise ValueError(
                    f"Dtype enforcement failed for '{col}': {e}"
                )

        # ── Step 3: Defensive since_date filter ───────────────
        # Ingestion already applied startPeriod at the API level.
        # This filter guards against OECD returning preliminary
        # rows outside the requested range.
        since_year = self._get_since_year()
        before = len(df)
        df = df[df['year'] >= since_year].copy()
        filtered = before - len(df)
        if filtered > 0:
            print(
                f"    [parse] Defensive filter removed {filtered} "
                f"rows outside since_year={since_year}"
            )

        # ── Step 4: Metric_id validation ───────────────────────
        # All metric_ids should be in VALID_OECD_METRIC_IDS —
        # the ingestion script only maps known measure codes.
        # This is a defensive check in case an unknown code
        # slipped through.
        unknown_metrics = set(df['metric_id'].unique()) - VALID_OECD_METRIC_IDS
        if unknown_metrics:
            print(
                f"    [parse] Dropping rows with unknown metric_ids: "
                f"{unknown_metrics}"
            )
            df = df[df['metric_id'].isin(VALID_OECD_METRIC_IDS)].copy()

        # ── Step 5: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna() & (df['value'].astype(str) != '')].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f"    [parse] Dropped {dropped} null value rows")

        # ── Step 6: Value casting ──────────────────────────────
        # OECD values are percentages and ratios — use :.10g
        # same as World Bank and IMF to preserve precision
        # without scientific notation.
        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        # ── Step 7: Reset index ────────────────────────────────
        df = df.reset_index(drop=True)

        return df


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key. Must match oecd_ingest.py.

        Naming convention:
            bronze/oecd_msti/full_file_2026-04-20.csv
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return f"bronze/oecd_msti/{batch_unit}_{self.run_date}.csv"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize CSV bytes from B2 into a DataFrame.
        Reads all columns as strings — same as ingestion to
        ensure consistent dtype handling before enforcement.
        """
        return pd.read_csv(
            io.BytesIO(data),
            dtype=str,
            low_memory=False,
        )


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _get_since_year(self) -> int:
        """
        Read last_retrieved for OECD from metadata.sources
        and return since_year (last_retrieved.year - 5).
        Returns 1981 (OECD MSTI start year) on first ever run.
        """
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_retrieved
                FROM metadata.sources
                WHERE source_id = 'oecd_msti'
            """)).fetchone()

        last = row[0] if row else None
        if last is None:
            return 1981  # OECD MSTI historical start year
        return last.year - 5

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast OECD value to controlled string representation.
        Uses :.10g — same as World Bank and IMF.
        OECD values are percentages (e.g. 1.23) and ratios
        (e.g. 8.4). Never integers so no int cast needed.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return f"{float(v):.10g}"
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = OECDTransformer()
    transformer.run()
