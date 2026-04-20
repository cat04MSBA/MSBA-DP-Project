"""
ingestion/pwt_transform.py
==========================
Transformation script for Penn World Tables 11.0.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the parsed PWT JSON from B2, validates schema and dtypes,
    applies defensive since_date filtering, casts values, and
    upserts into standardized.observations.

WHY TRANSFORMATION IS SIMPLE HERE:
    pwt_ingest.py already does the heavy lifting:
    - Wide → tall melt of 47 metric variables
    - NaN dropping
    - metric_id construction (pwt.{variable})
    - countrycode used directly as country_iso3 (already ISO3)
    - since_date filtering applied at ingestion time
    The parsed JSON on B2 is already in canonical shape.
    Transformation validates, type-checks, and casts — nothing more.

WHY NO COUNTRY CODE MAPPING:
    PWT uses ISO3 codes (countrycode column) natively.
    The ingestion script stores them directly as country_iso3.
    No crosswalk lookup needed. The pre-upsert fk_validity check
    validates them against metadata.countries as usual.

SCALE NOTE:
    PWT is the largest source — up to ~640,000 rows before NaN
    dropping, ~200,000-400,000 after. The temp table upsert
    in 500-row chunks (from base_transformer.py) handles this
    correctly. Expect transformation to take several minutes.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import pandas as pd
from datetime import date
from io import BytesIO
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

# Valid metric_id prefix for PWT — defensive check.
PWT_METRIC_PREFIX = 'pwt.'


class PWTTransformer(BaseTransformer):
    """
    Transformation script for Penn World Tables 11.0.
    Inherits all pipeline orchestration from BaseTransformer.
    """

    def __init__(self):
        super().__init__(source_id='pwt')
        self.run_date = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        PWT is a single batch unit. Returns ['full_file'].
        Reads run_date from the most recent ingestion checkpoint.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'pwt'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
                LIMIT 1
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed PWT ingestion checkpoint found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = rows[0][1].date().isoformat()
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate schema, enforce dtypes, apply defensive
        since_date filter, cast values.

        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. Defensive metric_id prefix validation
        4. Defensive since_date filter
        5. Null value drop
        6. Value casting
        7. Reset index

        WHY DEFENSIVE FILTERS:
            Ingestion already applied since_date filtering and
            NaN dropping. These defensive checks guard against
            edge cases where the ingestion script produced an
            unexpected output — caught here with clear messages
            rather than failing silently during the upsert.
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"PWT B2 file missing expected columns: {missing_cols}"
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

        # ── Step 3: Metric_id prefix validation ────────────────
        # All PWT metric_ids should start with 'pwt.'.
        # Flag any that do not — indicates a parsing error in
        # the ingestion script or a corrupted B2 file.
        invalid_metrics = df[
            ~df['metric_id'].str.startswith(PWT_METRIC_PREFIX)
        ]
        if len(invalid_metrics) > 0:
            invalid_ids = invalid_metrics['metric_id'].unique().tolist()
            print(
                f"    [parse] Warning: {len(invalid_metrics)} rows "
                f"have non-PWT metric_ids: {invalid_ids[:5]}. "
                f"These will fail fk_validity check."
            )

        # ── Step 4: Defensive since_date filter ────────────────
        # Ingestion already applied this filter. This is a
        # defensive double-check in case of edge cases.
        since_year = self._get_since_year()
        before = len(df)
        df = df[df['year'] >= since_year].copy()
        filtered = before - len(df)
        if filtered > 0:
            print(
                f"    [parse] Defensive filter removed {filtered} "
                f"rows outside since_year={since_year}"
            )

        # ── Step 5: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna() & (df['value'] != '')].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null rows")

        # ── Step 6: Value casting ──────────────────────────────
        # PWT values are floats (GDP, TFP, shares, ratios).
        # :.10g: up to 10 significant figures without scientific
        # notation. Same as World Bank and IMF.
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
        Return B2 key for parsed JSON. Must match pwt_ingest.py.

        Naming convention:
            bronze/pwt/full_file_2026-04-20.json
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return f"bronze/pwt/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize JSON bytes from B2 into DataFrame."""
        return pd.read_json(BytesIO(data), orient='records')


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _get_since_year(self) -> int:
        """
        Read last_retrieved for PWT from metadata.sources
        and return since_year (last_retrieved.year - 5).
        Returns 1950 (PWT start year) on first ever run.
        """
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_retrieved
                FROM metadata.sources
                WHERE source_id = 'pwt'
            """)).fetchone()

        last = row[0] if row else None
        if last is None:
            return 1950  # PWT historical start year
        return last.year - 5

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast PWT value to controlled string representation.
        PWT values are floats — use :.10g same as World Bank/IMF.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return f"{float(v):.10g}"
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = PWTTransformer()
    transformer.run()
