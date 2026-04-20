"""
ingestion/imf_transform.py
==========================
Transformation script for the IMF DataMapper API.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads raw IMF JSON files from B2, maps IMF country codes to
    ISO3, filters by since_date, validates and standardizes into
    the canonical silver layer shape, and upserts into
    standardized.observations.

KEY DIFFERENCE FROM WORLD BANK TRANSFORMATION:
    The ingestion script stores the raw IMF country code in the
    country_iso3 column (e.g. 'US', 'LBN', 'WEO_AFG'). The
    transformation script must map these to ISO3 using
    metadata.country_codes. This lookup is loaded once as a
    dict at the start of parse() and applied in one vectorized
    pandas operation — no row-by-row lookup.

WHY COUNTRY CODE MAPPING LIVES IN TRANSFORMATION NOT INGESTION:
    Ingestion is responsible for faithful raw storage.
    The IMF country code IS the raw value from the source.
    Mapping to ISO3 is a standardization step — it belongs in
    transformation. This also means the B2 file always reflects
    exactly what the IMF returned, making it auditable.

WHY since_date FILTERING HAPPENS HERE NOT IN INGESTION:
    The IMF API returns full history regardless of date filters.
    Ingestion stores the full raw response on B2 — complete and
    auditable. Transformation applies the since_date filter so
    only rows within the 5-year revision window are upserted.
    Rows outside the window are not rejected — they simply are
    not upserted this run. The full history remains on B2.

COUNTRY CODE MAPPING STRATEGY:
    Load the full IMF → ISO3 crosswalk from metadata.country_codes
    as a Python dict once at parse() start. Apply as a vectorized
    pandas map() operation. Rows with unmappable IMF codes are
    dropped and counted — they will be caught by fk_validity check
    in check_pre_upsert() anyway, but dropping early avoids
    unnecessary downstream processing.

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


class IMFTransformer(BaseTransformer):
    """
    Transformation script for IMF DataMapper.
    Inherits all pipeline orchestration from BaseTransformer.
    Implements the four source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='imf')
        self.run_date = None

        # IMF → ISO3 crosswalk loaded once at first parse() call.
        # Stored as instance variable so it is loaded once per run
        # not once per batch — avoids repeated DB queries.
        self._imf_to_iso3 = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        Return the ordered list of batch_units to transform.
        Reads completed ingestion checkpoints from ops.checkpoints.
        Also extracts run_date from the most recent checkpoint.

        WHY READ FROM CHECKPOINTS:
            Guarantees transformation processes exactly the batch
            units ingestion successfully completed — no more,
            no less.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'imf'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed IMF ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = rows[0][1].date().isoformat()
        return sorted({row[0] for row in rows})


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate, map country codes, filter by date, enforce
        dtypes, and cast values.

        Five steps:
        1. Schema validation
        2. Dtype enforcement
        3. Country code mapping — IMF code → ISO3
        4. since_date filtering
        5. Value casting

        Args:
            df_raw: DataFrame deserialized from B2 JSON.

        Returns:
            Standardized DataFrame ready for pre-upsert checks.
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"IMF B2 file missing expected columns: {missing_cols}"
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
                    f"Dtype enforcement failed for column '{col}': {e}"
                )

        # ── Step 3: Country code mapping ───────────────────────
        # Load IMF → ISO3 crosswalk once per run (cached in
        # self._imf_to_iso3 after first call). Apply as a
        # vectorized map() — one operation across the entire
        # DataFrame, not a row-by-row lookup.
        imf_to_iso3 = self._get_imf_to_iso3()
        df['country_iso3'] = df['country_iso3'].map(imf_to_iso3)

        # Count and log unmappable codes before dropping.
        # These are IMF aggregate codes (e.g. 'WORLD', 'ADVEC')
        # that have no ISO3 equivalent. They are not errors —
        # they are expected and documented. Dropping them here
        # is more efficient than letting fk_validity reject them
        # row by row downstream.
        unmapped = df['country_iso3'].isna().sum()
        if unmapped > 0:
            print(
                f"    [parse] Dropped {unmapped} rows with "
                f"unmappable IMF country codes (aggregates expected)"
            )
        df = df[df['country_iso3'].notna()].copy()

        # ── Step 4: since_date filtering ───────────────────────
        # Apply the 5-year revision window. Rows outside the
        # window are not rejected — just not upserted this run.
        # Full history remains on B2.
        since_year = self._get_since_year()
        df = df[df['year'] >= since_year].copy()

        # ── Step 5: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna() & (df['value'] != 'None')].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f"    [parse] Dropped {dropped} null value rows")

        # ── Step 6: Value casting ──────────────────────────────
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
        Return the B2 object key for a given batch_unit.
        Must match imf_ingest.py exactly.

        Naming convention:
            bronze/imf/NGDP_RPCH_2026-04-20.json
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return f"bronze/imf/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize JSON bytes from B2 into a DataFrame.
        Exact inverse of imf_ingest.py serialize().
        """
        return pd.read_json(BytesIO(data), orient='records')


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _get_imf_to_iso3(self) -> dict:
        """
        Load the IMF country code → ISO3 crosswalk from
        metadata.country_codes. Cached after first call so
        the DB is queried only once per transformation run
        regardless of how many batches are processed.

        WHY CACHED AS INSTANCE VARIABLE:
            This dict is used in every batch's parse() call.
            Without caching it would add one DB query per batch.
            For a run with 100+ IMF indicators that is 100+
            identical queries for the same static data.

        Returns:
            Dict mapping IMF code → ISO3 string.
        """
        if self._imf_to_iso3 is not None:
            return self._imf_to_iso3

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code, iso3
                FROM metadata.country_codes
                WHERE source_id = 'imf'
            """)).fetchall()

        self._imf_to_iso3 = {r[0]: r[1] for r in rows}
        return self._imf_to_iso3

    def _get_since_year(self) -> int:
        """
        Read last_retrieved from metadata.sources for IMF and
        return the since_year (last_retrieved year - 5).
        Used to filter rows in parse() to the 5-year window.

        Returns:
            Integer year. Returns 1950 on first ever run.
        """
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_retrieved
                FROM metadata.sources
                WHERE source_id = 'imf'
            """)).fetchone()

        last = row[0] if row else None
        if last is None:
            return 1950
        return last.year - 5

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast a value to controlled string representation.
        Same :.10g approach as World Bank — preserves source
        precision without scientific notation for normal values.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return f"{float(v):.10g}"
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = IMFTransformer()
    transformer.run()
