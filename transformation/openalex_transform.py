"""
ingestion/openalex_transform.py
================================
Transformation script for the OpenAlex AI Publications Dataset.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads raw OpenAlex JSON files from B2, maps ISO2 country
    codes to ISO3, validates and standardizes into the canonical
    silver layer shape, and upserts into standardized.observations.

KEY DIFFERENCE FROM WORLD BANK:
    The ingestion script stores ISO2 country codes (e.g. 'US',
    'LB') in country_iso3 column. The transformation script
    maps these to ISO3 using metadata.country_codes, loaded
    once as a cached dict and applied vectorized.

WHY VALUE IS AN INTEGER COUNT:
    OpenAlex returns publication counts as integers. Unlike
    World Bank or IMF which return floats, counts are always
    whole numbers. The _cast_value() method handles this by
    converting to int first to avoid '12450.0' style strings.

NO since_date FILTERING NEEDED HERE:
    Unlike IMF which returns full history in one call, OpenAlex
    ingestion already fetches only the years in the since_date
    window (get_batch_units() respects since_date). So every
    row in the B2 file is already within the correct window.
    No additional date filtering needed in transformation.

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


class OpenAlexTransformer(BaseTransformer):
    """
    Transformation script for OpenAlex AI Publications Dataset.
    Inherits all pipeline orchestration from BaseTransformer.
    Implements the four source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='openalex')
        self.run_date = None

        # ISO2 → ISO3 crosswalk cached after first load.
        # Loaded once per run, not once per batch.
        self._openalex_to_iso3 = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        Return the ordered list of years to transform.
        Reads completed ingestion checkpoints from ops.checkpoints.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'openalex'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed OpenAlex ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = date.today().isoformat()
        return sorted({row[0] for row in rows})


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate, map ISO2 → ISO3, enforce dtypes, cast values.

        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. Country code mapping — ISO2 → ISO3
        4. Null value drop
        5. Value casting — integer count → string

        Args:
            df_raw: DataFrame deserialized from B2 JSON.

        Returns:
            Standardized DataFrame ready for pre-upsert checks.
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"OpenAlex B2 file missing expected columns: "
                f"{missing_cols}"
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

        # ── Step 3: Country code mapping ───────────────────────
        # Map ISO2 → ISO3 using cached crosswalk dict.
        # Applied as a vectorized map() — one operation across
        # the full DataFrame, not a row-by-row lookup.
        openalex_to_iso3 = self._get_openalex_to_iso3()
        df['country_iso3'] = df['country_iso3'].map(openalex_to_iso3)

        # Drop rows where ISO2 code has no ISO3 mapping.
        # These are typically unknown or non-standard codes.
        unmapped = df['country_iso3'].isna().sum()
        if unmapped > 0:
            print(
                f"    [parse] Dropped {unmapped} rows with "
                f"unmappable OpenAlex ISO2 codes"
            )
        df = df[df['country_iso3'].notna()].copy()

        # ── Step 4: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna()].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(f"    [parse] Dropped {dropped} null value rows")

        # ── Step 5: Value casting ──────────────────────────────
        # OpenAlex counts are integers. Cast to int first to
        # avoid '12450.0' float string representation, then
        # to string for the silver layer value column.
        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        # ── Step 6: Reset index ────────────────────────────────
        df = df.reset_index(drop=True)

        return df


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key. Must match openalex_ingest.py.

        Naming convention:
            bronze/openalex/2020_2026-04-20.json
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return f"bronze/openalex/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize JSON bytes from B2. Inverse of serialize()."""
        return pd.read_json(BytesIO(data), orient='records', convert_dates=False, dtype=False)


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _get_openalex_to_iso3(self) -> dict:
        """
        Load the OpenAlex ISO2 → ISO3 crosswalk from
        metadata.country_codes. Cached after first call.

        WHY CACHED:
            Used in every batch's parse() call. Without caching
            it adds one DB query per year batch — unnecessary
            for static metadata that does not change during a run.

        Returns:
            Dict mapping ISO2 code → ISO3 string.
        """
        if self._openalex_to_iso3 is not None:
            return self._openalex_to_iso3

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code, iso3
                FROM metadata.country_codes
                WHERE source_id = 'openalex'
            """)).fetchall()

        self._openalex_to_iso3 = {r[0]: r[1] for r in rows}
        return self._openalex_to_iso3

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast publication count to string.

        WHY INT FIRST THEN STR:
            OpenAlex counts are integers stored as floats in
            pandas JSON deserialization (e.g. 12450.0). Casting
            to int first avoids '12450.0' in the silver layer
            which would be confusing for a count metric and
            could mislead researchers into thinking counts have
            decimal precision.

        Args:
            v: Raw value (int, float, or str).

        Returns:
            String integer representation, e.g. '12450'.
            Empty string if value cannot be represented.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            # Convert to int to remove decimal, then to string.
            return str(int(float(v)))
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = OpenAlexTransformer()
    transformer.run()
