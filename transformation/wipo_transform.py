"""
ingestion/wipo_transform.py
============================
Transformation script for WIPO IP Statistics AI Patents.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the parsed WIPO CSV from B2, maps ISO2 country codes
    to ISO3 using metadata.country_codes, validates and casts
    values, and upserts into standardized.observations.

KEY DIFFERENCE FROM OTHER SOURCES:
    WIPO ingestion stores ISO2 codes in country_iso3 column.
    Transformation maps ISO2 → ISO3 via metadata.country_codes
    where source_id='wipo_ip'. Same pattern as IMF and OpenAlex.
    Crosswalk loaded once as cached dict, applied vectorized.

WHY since_date FILTERING IS NOT NEEDED HERE:
    WIPO ingestion already applies the since_date year filter
    before saving to B2. Every row in the B2 file is already
    within the revision window. No additional filtering needed.

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


class WIPOTransformer(BaseTransformer):
    """
    Transformation script for WIPO IP Statistics.
    Inherits all pipeline orchestration from BaseTransformer.
    """

    def __init__(self):
        super().__init__(source_id='wipo_ip')
        self.run_date = None
        self._wipo_to_iso3 = None  # cached crosswalk


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        WIPO is a single batch unit. Returns ['full_file'].
        Reads run_date from the most recent ingestion checkpoint.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'wipo_ip'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
                LIMIT 1
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed WIPO ingestion checkpoint found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = rows[0][1].date().isoformat()
        return ['full_file']


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate schema, map ISO2 → ISO3, enforce dtypes,
        cast patent counts to string.

        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. ISO2 → ISO3 mapping (vectorized)
        4. Drop unmapped rows
        5. Null value drop
        6. Value casting — int count → string
        7. Reset index
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"WIPO B2 file missing expected columns: {missing_cols}"
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

        # ── Step 3: ISO2 → ISO3 mapping ────────────────────────
        # country_iso3 column currently holds ISO2 codes from WIPO.
        # Map to ISO3 using the pre-seeded crosswalk.
        # Loaded once per run as a cached dict, applied vectorized.
        wipo_to_iso3 = self._get_wipo_to_iso3()
        df['country_iso3'] = df['country_iso3'].map(wipo_to_iso3)

        # ── Step 4: Drop unmapped rows ─────────────────────────
        unmapped = df['country_iso3'].isna().sum()
        if unmapped > 0:
            print(
                f"    [parse] Dropped {unmapped} rows with "
                f"unmappable WIPO ISO2 codes"
            )
        df = df[df['country_iso3'].notna()].copy()

        # ── Step 5: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna()].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null value rows")

        # ── Step 6: Value casting ──────────────────────────────
        # WIPO counts are integers. Cast to int then string
        # to avoid '2.0' style float strings.
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
        Return B2 key. Must match wipo_ingest.py exactly.

        Naming convention:
            bronze/wipo_ip/full_file_2026-04-20.csv
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return f"bronze/wipo_ip/{batch_unit}_{self.run_date}.csv"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize CSV bytes from B2 into DataFrame."""
        return pd.read_csv(io.BytesIO(data), dtype=str)


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    def _get_wipo_to_iso3(self) -> dict:
        """
        Load WIPO ISO2 → ISO3 crosswalk from metadata.country_codes.
        Cached after first call — one DB query per run.
        """
        if self._wipo_to_iso3 is not None:
            return self._wipo_to_iso3

        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code, iso3
                FROM metadata.country_codes
                WHERE source_id = 'wipo_ip'
            """)).fetchall()

        self._wipo_to_iso3 = {r[0]: r[1] for r in rows}
        return self._wipo_to_iso3

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast WIPO patent count to string integer.
        Converts '2.0' → '2', preserving integer semantics.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return str(int(float(v)))
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = WIPOTransformer()
    transformer.run()
