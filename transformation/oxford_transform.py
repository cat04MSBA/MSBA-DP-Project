"""
ingestion/oxford_transform.py
==============================
Transformation script for Oxford Insights GAIRI.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads the parsed Oxford JSON files from B2 (written by
    oxford_ingest.py), validates schema and dtypes, casts
    scores to controlled string representation, and upserts
    into standardized.observations.

WHY TRANSFORMATION IS SIMPLE HERE:
    oxford_ingest.py already does the heavy work:
    - Country name → ISO3 mapping (via metadata.country_codes)
    - Score column detection across inconsistent editions
    - Non-numeric score filtering
    The parsed JSON on B2 is already in canonical shape.
    Transformation validates it and casts values — nothing more.

NOTE ON TWO B2 FILES PER YEAR:
    For each Oxford year there are two B2 files:
    - oxford_{year}_{date}.xlsx → original XLSX (faithful raw copy)
    - oxford_{year}_{date}_parsed.json → parsed DataFrame (canonical)
    The transformation script reads the parsed JSON, not the XLSX.
    This keeps transformation consistent with all other sources
    which read JSON from B2.

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


class OxfordTransformer(BaseTransformer):
    """
    Transformation script for Oxford Insights GAIRI.
    Inherits all pipeline orchestration from BaseTransformer.
    """

    def __init__(self):
        super().__init__(source_id='oxford')
        self.run_date = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        Return the list of years to transform.
        Reads completed ingestion checkpoints — processes exactly
        the years ingestion successfully completed.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'oxford'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed Oxford ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        # Extract run_date from the most recent checkpoint's
        # checkpointed_at timestamp — the date ingestion actually
        # ran and embedded in all B2 file keys.
        # WHY NOT date.today(): see world_bank_transform.py comment.
        self.run_date = rows[0][1].date().isoformat()
        return sorted({row[0] for row in rows})


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate schema, enforce dtypes, cast score values.

        Oxford scores are floats (e.g. 85.48). The silver layer
        stores TEXT. We use :.6g format — 6 significant figures
        is sufficient for a 0-100 index score and avoids
        unnecessary trailing zeros (85.48 not 85.480000).

        Steps:
        1. Schema validation
        2. Dtype enforcement
        3. Null value drop
        4. Value casting
        5. Reset index
        """
        # ── Step 1: Schema validation ──────────────────────────
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"Oxford B2 file missing expected columns: {missing_cols}"
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

        # ── Step 3: Null value drop ────────────────────────────
        before = len(df)
        df = df[df['value'].notna()].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null rows")

        # ── Step 4: Value casting ──────────────────────────────
        # Oxford scores are 0-100 floats. :.6g gives up to 6
        # significant figures without trailing zeros or scientific
        # notation. 85.48 → '85.48', 100.0 → '100'.
        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        # ── Step 5: Reset index ────────────────────────────────
        df = df.reset_index(drop=True)

        return df


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 key for the parsed JSON file.
        Must match oxford_ingest.py get_b2_key_parsed() exactly.

        Naming convention:
            bronze/oxford/oxford_2025_2026-04-20_parsed.json
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() first."
            )
        return (
            f"bronze/oxford/oxford_{batch_unit}"
            f"_{self.run_date}_parsed.json"
        )


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize JSON bytes from B2 into DataFrame."""
        return pd.read_json(BytesIO(data), orient='records', convert_dates=False, dtype=False)


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast Oxford score to controlled string representation.

        WHY :.6g (not :.10g like other sources):
            Oxford scores are 0-100 index values reported to
            2 decimal places at most. 6 significant figures
            is more than sufficient and avoids unnecessary
            precision (85.4800000 → '85.48').
            World Bank and IMF use :.10g because economic
            values can have more significant digits.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return f"{float(v):.6g}"
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    transformer = OxfordTransformer()
    transformer.run()
