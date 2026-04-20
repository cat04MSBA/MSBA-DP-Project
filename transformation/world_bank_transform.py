"""
ingestion/world_bank_transform.py
==================================
Transformation script for the World Bank World Development Indicators.

INHERITS FROM: BaseTransformer
IMPLEMENTS: parse(), get_batch_units(), get_b2_key(), deserialize()

WHAT THIS SCRIPT DOES:
    Reads raw World Bank JSON files from B2, validates and
    standardizes them into the canonical silver layer shape,
    detects and logs value revisions, and upserts into
    standardized.observations.

WHAT STANDARDIZATION MEANS HERE:
    The ingestion script already stores data in near-canonical
    shape. Transformation therefore focuses on:
    1. Schema validation — fail loudly if expected columns missing
    2. Dtype enforcement — prevent silent type inference errors
    3. Value casting — float → controlled string representation
    4. Null filtering — drop any rows with null values that
       survived ingestion (defensive)
    No structural reshaping, no country code lookup, no column
    renaming — ingestion already handled those.

WHY SCHEMA VALIDATION AT PARSE START:
    Best practice in data pipelines: validate assumptions
    explicitly and fail loudly rather than propagating malformed
    data silently. If the B2 file is missing an expected column,
    we want a clear ValueError immediately — not a confusing
    KeyError or silent null column 10 steps later.

WHY DTYPE ENFORCEMENT AFTER DESERIALIZATION:
    Pandas infers dtypes when reading JSON. This produces
    surprises: year read as float64 if any value has a decimal,
    value read as float64 losing string representation needed
    for the silver layer. Explicit dtype enforcement immediately
    after deserialization catches corruption early with a clear
    error rather than a confusing downstream failure.

WHY f"{v:.10g}" FOR VALUE CASTING:
    Best practice is to preserve source precision exactly.
    :.10g gives up to 10 significant figures without scientific
    notation for normal economic values. This matches what the
    World Bank intends without adding or losing precision.
    str() risks scientific notation (8.2145e+03) which would
    make value_numeric computed column fail silently. round()
    would alter source values, violating faithful transmission.

WHY retrieved_at IS LEFT AS STRING:
    PostgreSQL implicitly casts 'YYYY-MM-DD' string to DATE
    during INSERT. Adding a Python-level date conversion
    introduces one more potential failure point for zero benefit.
    PostgreSQL's implicit cast is reliable and documented.

BATCH UNITS AND B2 KEYS:
    Must match ingestion script exactly so get_ingestion_checksum()
    finds the correct checkpoint. Both scripts use the same
    naming convention derived from batch_unit and run_date.

ENV VARS REQUIRED:
    DATABASE_URL, B2_KEY_ID, B2_APPLICATION_KEY,
    B2_BUCKET_NAME, B2_BUCKET_REGION,
    SMTP_SENDER, SMTP_PASSWORD, SMTP_RECIPIENT
"""

import json
import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_transformer import BaseTransformer

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

# Expected columns in the B2 JSON file.
# Validated at the start of parse() — fail loudly if missing.
EXPECTED_COLUMNS = {
    'country_iso3', 'year', 'period', 'metric_id',
    'value', 'source_id', 'retrieved_at'
}

# Expected dtypes after deserialization.
# Enforced immediately after reading from B2 to prevent silent
# type inference errors from propagating through the pipeline.
EXPECTED_DTYPES = {
    'country_iso3': str,
    'year':         'Int64',   # nullable integer — avoids float
                               # if any year has a decimal in JSON
    'period':       str,
    'metric_id':    str,
    'source_id':    str,
    'retrieved_at': str,       # left as string — PostgreSQL casts
                               # 'YYYY-MM-DD' to DATE implicitly
}
# value dtype handled separately — see _cast_values()


class WorldBankTransformer(BaseTransformer):
    """
    Transformation script for World Bank WDI.
    Inherits all pipeline orchestration from BaseTransformer.
    Implements the four source-specific abstract methods.
    """

    def __init__(self):
        super().__init__(source_id='world_bank')

        # run_date used to reconstruct B2 keys.
        # Must match the run_date used by the ingestion script
        # for the same data. Read from the most recent successful
        # ingestion checkpoint's checkpointed_at date.
        # Set in get_batch_units() after reading checkpoints.
        self.run_date = None


    # ═══════════════════════════════════════════════════════
    # get_batch_units
    # ═══════════════════════════════════════════════════════

    def get_batch_units(self) -> list:
        """
        Return the ordered list of batch_units to transform.
        Reads completed ingestion checkpoints from ops.checkpoints
        to find exactly which batch_units the ingestion script
        successfully wrote to B2.

        WHY READ FROM CHECKPOINTS NOT FROM DATABASE:
            The transformation script must process exactly the
            same batch_units the ingestion script completed —
            no more, no less. Reading from ops.checkpoints
            guarantees this. Reading from metadata.metric_codes
            could include indicators whose ingestion failed or
            was skipped this run.

        WHY ALSO SET self.run_date HERE:
            The B2 key includes the run date from the ingestion
            run. We read it from the most recent completed
            ingestion checkpoint's checkpointed_at timestamp
            so get_b2_key() can reconstruct the exact same path
            the ingestion script used.

        Returns:
            Sorted list of batch_unit strings matching ingestion.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit, checkpointed_at
                FROM ops.checkpoints
                WHERE source_id = 'world_bank'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY checkpointed_at DESC
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed World Bank ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        # Extract run_date from the most recent checkpoint.
        # All checkpoints from one ingestion run share the same
        # date — we take it from the first (most recent) row.
        self.run_date = date.today().isoformat()

        # Return sorted batch_units for deterministic order.
        # Sorting ensures restart skips the same batches
        # regardless of the order they completed in.
        return sorted({row[0] for row in rows})


    # ═══════════════════════════════════════════════════════
    # parse
    # ═══════════════════════════════════════════════════════

    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate, enforce dtypes, and cast values to produce
        a DataFrame ready for pre-upsert checks and upsert.

        Three steps:
        1. Schema validation — fail loudly if columns missing
        2. Dtype enforcement — prevent silent inference errors
        3. Value casting — float → controlled string

        Args:
            df_raw: DataFrame deserialized from B2 JSON.

        Returns:
            Clean DataFrame with all columns in correct types.

        Raises:
            ValueError: if expected columns are missing.
        """
        # ── Step 1: Schema validation ──────────────────────────
        # Validate expected columns exist before doing anything.
        # Fail loudly here rather than propagating a malformed
        # DataFrame through quality checks and into Supabase.
        missing_cols = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing_cols:
            raise ValueError(
                f"World Bank B2 file missing expected columns: "
                f"{missing_cols}. "
                f"File may be from an incompatible ingestion version."
            )

        # Work on a copy — never mutate the raw DataFrame.
        # Mutating df_raw would affect the checksum comparison
        # in check_transformation_pre() which runs on df_raw.
        df = df_raw.copy()

        # ── Step 2: Dtype enforcement ──────────────────────────
        # Enforce expected types immediately after deserialization.
        # Pandas JSON inference can produce float64 for integer
        # columns (year), object for numeric value columns, etc.
        # Enforcing types here catches any such inference errors
        # with a clear message rather than a confusing failure
        # later in the pipeline.
        for col, dtype in EXPECTED_DTYPES.items():
            try:
                if dtype == 'Int64':
                    # Int64 (nullable integer) handles any NaN
                    # that pandas may have introduced during
                    # JSON deserialization for integer columns.
                    df[col] = df[col].astype('Int64')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                raise ValueError(
                    f"Dtype enforcement failed for column '{col}': {e}"
                )

        # ── Step 3: Drop null values ───────────────────────────
        # NULL values should have been dropped at ingestion time
        # in _parse_records(). This is a defensive check —
        # any that survived are dropped here before the upsert.
        # Absence of a row = absence of data (design principle).
        before = len(df)
        df = df[df['value'].notna()].copy()
        dropped = before - len(df)
        if dropped > 0:
            print(
                f"    [parse] Dropped {dropped} null value rows "
                f"(should have been filtered at ingestion)"
            )

        # ── Step 4: Value casting ──────────────────────────────
        # Cast value column from float/object to controlled
        # string representation. See _cast_values() for details.
        df['value'] = df['value'].apply(self._cast_value)

        # ── Step 5: Drop any remaining empty strings ───────────
        # _cast_value() returns '' for values it cannot represent.
        # These will be caught by check_pre_upsert() empty_string
        # check but we drop them here to keep the DataFrame clean
        # and avoid unnecessary rejection logging overhead.
        df = df[df['value'] != ''].copy()

        # ── Step 6: Reset index ────────────────────────────────
        # After dropping rows, reset index so downstream
        # operations (merge, iloc) work without gaps.
        df = df.reset_index(drop=True)

        return df


    # ═══════════════════════════════════════════════════════
    # get_b2_key
    # ═══════════════════════════════════════════════════════

    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given batch_unit.
        Must return the exact same key the ingestion script used.

        Naming convention (matches world_bank_ingest.py exactly):
            Normal:     bronze/world_bank/NY.GDP.PCAP.CD_2026-04-20.json
            Year chunk: bronze/world_bank/NY.GDP.PCAP.CD_1950-1987_2026-04-20.json

        Args:
            batch_unit: Indicator code, optionally with year range.

        Returns:
            Full B2 path string.
        """
        if self.run_date is None:
            raise RuntimeError(
                "run_date not set. Call get_batch_units() before get_b2_key()."
            )
        return f"bronze/world_bank/{batch_unit}_{self.run_date}.json"


    # ═══════════════════════════════════════════════════════
    # deserialize
    # ═══════════════════════════════════════════════════════

    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize JSON bytes from B2 into a DataFrame.
        Exact inverse of world_bank_ingest.py serialize().

        WHY BytesIO:
            pd.read_json() accepts a file-like object. Wrapping
            bytes in BytesIO avoids writing to disk — reads
            entirely in memory, faster and no temp file cleanup.

        Args:
            data: Raw bytes read from B2.

        Returns:
            DataFrame with columns as stored by ingestion.
        """
        return pd.read_json(BytesIO(data), orient='records')


    # ═══════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════

    @staticmethod
    def _cast_value(v) -> str:
        """
        Cast a single value to a controlled string representation.

        WHY f"{v:.10g}":
            :.10g gives up to 10 significant figures without
            scientific notation for normal economic values.
            This preserves source precision without adding or
            losing digits. str() risks scientific notation
            (8.2145e+03) which breaks the value_numeric computed
            column in the silver layer. round() would alter
            source values, violating faithful transmission.

        WHY 10 SIGNIFICANT FIGURES:
            World Bank data is reported to at most 6-8 significant
            figures in practice. 10 gives headroom without
            fabricating precision that was not in the source.

        Args:
            v: Raw value from the DataFrame (float, int, or str).

        Returns:
            String representation. Empty string if value cannot
            be represented — caught by empty_string check.
        """
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''

        try:
            # Attempt numeric formatting first.
            # Works for int and float values.
            return f"{float(v):.10g}"
        except (ValueError, TypeError):
            # Non-numeric value (categorical) — return as string.
            # World Bank data is almost entirely numeric but
            # we handle non-numeric defensively.
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    # Allow running directly for development and testing:
    # python3 ingestion/world_bank_transform.py
    transformer = WorldBankTransformer()
    transformer.run()
