"""
transformation/oecd_transform.py
=================================
Transformation script for OECD MSTI.

BATCH UNITS: One per metric — 'berd_gdp', 'goverd_gdp',
'researchers_per_thousand'. Matches oecd_ingest.py exactly.

Each metric is read from its own B2 file, validated, and
upserted separately. Transformation checkpoint per metric.
"""

import io
import pandas as pd
from datetime import date
from sqlalchemy import text

from database.base_transformer import BaseTransformer

EXPECTED_COLUMNS = {
    'country_iso3', 'year', 'period', 'metric_id',
    'value', 'source_id', 'retrieved_at'
}

VALID_OECD_METRIC_IDS = {
    'oecd.berd_gdp',
    'oecd.goverd_gdp',
    'oecd.researchers_per_thousand',
}

# Batch unit → metric_id (matches oecd_ingest.py)
BATCH_TO_METRIC = {
    'berd_gdp':                 'oecd.berd_gdp',
    'goverd_gdp':               'oecd.goverd_gdp',
    'researchers_per_thousand': 'oecd.researchers_per_thousand',
}


class OECDTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(source_id='oecd_msti')
        self.run_date = None


    def get_batch_units(self) -> list:
        """
        Return the list of metrics that have completed ingestion
        checkpoints within the last 10 days.
        Sets run_date = date.today() to match ingestion B2 keys.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit
                FROM ops.checkpoints
                WHERE source_id = 'oecd_msti'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY batch_unit
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed OECD ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        # run_date matches ingestion which uses date.today().isoformat()
        self.run_date = date.today().isoformat()
        return [row[0] for row in rows]


    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Validate schema, enforce dtypes, apply defensive filters,
        cast values for one OECD metric batch.
        """
        # ── Schema validation ──────────────────────────────────
        missing = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing:
            raise ValueError(f"OECD B2 file missing columns: {missing}")

        df = df_raw.copy()

        # ── Dtype enforcement ──────────────────────────────────
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        for col in ['country_iso3', 'period', 'metric_id', 'source_id', 'retrieved_at']:
            df[col] = df[col].astype(str)

        # ── Metric_id validation ───────────────────────────────
        invalid = df[~df['metric_id'].isin(VALID_OECD_METRIC_IDS)]
        if len(invalid) > 0:
            print(f"    [parse] Warning: {len(invalid)} rows with unexpected metric_ids")

        # ── Defensive since_date filter ────────────────────────
        since_year = self._get_since_year()
        before = len(df)
        df = df[df['year'] >= since_year].copy()
        if len(df) < before:
            print(f"    [parse] Defensive filter removed {before - len(df)} rows")

        # ── Null value drop ────────────────────────────────────
        before = len(df)
        df = df[df['value'].notna() & (df['value'].astype(str) != '')].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null rows")

        # ── Value casting ──────────────────────────────────────
        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        return df.reset_index(drop=True)


    def get_b2_key(self, batch_unit: str) -> str:
        """
        Must match oecd_ingest.py get_b2_key() exactly.
        bronze/oecd_msti/{metric_name}_{date}.csv
        """
        if self.run_date is None:
            raise RuntimeError("Call get_batch_units() first.")
        return f"bronze/oecd_msti/{batch_unit}_{self.run_date}.csv"


    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False)


    def _get_since_year(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_retrieved FROM metadata.sources
                WHERE source_id = 'oecd_msti'
            """)).fetchone()
        last = row[0] if row else None
        return last.year - 5 if last else 1981

    @staticmethod
    def _cast_value(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return f"{float(v):.10g}"
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    OECDTransformer().run()
