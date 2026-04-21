"""
transformation/pwt_transform.py
================================
Transformation script for Penn World Tables 11.0.

BATCH UNITS: One per PWT variable — 47 total.
Each reads its own B2 JSON file, validates, upserts.
"""

import pandas as pd
from datetime import date
from io import BytesIO
from sqlalchemy import text

from database.base_transformer import BaseTransformer

EXPECTED_COLUMNS = {
    'country_iso3', 'year', 'period', 'metric_id',
    'value', 'source_id', 'retrieved_at'
}
PWT_METRIC_PREFIX = 'pwt.'


class PWTTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(source_id='pwt')
        self.run_date = None


    def get_batch_units(self) -> list:
        """
        Return the list of PWT variables that have completed
        ingestion checkpoints within the last 10 days.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit
                FROM ops.checkpoints
                WHERE source_id = 'pwt'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY batch_unit
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed PWT ingestion checkpoints found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = date.today().isoformat()
        return [row[0] for row in rows]


    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        missing = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing:
            raise ValueError(f"PWT B2 file missing columns: {missing}")

        df = df_raw.copy()
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        for col in ['country_iso3', 'period', 'metric_id', 'source_id', 'retrieved_at']:
            df[col] = df[col].astype(str)

        # Defensive metric_id prefix check.
        invalid = df[~df['metric_id'].str.startswith(PWT_METRIC_PREFIX)]
        if len(invalid) > 0:
            print(f"    [parse] Warning: {len(invalid)} rows with non-PWT metric_ids")

        # Defensive since_date filter.
        since_year = self._get_since_year()
        before = len(df)
        df = df[df['year'] >= since_year].copy()
        if len(df) < before:
            print(f"    [parse] Defensive filter removed {before - len(df)} rows")

        # Drop nulls.
        before = len(df)
        df = df[df['value'].notna() & (df['value'].astype(str) != '')].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null rows")

        # Cast values.
        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        return df.reset_index(drop=True)


    def get_b2_key(self, batch_unit: str) -> str:
        """
        Must match pwt_ingest.py exactly.
        bronze/pwt/{variable_name}_{date}.json
        """
        if self.run_date is None:
            raise RuntimeError("Call get_batch_units() first.")
        return f"bronze/pwt/{batch_unit}_{self.run_date}.json"


    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(BytesIO(data), orient='records', convert_dates=False, dtype=False)


    def _get_since_year(self) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT last_retrieved FROM metadata.sources
                WHERE source_id = 'pwt'
            """)).fetchone()
        last = row[0] if row else None
        return last.year - 5 if last else 1950

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
    PWTTransformer().run()
