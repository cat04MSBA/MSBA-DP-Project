"""
transformation/wipo_transform.py
=================================
Transformation script for WIPO IP Statistics.

BATCH UNIT: 'ai_patent_count' — matches wipo_ingest.py.
Reads bronze/wipo_ip/ai_patent_count_{date}.csv from B2,
maps ISO2 → ISO3, casts counts, upserts.
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


class WIPOTransformer(BaseTransformer):

    def __init__(self):
        super().__init__(source_id='wipo_ip')
        self.run_date     = None
        self._wipo_to_iso3 = None


    def get_batch_units(self) -> list:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT batch_unit
                FROM ops.checkpoints
                WHERE source_id = 'wipo_ip'
                  AND stage     = 'ingestion_batch'
                  AND status    = 'complete'
                  AND checkpointed_at >= NOW() - INTERVAL '10 days'
                ORDER BY batch_unit
            """)).fetchall()

        if not rows:
            raise ValueError(
                "No completed WIPO ingestion checkpoint found "
                "within the last 10 days. Run ingestion first."
            )

        self.run_date = date.today().isoformat()
        return [row[0] for row in rows]


    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        missing = EXPECTED_COLUMNS - set(df_raw.columns)
        if missing:
            raise ValueError(f"WIPO B2 file missing columns: {missing}")

        df = df_raw.copy()
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        for col in ['country_iso3', 'period', 'metric_id', 'source_id', 'retrieved_at']:
            df[col] = df[col].astype(str)

        # Map ISO2 → ISO3.
        wipo_to_iso3 = self._get_wipo_to_iso3()
        df['country_iso3'] = df['country_iso3'].map(wipo_to_iso3)

        unmapped = df['country_iso3'].isna().sum()
        if unmapped > 0:
            print(f"    [parse] Dropped {unmapped} rows with unmappable ISO2 codes")
        df = df[df['country_iso3'].notna()].copy()

        before = len(df)
        df = df[df['value'].notna() & (df['value'].astype(str) != '')].copy()
        if len(df) < before:
            print(f"    [parse] Dropped {before - len(df)} null rows")

        df['value'] = df['value'].apply(self._cast_value)
        df = df[df['value'] != ''].copy()

        return df.reset_index(drop=True)


    def get_b2_key(self, batch_unit: str) -> str:
        if self.run_date is None:
            raise RuntimeError("Call get_batch_units() first.")
        return f"bronze/wipo_ip/{batch_unit}_{self.run_date}.csv"


    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(data), dtype=str)


    def _get_wipo_to_iso3(self) -> dict:
        if self._wipo_to_iso3 is not None:
            return self._wipo_to_iso3
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT code, iso3 FROM metadata.country_codes
                WHERE source_id = 'wipo_ip'
            """)).fetchall()
        self._wipo_to_iso3 = {r[0]: r[1] for r in rows}
        return self._wipo_to_iso3

    @staticmethod
    def _cast_value(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        try:
            return str(int(float(v)))
        except (ValueError, TypeError):
            s = str(v).strip()
            return s if s else ''


if __name__ == "__main__":
    WIPOTransformer().run()
