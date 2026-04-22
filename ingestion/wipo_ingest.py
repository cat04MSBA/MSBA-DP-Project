"""
ingestion/wipo_ingest.py
========================
Ingestion script for WIPO IP Statistics AI Patent Applications.

BATCH UNIT: 'ai_patent_count' — the single WIPO metric.

WHY PER METRIC (not full_file):
    WIPO has one metric. Using the metric name as the batch unit
    instead of 'full_file' makes ops.checkpoints readable:
    'ai_patent_count: complete' tells you exactly what was processed.
    Consistent with the per-metric design principle across all sources.

B2 FILE:
    bronze/wipo_ip/ai_patent_count_{date}.csv

CSV STRUCTURE:
    - 6 header rows to skip
    - Columns: Origin (country name), Origin (Code) (ISO2),
               Office, then years 1980-2024 as columns
    - Wide format melted to tall in ingestion
    - Empty cells dropped (absence = no data)
"""

import io
import time
import pandas as pd
from datetime import date

from database.base_ingestor import BaseIngestor

WIPO_HEADER_ROWS = 6
RETRY_DELAYS     = [60, 300, 600]
BATCH_UNIT       = 'ai_patent_count'


class WIPOIngestor(BaseIngestor):

    def __init__(self):
        super().__init__(source_id='wipo_ip')
        self.run_date = date.today().isoformat()


    def get_batch_units(self, since_date: date) -> list:
        """
        Returns ['ai_patent_count'] — WIPO has one metric.
        Using the metric name instead of 'full_file' makes
        checkpoints directly readable and consistent with
        the per-metric design principle.
        """
        self._since_date = since_date
        return [BATCH_UNIT]


    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        last_error = None
        for attempt, delay in enumerate(RETRY_DELAYS, 1):
            try:
                return self._read_wipo_file(since_date)
            except Exception as e:
                last_error = e
                if attempt < len(RETRY_DELAYS):
                    print(f"    [retry {attempt}] WIPO: {e}. Waiting {delay}s...")
                    time.sleep(delay)
        raise RuntimeError(
            f"WIPO failed after {len(RETRY_DELAYS)} retries. "
            f"Last error: {last_error}"
        )


    def _read_wipo_file(self, since_date: date) -> tuple:
        """
        Download WIPO CSV from B2 (original file uploaded by
        upload_to_b2.py), melt wide→tall, filter by since_date.
        """
        # Read original file from B2 — uploaded by upload_to_b2.py
        # at path: bronze/wipo_ip/source_{date}.csv
        source_key = f"bronze/wipo_ip/source_{self.run_date}.csv"
        csv_bytes  = self.b2.download(source_key)

        df_wide = pd.read_csv(
            io.BytesIO(csv_bytes),
            skiprows = WIPO_HEADER_ROWS,
            dtype    = str,
        )

        # Identify year columns (4-digit numeric strings).
        year_cols = [
            col for col in df_wide.columns
            if str(col).strip().isdigit() and len(str(col).strip()) == 4
        ]
        if not year_cols:
            raise ValueError("No year columns found in WIPO CSV.")

        # Melt wide → tall.
        df_tall = df_wide.melt(
            id_vars    = ['Origin', 'Origin (Code)'],
            value_vars = year_cols,
            var_name   = 'year_str',
            value_name = 'patent_count',
        )

        # Drop empty cells — absence = no data.
        df_tall = df_tall[
            df_tall['patent_count'].notna() &
            (df_tall['patent_count'].str.strip() != '')
        ].copy()

        # Apply since_date year filter.
        df_tall = df_tall[
            df_tall['year_str'].astype(int) >= since_date.year
        ].copy()

        df = self._parse_wipo(df_tall)

        # raw_row_count = rows AFTER melt, empty-drop, year-filter,
        # and parsing — the count that matches len(df) exactly.
        # WHY NOT count before the since_date filter:
        #     The filter removes years before the window. If counted
        #     before filtering, raw_row_count > len(df) on any
        #     incremental run, causing check_ingestion_pre() to raise
        #     a false CRITICAL failure. Counting after all drops is
        #     consistent with §4.5 of the reference doc.
        raw_row_count = len(df)

        return raw_row_count, df


    def _parse_wipo(self, df_tall: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in df_tall.iterrows():
            iso2         = str(row.get('Origin (Code)', '') or '').strip()
            year_str     = str(row.get('year_str', '') or '').strip()
            patent_count = str(row.get('patent_count', '') or '').strip()

            if not iso2 or not year_str or not patent_count:
                continue
            try:
                year  = int(year_str)
                count = int(float(patent_count))
            except (ValueError, TypeError):
                continue

            if count == 0:
                continue

            rows.append({
                'country_iso3': iso2,
                'year':         year,
                'period':       'annual',
                'metric_id':    'wipo.ai_patent_count',
                'value':        str(count),
                'source_id':    'wipo_ip',
                'retrieved_at': date.today().isoformat(),
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=['country_iso3', 'year', 'period', 'metric_id',
                     'value', 'source_id', 'retrieved_at']
        )


    def fetch_metric_metadata(self, metric_id: str) -> dict:
        return {
            'metric_id':   'wipo.ai_patent_count',
            'metric_name': 'AI Patent Applications',
            'source_id':   'wipo_ip',
            'category':    'AI Production',
            'unit':        'count',
            'description': 'AI patent applications by country per year.',
            'frequency':   'annual',
        }


    def get_b2_key(self, batch_unit: str) -> str:
        """
        bronze/wipo_ip/ai_patent_count_{date}.csv
        batch_unit is always 'ai_patent_count'.
        """
        return f"bronze/wipo_ip/{batch_unit}_{self.run_date}.csv"


    def serialize(self, df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False).encode('utf-8')

    def deserialize(self, data: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(data), dtype=str)


if __name__ == "__main__":
    WIPOIngestor().run()
