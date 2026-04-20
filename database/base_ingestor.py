"""
database/base_ingestor.py
=========================
Abstract base class for all ingestion scripts.

WHAT THIS FILE DOES:
    Provides the complete ingestion pipeline skeleton that every
    source script inherits. A source script only needs to
    implement these methods:
        fetch()                 → hit the source API or read the
                                  source file, return raw data
        fetch_metric_metadata() → given an unknown metric code,
                                  fetch its metadata from source
        get_batch_units()       → return list of batch units
        get_b2_key()            → return B2 path for a batch unit
        serialize()             → DataFrame → bytes for B2
        deserialize()           → bytes from B2 → DataFrame

    Everything else — ops.pipeline_runs logging, B2 uploads,
    quality checks, checkpointing, restart logic, unknown metric
    detection, rejection logging, and alert emails — is handled
    here once and inherited by all 7 source scripts.

WHY A BASE CLASS:
    All 7 sources follow the same ingestion sequence. Writing
    it once means a new source script is ~50 lines not ~500,
    every source is guaranteed to log and checkpoint identically,
    and a bug fix applies to all sources at once.

WHY ops.pipeline_runs IS OPENED AND CLOSED HERE:
    The audit trail must be complete regardless of how the script
    is triggered — Prefect schedule, manual trigger, or direct
    command line execution during development. The base class
    opens and closes the run entry unconditionally on every
    execution. If logging lived in the Prefect subflow, a
    developer running the script directly would produce no
    audit trail.

WHY UNKNOWN METRIC DETECTION LIVES HERE:
    Every source encounters unknown metrics the same way.
    Detection, row parking, and the alert email are identical
    across all sources. Only the API call to fetch that metric's
    metadata differs per source, which is why each source
    implements fetch_metric_metadata() independently.

WHY PARKED ROWS ARE PERSISTED TO B2:
    When an unknown metric is detected, its rows are held in
    memory during the current run then saved to B2 as JSON.
    This allows add_metric.py --action yes to immediately
    upsert the rows after approval without waiting for the
    next scheduled run. Without persistence, Oxford rows
    parked during an upload could wait months (until the next
    Oxford file upload) before being ingested. B2 path:
        bronze/parked/{source_id}/{metric_id}_{date}.json
    These files are deleted by add_metric.py after upsert —
    the one exception to the no-delete rule, because parked
    rows are temporary staging data, not permanent bronze records.

PERFORMANCE DECISIONS:
    - Known metrics loaded ONCE per run in run(), passed to
      _handle_unknown_metrics() per batch. Avoids one DB query
      per batch — critical for World Bank with 26,000+ batches.
    - Rejections accumulated across all batches and flushed in
      one bulk INSERT at run end instead of one INSERT per batch.
    - B2 client and DB engine instantiated once at __init__
      and reused across all batches.

INGESTION SEQUENCE (per batch unit):
    1.  Read last_retrieved from metadata.sources
    2.  Open ops.pipeline_runs entry with status='running'
    3.  Load known metrics once for unknown metric detection
    4.  For each batch unit:
        a. Write checkpoint status='in_progress'
        b. fetch() raw data from source
        c. Detect and park unknown metric rows
        d. check_ingestion_pre() — row count match
        e. Serialize and upload to B2
        f. Download back from B2 and deserialize
        g. check_ingestion_post() — checksum + row count
        h. write_complete() checkpoint with verified checksum
    5.  Write ingestion_complete checkpoint
    6.  Persist parked rows to B2 for later approval
    7.  Bulk flush rejections to ops.rejection_summary
    8.  Update ops.pipeline_runs with final status
    9.  Update last_retrieved in metadata.sources
"""

import json
import traceback
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, date
from sqlalchemy import text

from database.connection import get_engine
from database.b2_upload import B2Client
from database.checkpoint import (
    write_start,
    write_complete,
    get_completed_batches,
)
from database.quality_checks import (
    CriticalCheckError,
    check_ingestion_pre,
    check_ingestion_post,
)
from database.email_utils import (
    send_critical_alert,
    send_unknown_metric_alert,
)


class BaseIngestor(ABC):
    """
    Abstract base class for all ingestion scripts.
    Subclasses implement the six abstract methods below.
    All shared pipeline logic lives here.
    """

    def __init__(self, source_id: str):
        """
        Initialise shared resources once per script run.

        Args:
            source_id: Must match metadata.sources.source_id exactly.
                       Examples: 'world_bank', 'imf', 'oxford'
        """
        self.source_id = source_id

        # One database engine for the entire run.
        # NullPool means the connection closes on script exit.
        self.engine = get_engine()

        # B2 client instantiated once. Credentials read from
        # env at instantiation and reused across all batches.
        self.b2 = B2Client()

        # Set when ops.pipeline_runs entry is opened.
        # Used as foreign key throughout the run.
        self.run_id = None

        # Counters written to ops.pipeline_runs at run end.
        self.total_inserted = 0
        self.total_rejected = 0

        # Rejection dicts accumulated across all batches.
        # Flushed to ops.rejection_summary in one bulk INSERT
        # at run end — more efficient than one INSERT per batch.
        self.all_rejections = []

        # Parked rows accumulated across all batches.
        # Each entry: {metric_id, batch_unit, rows (DataFrame)}.
        # Persisted to B2 at run end so add_metric.py can
        # upsert them immediately after team approval.
        self.parked_rows = []


    # ═══════════════════════════════════════════════════════
    # ABSTRACT METHODS — implemented by each source script
    # ═══════════════════════════════════════════════════════

    @abstractmethod
    def fetch(self, batch_unit: str, since_date: date) -> tuple:
        """
        Fetch raw data for one batch unit from the source.

        Must return (raw_row_count, df) where:
            raw_row_count: int — records in the raw API response
                           before any parsing. Used by
                           check_ingestion_pre().
            df:            pd.DataFrame — parsed data with columns:
                           country_iso3, year, period, metric_id,
                           value, source_id, retrieved_at.
        """

    @abstractmethod
    def fetch_metric_metadata(self, metric_id: str) -> dict:
        """
        Fetch metadata for an unknown metric from the source API.
        Called when a metric_id appears that is not registered
        in metadata.metrics.

        Must return a dict with keys:
            metric_id, metric_name, source_id, category,
            unit, description, frequency
        """

    @abstractmethod
    def get_batch_units(self, since_date: date) -> list:
        """
        Return the ordered list of batch_units to process.
        Each item is passed to fetch() as batch_unit.
        """

    @abstractmethod
    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key (path within bucket) for a
        given batch_unit.
        """

    @abstractmethod
    def serialize(self, df: pd.DataFrame) -> bytes:
        """
        Serialize DataFrame to bytes for B2 upload.
        Format is source-specific (JSON, CSV, XLSX, Stata).
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize bytes from B2 back into a DataFrame.
        Must be the exact inverse of serialize().
        """


    # ═══════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════════════════

    def run(self):
        """
        Execute the full ingestion pipeline for this source.
        Called by the Prefect subflow for each source.
        """
        with self.engine.connect() as conn:
            try:
                # ── Step 1: Read last_retrieved ───────────────
                since_date = self._get_since_date(conn)

                # ── Step 2: Open pipeline run entry ───────────
                self.run_id = self._open_pipeline_run(conn)
                conn.commit()

                # ── Step 3: Load known metrics ONCE ───────────
                # Passed to _handle_unknown_metrics() on every
                # batch. Loading once avoids one DB query per
                # batch — critical for World Bank with 26,000+
                # batches on the first full historical run.
                known_metrics = self._load_known_metrics(conn)

                # ── Step 4: Get batch units ────────────────────
                batch_units = self.get_batch_units(since_date)

                # ── Step 5: Find already-completed batches ─────
                completed = get_completed_batches(
                    conn, self.run_id, self.source_id,
                    stage='ingestion_batch'
                )

                # ── Step 6: Process each batch unit ───────────
                for batch_unit in batch_units:
                    if batch_unit in completed:
                        print(f"  [skip] {batch_unit} already complete")
                        continue

                    self._process_batch(
                        conn, batch_unit, since_date, known_metrics
                    )
                    # Commit after each batch so completed
                    # checkpoints survive a mid-run crash.
                    conn.commit()

                # ── Step 7: Write ingestion_complete ──────────
                cp_id = write_start(
                    conn, self.run_id, self.source_id,
                    stage='ingestion_complete',
                    batch_unit='all'
                )
                write_complete(conn, cp_id, checksum='', row_count=0)
                conn.commit()

                # ── Step 8: Persist parked rows to B2 ─────────
                # Save any unknown-metric rows to B2 so
                # add_metric.py can upsert them after approval
                # without waiting for the next scheduled run.
                self._persist_parked_rows()

                # ── Step 9: Bulk flush rejections ─────────────
                self._flush_rejections(conn)
                conn.commit()

                # ── Step 10: Close pipeline run ────────────────
                final_status = (
                    'success' if self.total_rejected == 0
                    else 'success_with_rej_rows'
                )
                self._close_pipeline_run(conn, final_status)

                # ── Step 11: Update last_retrieved ────────────
                self._update_last_retrieved(conn)
                conn.commit()

                print(
                    f"✓ {self.source_id} ingestion complete. "
                    f"Inserted: {self.total_inserted}, "
                    f"Rejected: {self.total_rejected}, "
                    f"Parked: {len(self.parked_rows)} metric(s)"
                )

            except CriticalCheckError as e:
                conn.rollback()
                self._handle_critical(e)

            except Exception as e:
                conn.rollback()
                self._handle_unexpected(e)


    # ═══════════════════════════════════════════════════════
    # BATCH PROCESSING
    # ═══════════════════════════════════════════════════════

    def _process_batch(self, conn, batch_unit: str,
                       since_date: date, known_metrics: set):
        """
        Process one batch unit through the full ingestion sequence:
            a. Write checkpoint in_progress
            b. fetch() raw data from source
            c. Detect and park unknown metric rows
            d. check_ingestion_pre() — row count match
            e. Serialize and upload to B2
            f. Download back from B2 and deserialize
            g. check_ingestion_post() — checksum + row count
            h. write_complete() with verified checksum
        """
        print(f"  [start] {batch_unit}")

        # ── a. Write in_progress checkpoint ───────────────────
        cp_id = write_start(
            conn, self.run_id, self.source_id,
            stage='ingestion_batch',
            batch_unit=batch_unit
        )

        # ── b. Fetch raw data ──────────────────────────────────
        raw_row_count, df = self.fetch(batch_unit, since_date)

        # ── c. Detect unknown metrics ──────────────────────────
        # Park rows with unregistered metric_ids, email team.
        # Returns clean df with unknown metric rows removed.
        # known_metrics passed in from run() — no DB query here.
        df = self._handle_unknown_metrics(
            df, batch_unit, known_metrics
        )

        # ── d. check_ingestion_pre ─────────────────────────────
        check_ingestion_pre(
            conn, self.run_id, self.source_id,
            batch_unit, raw_row_count, df
        )

        # ── e. Serialize and upload to B2 ─────────────────────
        b2_key    = self.get_b2_key(batch_unit)
        raw_bytes = self.serialize(df)
        self.b2.upload(b2_key, raw_bytes)

        # ── f. Download back from B2 ───────────────────────────
        downloaded_bytes = self.b2.download(b2_key)
        df_readback      = self.deserialize(downloaded_bytes)

        # ── g. check_ingestion_post ────────────────────────────
        verified_checksum, verified_row_count = check_ingestion_post(
            conn, self.run_id, self.source_id,
            batch_unit, df, df_readback
        )

        # ── h. Write complete checkpoint ───────────────────────
        write_complete(conn, cp_id, verified_checksum, verified_row_count)

        self.total_inserted += len(df)
        print(f"  [done]  {batch_unit} — {len(df)} rows")


    # ═══════════════════════════════════════════════════════
    # UNKNOWN METRIC DETECTION
    # ═══════════════════════════════════════════════════════

    def _handle_unknown_metrics(self, df: pd.DataFrame,
                                batch_unit: str,
                                known_metrics: set) -> pd.DataFrame:
        """
        Detect rows whose metric_id is not in metadata.metrics.
        Park those rows in self.parked_rows (persisted to B2 at
        run end). Email the team with auto-fetched metadata.
        Return the clean DataFrame with unknown rows removed.

        WHY known_metrics IS PASSED IN (not queried here):
            This function is called once per batch. For World Bank
            with 26,000+ batches on the first full historical run,
            querying the DB here would add 26,000 identical queries.
            The caller loads known_metrics once in run() and passes
            it to every batch — one query total per run.

        WHY PARKED ROWS ARE ACCUMULATED IN MEMORY (not written to B2 here):
            Writing to B2 inside the batch loop would add one B2
            write per unknown metric per batch. Since the same
            unknown metric might appear across many batches, we
            accumulate all parked rows in memory and persist them
            once at run end in _persist_parked_rows(). More
            efficient and produces one file per metric not many.
        """
        unknown_mask       = ~df['metric_id'].isin(known_metrics)
        unknown_metric_ids = df[unknown_mask]['metric_id'].unique()

        if len(unknown_metric_ids) == 0:
            return df

        for metric_id in unknown_metric_ids:
            unknown_rows = df[df['metric_id'] == metric_id].copy()

            # Accumulate in memory — persisted to B2 at run end.
            self.parked_rows.append({
                'metric_id':  metric_id,
                'batch_unit': batch_unit,
                'rows':       unknown_rows,
            })

            # Auto-fetch metric metadata from source API.
            try:
                metadata = self.fetch_metric_metadata(metric_id)
            except Exception as e:
                metadata = {
                    'metric_name': 'UNKNOWN — fetch failed',
                    'unit':        None,
                    'category':    None,
                    'description': f'Auto-fetch failed: {e}',
                    'frequency':   'annual',
                }

            send_unknown_metric_alert(
                self.source_id, self.run_id,
                metric_id, metadata,
                len(unknown_rows), batch_unit
            )

        return df[~unknown_mask].copy()


    # ═══════════════════════════════════════════════════════
    # PARKED ROW PERSISTENCE
    # ═══════════════════════════════════════════════════════

    def _persist_parked_rows(self):
        """
        Serialize all parked rows to B2 JSON files, grouped by
        metric_id. One file per unique metric_id.

        WHY ONE FILE PER METRIC (not one per batch):
            The same unknown metric may appear across many batches
            (e.g. World Bank returns the same new indicator across
            60 years of data). Grouping by metric_id produces one
            coherent file that add_metric.py can read and upsert
            in one operation, rather than many small files.

        WHY PERSISTED TO B2 (not just memory):
            The ingestion script exits after run() completes.
            Any in-memory data is lost. Persisting to B2 means
            the parked rows survive across process boundaries —
            add_metric.py can run hours or days later and still
            find the rows to upsert.

        B2 PATH CONVENTION:
            bronze/parked/{source_id}/{metric_id}_{date}.json
            'parked' prefix distinguishes these from regular
            bronze data files. add_metric.py knows to look here.

        WHY THIS IS THE ONE EXCEPTION TO THE NO-DELETE RULE:
            Regular bronze files are never deleted — they are the
            permanent raw source record. Parked files are temporary
            staging data — they exist only until the team approves
            or rejects the metric. Once add_metric.py processes
            them (approve or reject), they are deleted from B2.
            Keeping them forever would accumulate stale staging
            data with no purpose.
        """
        if not self.parked_rows:
            return

        # Group all parked rows by metric_id.
        # Multiple batch entries for the same metric get merged.
        by_metric = {}
        for entry in self.parked_rows:
            metric_id = entry['metric_id']
            if metric_id not in by_metric:
                by_metric[metric_id] = []
            by_metric[metric_id].append(entry['rows'])

        today = date.today().isoformat()

        for metric_id, dfs in by_metric.items():
            # Combine all batches for this metric into one DataFrame.
            combined = pd.concat(dfs, ignore_index=True)

            # Serialize to JSON bytes.
            json_bytes = combined.to_json(
                orient='records',
                date_format='iso',
            ).encode('utf-8')

            # Sanitize metric_id for use in B2 key.
            # metric_id contains dots (e.g. 'wb.ny_gdp_pcap_cd')
            # which are safe in B2 keys but we replace with
            # underscores for cleaner filenames.
            safe_metric_id = metric_id.replace('.', '_')
            b2_key = (
                f"bronze/parked/{self.source_id}/"
                f"{safe_metric_id}_{today}.json"
            )

            try:
                self.b2.upload(b2_key, json_bytes)
                print(
                    f"  [parked] {metric_id} → {b2_key} "
                    f"({len(combined)} rows)"
                )
            except Exception as e:
                # Parking failure must not mask ingestion success.
                # Log and continue — the team was already emailed
                # about this metric. They can re-run ingestion to
                # re-generate the parked file.
                print(
                    f"  ⚠ Could not persist parked rows for "
                    f"{metric_id} to B2: {e}"
                )


    # ═══════════════════════════════════════════════════════
    # METADATA LOADERS
    # ═══════════════════════════════════════════════════════

    def _load_known_metrics(self, conn) -> set:
        """
        Load all known metric_ids from metadata.metrics.
        Called once in run() and passed to every batch to
        avoid repeated DB queries per batch.
        """
        rows = conn.execute(text(
            "SELECT metric_id FROM metadata.metrics"
        )).fetchall()
        return {r[0] for r in rows}


    # ═══════════════════════════════════════════════════════
    # ops.pipeline_runs HELPERS
    # ═══════════════════════════════════════════════════════

    def _open_pipeline_run(self, conn) -> int:
        """
        Insert a row into ops.pipeline_runs with status='running'.
        Returns run_id. A crash leaves status='running' with no
        completed_at — detectable by the team in ops.pipeline_runs.
        """
        result = conn.execute(text("""
            INSERT INTO ops.pipeline_runs (
                source_id, started_at, status
            ) VALUES (
                :source_id, :started_at, 'running'
            )
            RETURNING run_id
        """), {
            'source_id':  self.source_id,
            'started_at': datetime.utcnow(),
        })
        return result.fetchone()[0]

    def _close_pipeline_run(self, conn, status: str,
                            error_message: str = None):
        """
        Update ops.pipeline_runs with final status, completion
        timestamp, row counts, and error message if applicable.
        """
        conn.execute(text("""
            UPDATE ops.pipeline_runs
            SET
                completed_at  = :completed_at,
                rows_inserted = :rows_inserted,
                rows_rejected = :rows_rejected,
                status        = :status,
                error_message = :error_message
            WHERE run_id = :run_id
        """), {
            'run_id':        self.run_id,
            'completed_at':  datetime.utcnow(),
            'rows_inserted': self.total_inserted,
            'rows_rejected': self.total_rejected,
            'status':        status,
            'error_message': error_message,
        })

    def _update_last_retrieved(self, conn):
        """
        Update last_retrieved in metadata.sources to today.
        Only called on successful runs — never on failures.
        The 5-year buffer in _get_since_date() ensures retroactive
        revisions are always caught on the next run.
        """
        conn.execute(text("""
            UPDATE metadata.sources
            SET last_retrieved = :today
            WHERE source_id = :source_id
        """), {
            'source_id': self.source_id,
            'today':     date.today(),
        })

    def _get_since_date(self, conn) -> date:
        """
        Read last_retrieved from metadata.sources and subtract
        5 years to get the data window start date.

        WHY 5 YEARS:
            International sources regularly revise historical
            values without announcement. Pulling 5 years back
            ensures any retroactive revision is caught on the
            next run without hitting the source excessively.

        Returns:
            date object. Returns date(1950, 1, 1) on first ever
            run (last_retrieved is NULL) to pull full history.
        """
        row = conn.execute(text("""
            SELECT last_retrieved
            FROM metadata.sources
            WHERE source_id = :source_id
        """), {'source_id': self.source_id}).fetchone()

        last = row[0] if row else None

        if last is None:
            return date(1950, 1, 1)

        return last.replace(year=last.year - 5)


    # ═══════════════════════════════════════════════════════
    # REJECTION LOGGING
    # ═══════════════════════════════════════════════════════

    def _flush_rejections(self, conn):
        """
        Bulk INSERT all accumulated rejection summaries to
        ops.rejection_summary in one round trip.

        WHY BULK INSERT AT RUN END:
            One DB round trip for all rejections across all batches
            regardless of how many batches ran. Far more efficient
            than one INSERT per batch in a loop.
        """
        if not self.all_rejections:
            return

        now  = datetime.utcnow()
        rows = [{**r, 'logged_at': now} for r in self.all_rejections]

        conn.execute(text("""
            INSERT INTO ops.rejection_summary (
                run_id, source_id, batch_unit,
                rejection_reason, row_count, logged_at
            ) VALUES (
                :run_id, :source_id, :batch_unit,
                :rejection_reason, :row_count, :logged_at
            )
        """), rows)

        self.total_rejected = sum(
            r['row_count'] for r in self.all_rejections
        )


    # ═══════════════════════════════════════════════════════
    # ERROR HANDLING
    # ═══════════════════════════════════════════════════════

    def _handle_critical(self, e: CriticalCheckError):
        """
        Handle a CRITICAL quality check failure.
        Logs to ops.pipeline_runs and sends immediate alert.
        last_retrieved is NOT updated — next run retries full window.
        """
        error_text = (
            f"CRITICAL: {e.check_name} failed at {e.stage}.\n"
            f"Batch:    {e.batch_unit}\n"
            f"Expected: {e.expected}\n"
            f"Actual:   {e.actual}\n"
            f"Details:  {e.details}"
        )
        print(f"\n✗ {error_text}")

        # Fresh connection because the original was rolled back.
        with self.engine.connect() as conn2:
            self._close_pipeline_run(conn2, 'failed', error_text)
            conn2.commit()

        send_critical_alert(
            self.source_id, self.run_id,
            error_text, 'ingestion'
        )

    def _handle_unexpected(self, e: Exception):
        """
        Handle an unexpected crash.
        Logs full traceback to ops.pipeline_runs and emails team.
        """
        error_text = (
            f"UNEXPECTED ERROR in {self.source_id} ingestion:\n"
            f"{traceback.format_exc()}"
        )
        print(f"\n✗ {error_text}")

        with self.engine.connect() as conn2:
            self._close_pipeline_run(conn2, 'failed', error_text)
            conn2.commit()

        send_critical_alert(
            self.source_id, self.run_id,
            error_text, 'ingestion'
        )
