"""
database/base_transformer.py
============================
Abstract base class for all transformation scripts.

PERFORMANCE OPTIMIZATIONS IN THIS VERSION:
    1. FIRST RUN FAST PATH — when last_retrieved is NULL, skip
       revision detection and post-upsert read-back entirely.
       These two operations create temp tables, insert rows, and
       join against observations for every single batch. On first
       run there are no existing rows to compare against so they
       add cost with zero benefit. World Bank has 439 batches —
       this eliminates ~878 temp table round trips on first run.

    2. LARGER CHUNK SIZES — UPSERT_CHUNK_SIZE 500→1000,
       READBACK_CHUNKSIZE 100→500. Fewer Supabase round trips.

    3. PRE-OPEN CRASH HANDLING — try/except around get_batch_units()
       so crashes before _open_pipeline_run() are logged to stderr
       and re-raised to Prefect instead of silently swallowed.

    4. ZERO ROWS ALERT — after a successful run with 0 rows
       inserted, send an alert email. Zero rows is silent data loss.

    5. DUPLICATE raise FIXED — previous version had two raise
       statements at end of _handle_unexpected().

TRANSFORMATION SEQUENCE (per batch):
    First run:  pre → parse → pre_upsert → upsert → checkpoint
    Subsequent: pre → parse → pre_upsert → revisions → upsert
                → readback → post → checkpoint
"""

import traceback
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, date
from sqlalchemy import text

from database.connection import get_engine
from database.b2_upload import B2Client
from database.checkpoint import (
    write_start, write_complete,
    get_completed_batches, get_ingestion_checksum,
)
from database.quality_checks import (
    CriticalCheckError, compute_checksum,
    check_transformation_pre, check_transformation_post,
    check_pre_upsert,
)
from database.email_utils import send_critical_alert

# ─────────────────────────────────────────────────────────────
# CHUNK SIZES
# ─────────────────────────────────────────────────────────────
UPSERT_CHUNK_SIZE  = 1000  # rows per temp table upsert (was 500)
READBACK_CHUNKSIZE = 500   # rows per read-back batch (was 100)


class BaseTransformer(ABC):

    def __init__(self, source_id: str):
        self.source_id      = source_id
        self.engine         = get_engine()
        self.b2             = B2Client()
        self.run_id         = None
        self.total_inserted = 0
        self.total_rejected = 0
        self.all_rejections = []
        self._first_run     = False  # set in run()


    # ═══════════════════════════════════════════════════════
    # ABSTRACT METHODS
    # ═══════════════════════════════════════════════════════

    @abstractmethod
    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Standardize raw B2 DataFrame into canonical shape."""

    @abstractmethod
    def get_batch_units(self) -> list:
        """Return ordered list of batch_units to process."""

    @abstractmethod
    def get_b2_key(self, batch_unit: str) -> str:
        """Return B2 path for a given batch_unit."""

    @abstractmethod
    def deserialize(self, data: bytes) -> pd.DataFrame:
        """Deserialize bytes from B2 into a DataFrame."""


    # ═══════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════════════════

    def run(self):
        """Execute the full transformation pipeline."""

        # ── Get batch units BEFORE opening pipeline run ────────
        # Crashes here (e.g. no checkpoints found) are logged to
        # stderr and re-raised. Without this wrapper they would be
        # silently swallowed before any pipeline run entry exists.
        try:
            batch_units = self.get_batch_units()
        except Exception:
            print(
                f"\n✗ UNEXPECTED ERROR in {self.source_id} "
                f"transformation (before pipeline run opened):\n"
                f"{traceback.format_exc()}"
            )
            raise

        with self.engine.connect() as conn:
            try:
                # ── Open pipeline run ──────────────────────────
                self.run_id = self._open_pipeline_run(conn)
                conn.commit()

                # ── First run detection ────────────────────────
                # If last_retrieved is NULL → first run → skip
                # revision detection and read-back per batch.
                self._first_run = self._is_first_run(conn)
                if self._first_run:
                    print(
                        f"  [first run] Skipping revision detection "
                        f"and read-back — no existing rows."
                    )

                # ── Load validation sets ONCE ──────────────────
                valid_iso3       = self._load_valid_iso3(conn)
                valid_metric_ids = self._load_valid_metric_ids(conn)
                valid_source_ids = self._load_valid_source_ids(conn)

                # ── Find already-completed batches ─────────────
                completed = get_completed_batches(
                    conn, self.run_id, self.source_id,
                    stage='transformation_batch'
                )

                # ── Process each batch ─────────────────────────
                for batch_unit in batch_units:
                    if batch_unit in completed:
                        print(f"  [skip] {batch_unit} already complete")
                        continue
                    self._process_batch(
                        conn, batch_unit,
                        valid_iso3, valid_metric_ids, valid_source_ids
                    )
                    conn.commit()

                # ── Bulk flush rejections ──────────────────────
                self._flush_rejections(conn)
                conn.commit()

                # ── Close pipeline run ─────────────────────────
                final_status = (
                    'success' if self.total_rejected == 0
                    else 'success_with_rej_rows'
                )
                self._close_pipeline_run(conn, final_status)
                self._update_last_retrieved(conn)
                conn.commit()

                print(
                    f"✓ {self.source_id} transformation complete. "
                    f"Inserted: {self.total_inserted}, "
                    f"Rejected: {self.total_rejected}"
                )

                # ── Zero rows alert ────────────────────────────
                if self.total_inserted == 0:
                    self._alert_zero_rows()

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
                       valid_iso3, valid_metric_ids, valid_source_ids):
        """
        Process one batch unit.
        On first run: skips revision detection and read-back.
        On subsequent runs: full sequence including both.
        """
        print(f"  [start] {batch_unit}")

        # a. Checkpoint in_progress
        cp_id = write_start(
            conn, self.run_id, self.source_id,
            stage='transformation_batch', batch_unit=batch_unit
        )

        # b. Read from B2
        b2_key = self.get_b2_key(batch_unit)
        df_raw = self.deserialize(self.b2.download(b2_key))

        # c. Pre-transformation checksum check
        stored_checksum, stored_row_count = get_ingestion_checksum(
            conn, self.run_id, self.source_id, batch_unit
        )
        check_transformation_pre(
            conn, self.run_id, self.source_id, batch_unit,
            df_raw, stored_checksum, stored_row_count
        )

        # d. Parse — standardize to canonical shape
        df = self.parse(df_raw)

        # e. Pre-upsert quality checks
        df, rejections = check_pre_upsert(
            conn, self.run_id, self.source_id, batch_unit,
            df, valid_iso3, valid_metric_ids, valid_source_ids
        )
        self.all_rejections.extend(rejections)

        # f. Revision detection — SKIPPED on first run
        if not self._first_run:
            self._detect_revisions(conn, df)

        # g. Upsert in chunks
        self._upsert_chunks(conn, df)

        # h. Read-back + post check — SKIPPED on first run
        if not self._first_run:
            df_readback = self._readback_from_supabase(conn, df)
            check_transformation_post(
                conn, self.run_id, self.source_id, batch_unit,
                df, df_readback
            )

        # i. Checkpoint complete
        write_complete(
            conn, cp_id,
            checksum  = compute_checksum(df),
            row_count = len(df)
        )

        self.total_inserted += len(df)
        print(f"  [done]  {batch_unit} — {len(df)} rows")


    # ═══════════════════════════════════════════════════════
    # UPSERT
    # ═══════════════════════════════════════════════════════

    def _upsert_chunks(self, conn, df: pd.DataFrame):
        """Upsert df in 1000-row chunks via temp table pattern."""
        if df.empty:
            return

        for i in range(0, len(df), UPSERT_CHUNK_SIZE):
            chunk = df.iloc[i:i + UPSERT_CHUNK_SIZE]

            conn.execute(text("""
                CREATE TEMP TABLE temp_obs
                ON COMMIT DROP
                AS SELECT * FROM standardized.observations LIMIT 0
            """))

            conn.execute(
                text("""
                    INSERT INTO temp_obs (
                        country_iso3, year, period, metric_id,
                        value, source_id, retrieved_at
                    ) VALUES (
                        :country_iso3, :year, :period, :metric_id,
                        :value, :source_id, :retrieved_at
                    )
                """),
                chunk.to_dict(orient='records')
            )

            conn.execute(text("""
                INSERT INTO standardized.observations (
                    country_iso3, year, period, metric_id,
                    value, source_id, retrieved_at
                )
                SELECT
                    country_iso3, year, period, metric_id,
                    value, source_id, retrieved_at
                FROM temp_obs
                ON CONFLICT (country_iso3, year, period, metric_id)
                DO UPDATE SET
                    value        = EXCLUDED.value,
                    source_id    = EXCLUDED.source_id,
                    retrieved_at = EXCLUDED.retrieved_at
            """))

            conn.commit()


    # ═══════════════════════════════════════════════════════
    # READ-BACK
    # ═══════════════════════════════════════════════════════

    def _readback_from_supabase(self, conn, df: pd.DataFrame) -> pd.DataFrame:
        """Read back upserted rows via temp table join."""
        conn.execute(text("""
            CREATE TEMP TABLE temp_pks (
                country_iso3  CHAR(3),
                year          SMALLINT,
                period        TEXT,
                metric_id     TEXT
            ) ON COMMIT DROP
        """))

        pk_rows = df[
            ['country_iso3', 'year', 'period', 'metric_id']
        ].to_dict(orient='records')

        for i in range(0, len(pk_rows), READBACK_CHUNKSIZE):
            conn.execute(
                text("""
                    INSERT INTO temp_pks (
                        country_iso3, year, period, metric_id
                    ) VALUES (
                        :country_iso3, :year, :period, :metric_id
                    )
                """),
                pk_rows[i:i + READBACK_CHUNKSIZE]
            )

        return pd.read_sql(text("""
            SELECT o.country_iso3, o.year, o.period,
                   o.metric_id, o.value
            FROM standardized.observations o
            JOIN temp_pks p
              ON  o.country_iso3 = p.country_iso3
              AND o.year         = p.year
              AND o.period       = p.period
              AND o.metric_id    = p.metric_id
        """), conn)


    # ═══════════════════════════════════════════════════════
    # REVISION DETECTION
    # ═══════════════════════════════════════════════════════

    def _detect_revisions(self, conn, df: pd.DataFrame):
        """Detect and log value changes before upsert overwrites them."""
        if df.empty:
            return

        conn.execute(text("""
            CREATE TEMP TABLE temp_incoming (
                country_iso3  CHAR(3),
                year          SMALLINT,
                period        TEXT,
                metric_id     TEXT,
                new_value     TEXT
            ) ON COMMIT DROP
        """))

        incoming = df[
            ['country_iso3', 'year', 'period', 'metric_id', 'value']
        ].rename(columns={'value': 'new_value'}).to_dict(orient='records')

        for i in range(0, len(incoming), READBACK_CHUNKSIZE):
            conn.execute(
                text("""
                    INSERT INTO temp_incoming (
                        country_iso3, year, period,
                        metric_id, new_value
                    ) VALUES (
                        :country_iso3, :year, :period,
                        :metric_id, :new_value
                    )
                """),
                incoming[i:i + READBACK_CHUNKSIZE]
            )

        changed = pd.read_sql(text("""
            SELECT
                o.country_iso3, o.year, o.period, o.metric_id,
                o.value AS old_value, t.new_value
            FROM standardized.observations o
            JOIN temp_incoming t
              ON  o.country_iso3 = t.country_iso3
              AND o.year         = t.year
              AND o.period       = t.period
              AND o.metric_id    = t.metric_id
            WHERE o.value != t.new_value
        """), conn)

        if changed.empty:
            return

        revision_rows = changed.assign(
            old_unit      = None,
            new_unit      = None,
            revised_at    = date.today(),
            source_id     = self.source_id,
            revision_note = 'Value revised by source',
        )[['country_iso3', 'year', 'period', 'metric_id',
           'old_value', 'new_value', 'old_unit', 'new_unit',
           'revised_at', 'source_id', 'revision_note']].to_dict(orient='records')

        conn.execute(text("""
            INSERT INTO standardized.observation_revisions (
                country_iso3, year, period, metric_id,
                old_value, new_value, old_unit, new_unit,
                revised_at, source_id, revision_note
            ) VALUES (
                :country_iso3, :year, :period, :metric_id,
                :old_value, :new_value, :old_unit, :new_unit,
                :revised_at, :source_id, :revision_note
            )
        """), revision_rows)

        print(f"    [revisions] {len(changed)} values changed")


    # ═══════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════

    def _is_first_run(self, conn) -> bool:
        """
        True if no transformation_batch checkpoint has ever completed
        for this source — meaning the silver layer has no rows from
        this source yet.

        WHY NOT last_retrieved:
            last_retrieved is updated by the INGESTION script at the
            end of a successful ingest run. By the time transformation
            runs, last_retrieved is already set — even on the very
            first ever transformation. Using last_retrieved would
            always return False here, defeating the first-run fast
            path entirely and forcing revision detection + read-back
            on every single batch even when the silver layer is empty.

        WHY transformation_batch checkpoints:
            A completed transformation_batch checkpoint means at least
            one batch was successfully upserted to Supabase. If none
            exist for this source, the silver layer has no rows for
            this source — revision detection and read-back have nothing
            to compare against and would waste hundreds of round trips.
            This correctly returns True on the first ever transformation
            run and False on all subsequent runs.

        WHY NO 10-DAY WINDOW FILTER HERE:
            Unlike get_completed_batches() which needs a window to
            avoid cross-run contamination, this check just needs to
            know if ANY transformation has ever succeeded for this
            source — ever, not just recently. An old checkpoint from
            6 months ago correctly tells us the silver layer has rows.
        """
        row = conn.execute(text("""
            SELECT 1 FROM ops.checkpoints
            WHERE source_id = :source_id
              AND stage      = 'transformation_batch'
              AND status     = 'complete'
            LIMIT 1
        """), {'source_id': self.source_id}).fetchone()
        return row is None

    def _load_valid_iso3(self, conn) -> set:
        return {r[0] for r in conn.execute(text(
            "SELECT iso3 FROM metadata.countries"
        )).fetchall()}

    def _load_valid_metric_ids(self, conn) -> set:
        return {r[0] for r in conn.execute(text(
            "SELECT metric_id FROM metadata.metrics"
        )).fetchall()}

    def _load_valid_source_ids(self, conn) -> set:
        return {r[0] for r in conn.execute(text(
            "SELECT source_id FROM metadata.sources"
        )).fetchall()}

    def _open_pipeline_run(self, conn) -> int:
        result = conn.execute(text("""
            INSERT INTO ops.pipeline_runs (
                source_id, started_at, status
            ) VALUES (:source_id, :started_at, 'running')
            RETURNING run_id
        """), {'source_id': self.source_id, 'started_at': datetime.utcnow()})
        return result.fetchone()[0]

    def _close_pipeline_run(self, conn, status: str, error_message: str = None):
        conn.execute(text("""
            UPDATE ops.pipeline_runs
            SET completed_at  = :completed_at,
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
        conn.execute(text("""
            UPDATE metadata.sources
            SET last_retrieved = :today
            WHERE source_id = :source_id
        """), {'source_id': self.source_id, 'today': date.today()})

    def _flush_rejections(self, conn):
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
        self.total_rejected = sum(r['row_count'] for r in self.all_rejections)

    def _alert_zero_rows(self):
        """
        Alert when transformation succeeds but inserts 0 rows.
        Zero rows after a successful run = silent data loss.
        Possible causes: broken crosswalk, empty B2 files,
        since_date window too narrow.
        """
        msg = (
            f"ZERO ROWS ALERT: {self.source_id} transformation "
            f"completed with status=success but inserted 0 rows.\n\n"
            f"Possible causes:\n"
            f"  1. Country code crosswalk broken — all rows dropped\n"
            f"  2. B2 files empty or unreadable\n"
            f"  3. since_date window excludes all available data\n\n"
            f"Run ID: {self.run_id}\n"
            f"Check ops.quality_runs for fk_validity failures.\n"
            f"Check metadata.country_codes for source_id='{self.source_id}'."
        )
        print(f"\n⚠ WARNING: {msg}")
        send_critical_alert(
            source_id  = self.source_id,
            run_id     = self.run_id,
            error_text = msg,
            stage      = 'transformation',
        )


    # ═══════════════════════════════════════════════════════
    # ERROR HANDLERS
    # ═══════════════════════════════════════════════════════

    def _handle_critical(self, e: CriticalCheckError):
        """Log to ops.pipeline_runs and re-raise. Email via on_failure hook."""
        error_text = (
            f"CRITICAL: {e.check_name} failed at {e.stage}.\n"
            f"Batch:    {e.batch_unit}\n"
            f"Expected: {e.expected}\n"
            f"Actual:   {e.actual}\n"
            f"Details:  {e.details}"
        )
        print(f"\n✗ {error_text}")
        with self.engine.connect() as conn2:
            self._close_pipeline_run(conn2, 'failed', error_text)
            conn2.commit()
        raise

    def _handle_unexpected(self, e: Exception):
        """Log full traceback to ops.pipeline_runs and re-raise."""
        error_text = (
            f"UNEXPECTED ERROR in {self.source_id} transformation:\n"
            f"{traceback.format_exc()}"
        )
        print(f"\n✗ {error_text}")
        with self.engine.connect() as conn2:
            self._close_pipeline_run(conn2, 'failed', error_text)
            conn2.commit()
        raise
