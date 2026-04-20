"""
database/base_transformer.py
============================
Abstract base class for all transformation scripts.

WHAT THIS FILE DOES:
    Provides the complete transformation pipeline skeleton that
    every source script inherits. A source script only needs to
    implement these methods:
        parse()          → standardize raw B2 DataFrame into the
                           canonical silver layer shape
        get_batch_units() → return list of batch units
        get_b2_key()     → return B2 path for a batch unit
        deserialize()    → bytes from B2 → DataFrame

    Everything else — reading from B2, quality checks,
    revision detection, temp table upsert, post-upsert
    verification, checkpoint writing, rejection logging,
    ops.pipeline_runs logging, and alert emails — is handled
    here once and inherited by all 7 source scripts.

WHY A BASE CLASS:
    All 7 sources follow the same transformation sequence.
    Writing it once means a new source script is ~30 lines
    not ~400, every source is guaranteed to check and upsert
    identically, and a bug fix applies to all sources at once.

STANDARDIZATION MEANS EXACTLY THREE THINGS:
    From design document Section 7:
    1. Country code mapping — source code → ISO3
    2. Value casting — verify numeric values without changing them
    3. Column mapping — source columns → standard schema columns
    No imputation, rounding, unit conversion, or reformatting.

TEMP TABLE UPSERT PATTERN:
    For each 500-row chunk:
    1. CREATE TEMP TABLE ON COMMIT DROP
    2. INSERT chunk into temp table
    3. INSERT INTO standardized.observations FROM temp table
       ON CONFLICT DO UPDATE
    Temp table is dropped automatically after each chunk by
    ON COMMIT DROP. Safer than direct bulk upsert on partitioned
    tables — a conflict in one chunk does not affect others.

REVISION DETECTION:
    Before every upsert, compare incoming values against stored
    values using a temp table join — safe, no string interpolation.
    Changed values are logged to observation_revisions before
    the upsert overwrites them.

PERFORMANCE DECISIONS:
    - Validation sets (iso3, metric_ids, source_ids) loaded ONCE
      per run and passed to check_pre_upsert() per batch.
    - Revision detection and read-back use temp tables instead
      of raw SQL string interpolation — safe and efficient.
    - Rejections bulk-flushed in one INSERT at run end.

TRANSFORMATION SEQUENCE (per batch unit):
    1.  Open ops.pipeline_runs entry with status='running'
    2.  Load validation sets once
    3.  For each batch unit:
        a. Write checkpoint in_progress
        b. Read B2 file
        c. check_transformation_pre() — checksum vs checkpoint
        d. parse() — standardize to canonical shape
        e. check_pre_upsert() — 4 validation checks
        f. Detect and log revisions via temp table
        g. Temp table upsert in 500-row chunks
        h. Read back from Supabase via temp table
        i. check_transformation_post() — checksum + row count
        j. Write checkpoint complete
    4.  Bulk flush rejections to ops.rejection_summary
    5.  Close ops.pipeline_runs with final status
    6.  Update last_retrieved in metadata.sources
"""

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
    get_ingestion_checksum,
)
from database.quality_checks import (
    CriticalCheckError,
    compute_checksum,
    check_transformation_pre,
    check_transformation_post,
    check_pre_upsert,
)
from database.email_utils import send_critical_alert

# Chunk sizes from design document Section 7.
UPSERT_CHUNK_SIZE  = 500   # rows per temp table upsert
READBACK_CHUNKSIZE = 100   # rows per Supabase read-back batch


class BaseTransformer(ABC):
    """
    Abstract base class for all transformation scripts.
    Subclasses implement the four abstract methods below.
    All shared pipeline logic lives here.
    """

    def __init__(self, source_id: str):
        """
        Initialise shared resources once per script run.

        Args:
            source_id: Must match metadata.sources.source_id exactly.
        """
        self.source_id = source_id

        # One database engine for the entire run.
        self.engine = get_engine()

        # B2 client for reading bronze files.
        self.b2 = B2Client()

        # Set when ops.pipeline_runs entry is opened.
        self.run_id = None

        # Counters written to ops.pipeline_runs at run end.
        self.total_inserted = 0
        self.total_rejected = 0

        # Rejection dicts accumulated across all batches.
        # Flushed to ops.rejection_summary in one bulk INSERT
        # at run end — more efficient than one INSERT per batch.
        self.all_rejections = []


    # ═══════════════════════════════════════════════════════
    # ABSTRACT METHODS — implemented by each source script
    # ═══════════════════════════════════════════════════════

    @abstractmethod
    def parse(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize a raw DataFrame read from B2 into the
        canonical silver layer shape.

        Standardization means exactly:
            1. Map source country codes → ISO3
            2. Verify numeric values cast cleanly (no change)
            3. Map source column names → standard columns

        Must return DataFrame with columns:
            country_iso3, year, period, metric_id,
            value, source_id, retrieved_at
        """

    @abstractmethod
    def get_batch_units(self) -> list:
        """
        Return the ordered list of batch_units to process.
        Must match exactly what the ingestion script used so
        get_ingestion_checksum() can find the correct checkpoint.
        """

    @abstractmethod
    def get_b2_key(self, batch_unit: str) -> str:
        """
        Return the B2 object key for a given batch_unit.
        Must return the same key the ingestion script used —
        transformation reads the same file ingestion wrote.
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> pd.DataFrame:
        """
        Deserialize bytes from B2 into a DataFrame.
        Must be the exact inverse of the ingestion script's
        serialize() so the same file can be read back correctly.
        """


    # ═══════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════════════════

    def run(self):
        """
        Execute the full transformation pipeline for this source.
        Called by the Prefect subflow after ingestion completes.
        """
        with self.engine.connect() as conn:
            try:
                # ── Step 1: Open pipeline run entry ───────────
                # status='running' so a crash leaves a detectable
                # record. Updated to final status at run end.
                self.run_id = self._open_pipeline_run(conn)
                conn.commit()

                # ── Step 2: Load validation sets ONCE ─────────
                # Passed to check_pre_upsert() on every batch.
                # Loading once avoids repeated DB queries per batch.
                valid_iso3       = self._load_valid_iso3(conn)
                valid_metric_ids = self._load_valid_metric_ids(conn)
                valid_source_ids = self._load_valid_source_ids(conn)

                # ── Step 3: Get batch units ────────────────────
                batch_units = self.get_batch_units()

                # ── Step 4: Find already-completed batches ─────
                # Skip batches with a completed transformation
                # checkpoint on restart after a crash.
                completed = get_completed_batches(
                    conn, self.run_id, self.source_id,
                    stage='transformation_batch'
                )

                # ── Step 5: Process each batch unit ───────────
                for batch_unit in batch_units:
                    if batch_unit in completed:
                        print(f"  [skip] {batch_unit} already complete")
                        continue

                    self._process_batch(
                        conn, batch_unit,
                        valid_iso3, valid_metric_ids, valid_source_ids
                    )
                    # Commit after each batch so completed
                    # checkpoints survive a mid-run crash.
                    conn.commit()

                # ── Step 6: Bulk flush rejections ─────────────
                self._flush_rejections(conn)
                conn.commit()

                # ── Step 7: Close pipeline run ─────────────────
                final_status = (
                    'success' if self.total_rejected == 0
                    else 'success_with_rej_rows'
                )
                self._close_pipeline_run(conn, final_status)

                # ── Step 8: Update last_retrieved ─────────────
                # Only on success. Failure leaves last_retrieved
                # unchanged so the next run retries the full window.
                self._update_last_retrieved(conn)
                conn.commit()

                print(
                    f"✓ {self.source_id} transformation complete. "
                    f"Inserted: {self.total_inserted}, "
                    f"Rejected: {self.total_rejected}"
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
                       valid_iso3: set,
                       valid_metric_ids: set,
                       valid_source_ids: set):
        """
        Process one batch unit through the full transformation
        sequence.

        Args:
            batch_unit:      Identifier for this batch.
            valid_iso3:      Known ISO3 codes for FK validation.
            valid_metric_ids: Known metric_ids for FK validation.
            valid_source_ids: Known source_ids for FK validation.
        """
        print(f"  [start] {batch_unit}")

        # ── a. Write in_progress checkpoint ───────────────────
        cp_id = write_start(
            conn, self.run_id, self.source_id,
            stage='transformation_batch',
            batch_unit=batch_unit
        )

        # ── b. Read B2 file ────────────────────────────────────
        # Read the bronze file written by the ingestion script.
        b2_key  = self.get_b2_key(batch_unit)
        df_raw  = self.deserialize(self.b2.download(b2_key))

        # ── c. check_transformation_pre ────────────────────────
        # Compare B2 read checksum against ingestion checkpoint.
        # If they differ, B2 was corrupted between stages.
        # CRITICAL — halt immediately, no retry.
        stored_checksum, stored_row_count = get_ingestion_checksum(
            conn, self.run_id, self.source_id, batch_unit
        )
        check_transformation_pre(
            conn, self.run_id, self.source_id, batch_unit,
            df_raw, stored_checksum, stored_row_count
        )

        # ── d. parse() ─────────────────────────────────────────
        # Source script standardizes df_raw into canonical shape.
        # Exactly: country code mapping, value casting, column
        # mapping. No imputation or value changes.
        df = self.parse(df_raw)

        # ── e. check_pre_upsert ────────────────────────────────
        # Four validation checks before any data touches Supabase.
        # Checks run only on rows that passed earlier checks
        # (chained masks) — avoids redundant work.
        # Returns (clean_df, rejections).
        df, rejections = check_pre_upsert(
            conn, self.run_id, self.source_id, batch_unit,
            df, valid_iso3, valid_metric_ids, valid_source_ids
        )
        self.all_rejections.extend(rejections)

        # ── f. Detect and log revisions ────────────────────────
        # Compare incoming values against stored values using a
        # temp table join. Log changes to observation_revisions
        # before the upsert overwrites them.
        self._detect_revisions(conn, df)

        # ── g. Temp table upsert ───────────────────────────────
        # Upsert clean rows in 500-row chunks.
        self._upsert_chunks(conn, df)

        # ── h. Read back from Supabase ─────────────────────────
        # Verify upserted rows match pre-upsert DataFrame.
        df_readback = self._readback_from_supabase(conn, df)

        # ── i. check_transformation_post ──────────────────────
        # CRITICAL if checksum or row count mismatches.
        check_transformation_post(
            conn, self.run_id, self.source_id, batch_unit,
            df, df_readback
        )

        # ── j. Write complete checkpoint ───────────────────────
        write_complete(
            conn, cp_id,
            checksum  = compute_checksum(df),
            row_count = len(df)
        )

        self.total_inserted += len(df)
        print(f"  [done]  {batch_unit} — {len(df)} rows")


    # ═══════════════════════════════════════════════════════
    # TEMP TABLE UPSERT
    # ═══════════════════════════════════════════════════════

    def _upsert_chunks(self, conn, df: pd.DataFrame):
        """
        Upsert df into standardized.observations in 500-row chunks
        using the temp table pattern.

        WHY TEMP TABLE PATTERN:
            Isolates each chunk before touching the partitioned
            parent. A conflict or error in one chunk does not
            affect rows already upserted from previous chunks.

        WHY ON COMMIT DROP:
            Automatically drops the temp table at transaction
            boundary — no explicit DROP needed and no temp table
            leak even if an error occurs mid-chunk.

        WHY 500-ROW CHUNKS:
            Balances Supabase free tier timeout limits against
            temp table creation overhead. Too small = excessive
            overhead. Too large = timeout risk.
        """
        chunks = [
            df.iloc[i:i + UPSERT_CHUNK_SIZE]
            for i in range(0, len(df), UPSERT_CHUNK_SIZE)
        ]

        for chunk in chunks:
            # Create temp table with same structure as observations.
            # LIMIT 0 copies structure only — no rows.
            # ON COMMIT DROP removes it automatically after chunk.
            conn.execute(text("""
                CREATE TEMP TABLE temp_obs
                ON COMMIT DROP
                AS SELECT * FROM standardized.observations LIMIT 0
            """))

            # Insert chunk into temp table.
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

            # Upsert from temp table into partitioned parent.
            # ON CONFLICT targets the four-part primary key.
            # DO UPDATE applies source revisions to the silver layer.
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

            # Commit triggers ON COMMIT DROP on temp_obs.
            conn.commit()


    # ═══════════════════════════════════════════════════════
    # SUPABASE READ-BACK
    # ═══════════════════════════════════════════════════════

    def _readback_from_supabase(self, conn,
                                df: pd.DataFrame) -> pd.DataFrame:
        """
        Read back upserted rows from Supabase for post-upsert
        verification using a temp table join.

        WHY TEMP TABLE FOR READ-BACK:
            Alternative is building a VALUES clause with hundreds
            of tuples interpolated directly into a SQL string —
            a SQL injection risk and inefficient for large batches.
            A temp table of PK values joined against observations
            is safe, uses the existing indexes, and scales cleanly
            regardless of batch size.

        Args:
            df: Pre-upsert DataFrame. PKs used to filter read-back.

        Returns:
            DataFrame of rows read back from Supabase.
        """
        # Create a temp table of PK values to join against.
        conn.execute(text("""
            CREATE TEMP TABLE temp_pks
            ON COMMIT DROP (
                country_iso3  CHAR(3),
                year          SMALLINT,
                period        TEXT,
                metric_id     TEXT
            )
        """))

        # Insert PKs in READBACK_CHUNKSIZE batches to avoid
        # Supabase timeout on large DataFrames.
        pk_rows = df[
            ['country_iso3', 'year', 'period', 'metric_id']
        ].to_dict(orient='records')

        for i in range(0, len(pk_rows), READBACK_CHUNKSIZE):
            chunk = pk_rows[i:i + READBACK_CHUNKSIZE]
            conn.execute(
                text("""
                    INSERT INTO temp_pks (
                        country_iso3, year, period, metric_id
                    ) VALUES (
                        :country_iso3, :year, :period, :metric_id
                    )
                """),
                chunk
            )

        # Join observations against temp_pks to read back only
        # the rows we just upserted. Uses existing indexes on
        # standardized.observations — no full table scan.
        df_readback = pd.read_sql(text("""
            SELECT
                o.country_iso3, o.year, o.period,
                o.metric_id, o.value
            FROM standardized.observations o
            JOIN temp_pks p
              ON  o.country_iso3 = p.country_iso3
              AND o.year         = p.year
              AND o.period       = p.period
              AND o.metric_id    = p.metric_id
        """), conn)

        return df_readback


    # ═══════════════════════════════════════════════════════
    # REVISION DETECTION
    # ═══════════════════════════════════════════════════════

    def _detect_revisions(self, conn, df: pd.DataFrame):
        """
        Compare incoming values against currently stored values
        using a temp table join. Log any changed values to
        standardized.observation_revisions before the upsert
        overwrites them.

        WHY TEMP TABLE (not raw SQL string):
            Building a WHERE IN clause with hundreds of tuples
            interpolated directly into SQL is a SQL injection
            risk and inefficient for large batches. A temp table
            joined against observations is safe, uses indexes,
            and scales to any batch size.

        WHY BEFORE THE UPSERT:
            The old value must be captured before the upsert
            overwrites it. After the upsert the old value is gone.

        Args:
            df: Clean DataFrame about to be upserted.
        """
        if df.empty:
            return

        # Create temp table of incoming PK + value combinations.
        conn.execute(text("""
            CREATE TEMP TABLE temp_incoming
            ON COMMIT DROP (
                country_iso3  CHAR(3),
                year          SMALLINT,
                period        TEXT,
                metric_id     TEXT,
                new_value     TEXT
            )
        """))

        # Insert incoming rows into temp table in chunks.
        incoming_rows = df[
            ['country_iso3', 'year', 'period', 'metric_id', 'value']
        ].rename(columns={'value': 'new_value'}).to_dict(orient='records')

        for i in range(0, len(incoming_rows), READBACK_CHUNKSIZE):
            chunk = incoming_rows[i:i + READBACK_CHUNKSIZE]
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
                chunk
            )

        # Join observations against temp_incoming to find rows
        # where the value has changed. Only existing rows can
        # have revisions — new rows are not revisions.
        changed = pd.read_sql(text("""
            SELECT
                o.country_iso3, o.year, o.period, o.metric_id,
                o.value     AS old_value,
                t.new_value AS new_value
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

        # Log each changed value to observation_revisions.
        # old_unit and new_unit are NULL here — source scripts
        # can override _detect_revisions() to populate them
        # when a unit change is also detected.
        revision_rows = []
        for _, row in changed.iterrows():
            revision_rows.append({
                'country_iso3':  row['country_iso3'],
                'year':          int(row['year']),
                'period':        row['period'],
                'metric_id':     row['metric_id'],
                'old_value':     row['old_value'],
                'new_value':     row['new_value'],
                'old_unit':      None,
                'new_unit':      None,
                'revised_at':    date.today(),
                'source_id':     self.source_id,
                'revision_note': 'Value revised by source',
            })

        conn.execute(text("""
            INSERT INTO standardized.observation_revisions (
                country_iso3, year, period, metric_id,
                old_value, new_value,
                old_unit, new_unit,
                revised_at, source_id, revision_note
            ) VALUES (
                :country_iso3, :year, :period, :metric_id,
                :old_value, :new_value,
                :old_unit, :new_unit,
                :revised_at, :source_id, :revision_note
            )
        """), revision_rows)

        print(f"    [revisions] {len(changed)} values changed")


    # ═══════════════════════════════════════════════════════
    # VALIDATION SET LOADERS
    # ═══════════════════════════════════════════════════════

    def _load_valid_iso3(self, conn) -> set:
        """Load all known ISO3 codes. Called once per run."""
        rows = conn.execute(text(
            "SELECT iso3 FROM metadata.countries"
        )).fetchall()
        return {r[0] for r in rows}

    def _load_valid_metric_ids(self, conn) -> set:
        """Load all known metric_ids. Called once per run."""
        rows = conn.execute(text(
            "SELECT metric_id FROM metadata.metrics"
        )).fetchall()
        return {r[0] for r in rows}

    def _load_valid_source_ids(self, conn) -> set:
        """Load all known source_ids. Called once per run."""
        rows = conn.execute(text(
            "SELECT source_id FROM metadata.sources"
        )).fetchall()
        return {r[0] for r in rows}


    # ═══════════════════════════════════════════════════════
    # ops.pipeline_runs HELPERS
    # ═══════════════════════════════════════════════════════

    def _open_pipeline_run(self, conn) -> int:
        """
        Insert a row into ops.pipeline_runs with status='running'.
        Returns run_id. A crash leaves status='running' with no
        completed_at — detectable by the team.
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
        """
        conn.execute(text("""
            UPDATE metadata.sources
            SET last_retrieved = :today
            WHERE source_id = :source_id
        """), {
            'source_id': self.source_id,
            'today':     date.today(),
        })


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
            error_text, 'transformation'
        )

    def _handle_unexpected(self, e: Exception):
        """
        Handle an unexpected crash.
        Logs full traceback to ops.pipeline_runs and emails team.
        """
        error_text = (
            f"UNEXPECTED ERROR in {self.source_id} transformation:\n"
            f"{traceback.format_exc()}"
        )
        print(f"\n✗ {error_text}")

        with self.engine.connect() as conn2:
            self._close_pipeline_run(conn2, 'failed', error_text)
            conn2.commit()

        send_critical_alert(
            self.source_id, self.run_id,
            error_text, 'transformation'
        )