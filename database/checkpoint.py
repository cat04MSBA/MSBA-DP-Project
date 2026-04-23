"""
database/checkpoint.py
======================
Reads and writes pipeline checkpoints to ops.checkpoints.

RESPONSIBILITY:
    Two operations only:
    - Write a checkpoint row (start and complete a batch unit)
    - Read completed checkpoints to determine restart point

    This file does not compute checksums, does not validate
    data, and does not move files. Those responsibilities
    belong to quality_checks.py and b2_upload.py respectively.

HOW CHECKPOINTS WORK (from design document Section 10):
    Every pipeline batch follows this sequence:
        1. write_start()    → inserts row with status='in_progress'
        2. [batch work happens]
        3. write_complete() → updates row to status='complete'
                              with verified checksum and row count

    On restart, get_completed_batches() returns all batch_units
    that reached status='complete' for this run. The caller
    skips those and resumes from the next incomplete unit.

    A batch with status='in_progress' that never reached
    'complete' is a crashed batch. The caller retries it.

WHY TWO SEPARATE CALLS (write_start / write_complete):
    Writing 'in_progress' at batch start means a crash between
    start and complete leaves a detectable record. If we only
    wrote on completion, a crashed batch would leave no trace
    and the restart logic could not distinguish "never started"
    from "started but crashed".

WHY CHECKSUM AND ROW COUNT ARE STORED HERE:
    The checksum stored at ingestion_post stage is read by the
    transformation script at transformation_pre stage to verify
    the B2 file was not corrupted between stages.
    ops.checkpoints is the handoff point between ingestion and
    transformation.

WHY ACCEPTS OPEN CONNECTION:
    The caller already has an open connection. Opening a second
    connection just for checkpoint writes wastes a NullPool
    connection slot. All checkpoint writes happen inside the
    same transaction as the batch work — if the batch rolls
    back, the in_progress checkpoint row rolls back too, which
    is exactly the correct behavior.

RETENTION:
    10 days in Supabase then migrated to B2 by retire_ops.py.
    10 days covers any realistic restart window — if a run is
    not resolved within 10 days, a full re-run is performed.
    Migration (not deletion) preserves the audit trail on B2.
"""

from datetime import datetime
from sqlalchemy import text


# ═══════════════════════════════════════════════════════════════
# WRITE — BATCH START
# ═══════════════════════════════════════════════════════════════
def write_start(conn, run_id: int, source_id: str,
                stage: str, batch_unit: str) -> int:
    """
    Insert a checkpoint row with status='in_progress' at the
    start of a batch. Returns the checkpoint_id so
    write_complete() can update the same row.

    WHY 'in_progress' AT START:
        A crash between write_start() and write_complete()
        leaves a detectable 'in_progress' row. Restart logic
        identifies crashed batches and retries them explicitly.
        Without this, a crashed batch leaves no trace.

    Args:
        conn:       Open SQLAlchemy connection.
        run_id:     ops.pipeline_runs.run_id for this run.
        source_id:  Source being processed.
        stage:      One of: ingestion_batch, ingestion_complete,
                    transformation_batch.
        batch_unit: Human-readable batch identifier.
                    Examples: 'NY.GDP.PCAP.CD_2020' (World Bank),
                    '2020' (IMF/OpenAlex/OECD),
                    '2023' (Oxford), 'full_file' (WIPO/PWT).

    Returns:
        checkpoint_id of the inserted row.
    """
    result = conn.execute(text("""
        INSERT INTO ops.checkpoints (
            run_id, source_id, stage, batch_unit,
            checksum, row_count, status, checkpointed_at
        ) VALUES (
            :run_id, :source_id, :stage, :batch_unit,
            :checksum, :row_count, :status, :checkpointed_at
        )
        RETURNING checkpoint_id
    """), {
        'run_id':          run_id,
        'source_id':       source_id,
        'stage':           stage,
        'batch_unit':      batch_unit,
        'checksum':        '',    # placeholder — set at write_complete()
        'row_count':       0,     # placeholder — set at write_complete()
        'status':          'in_progress',
        'checkpointed_at': datetime.utcnow(),
    })

    return result.fetchone()[0]


# ═══════════════════════════════════════════════════════════════
# WRITE — BATCH COMPLETE
# ═══════════════════════════════════════════════════════════════
def write_complete(conn, checkpoint_id: int,
                   checksum: str, row_count: int) -> None:
    """
    Update an existing checkpoint row from 'in_progress'
    to 'complete' after a batch finishes successfully.

    WHY UPDATE INSTEAD OF INSERT:
        write_start() already inserted the row. Updating the
        same row keeps one row per batch unit per run —
        clean and queryable. Inserting a second row would
        require filtering by status on every read.

    WHY CHECKSUM AND ROW COUNT ARE SET HERE NOT AT START:
        These are the verified values from quality_checks.py —
        specifically the checksum computed at ingestion_post
        or transformation_batch stage. Setting them here
        guarantees only verified checksums are stored. They
        are not known until batch work and checks complete.

    Args:
        conn:          Open SQLAlchemy connection.
        checkpoint_id: ID returned by write_start().
        checksum:      SHA256 checksum verified at this stage.
        row_count:     Row count verified at this stage.
    """
    conn.execute(text("""
        UPDATE ops.checkpoints
        SET
            checksum        = :checksum,
            row_count       = :row_count,
            status          = 'complete',
            checkpointed_at = :checkpointed_at
        WHERE checkpoint_id = :checkpoint_id
    """), {
        'checkpoint_id':   checkpoint_id,
        'checksum':        checksum,
        'row_count':       row_count,
        'checkpointed_at': datetime.utcnow(),
    })


# ═══════════════════════════════════════════════════════════════
# READ — COMPLETED BATCHES FOR RESTART
# ═══════════════════════════════════════════════════════════════
def get_completed_batches(conn, run_id: int,
                          source_id: str, stage: str) -> set:
    """
    Return the set of batch_units that completed successfully
    for this source and stage within the last 10 days.

    Used by restart logic in base_ingestor.py and
    base_transformer.py to determine which batches to skip
    when resuming after a crash.

    WHY A SET:
        The caller does: if batch_unit in completed_batches: skip
        Set lookup is O(1). List lookup is O(n) across potentially
        thousands of batch units per run.

    WHY FILTER BY source_id NOT run_id:
        When a flow times out or crashes, the next run gets a new
        run_id. Filtering by run_id means the new run sees zero
        completed batches and starts from scratch — defeating the
        entire purpose of checkpointing. Filtering by source_id
        instead finds all completed batches for this source within
        the 10-day window, regardless of which run completed them.
        This correctly skips already-done batches even across
        run_id boundaries caused by timeouts or crashes.

        The run_id parameter is kept for API consistency and future
        use (e.g. audit logging) but is not used in the query.

    WHY FILTER TO 10 DAYS:
        Checkpoints older than 10 days are irrelevant for restart.
        Filtering prevents stale checkpoints from a much older
        run accidentally matching a new run's batch_units.

    Args:
        conn:      Open SQLAlchemy connection.
        run_id:    Current run_id (kept for API consistency).
        source_id: Source being processed.
        stage:     Pipeline stage to check.

    Returns:
        Set of batch_unit strings with status='complete'.
    """
    rows = conn.execute(text("""
        SELECT DISTINCT batch_unit
        FROM ops.checkpoints
        WHERE source_id = :source_id
          AND stage     = :stage
          AND status    = 'complete'
          AND checkpointed_at >= NOW() - INTERVAL '10 days'
    """), {
        'source_id': source_id,
        'stage':     stage,
    }).fetchall()

    return {row[0] for row in rows}


# ═══════════════════════════════════════════════════════════════
# READ — STORED CHECKSUM FOR TRANSFORMATION_PRE
# ═══════════════════════════════════════════════════════════════
def get_ingestion_checksum(conn, run_id: int,
                           source_id: str,
                           batch_unit: str) -> tuple:
    """
    Retrieve the checksum and row_count stored at ingestion
    time for a specific batch_unit.

    Called by the transformation script at transformation_pre
    to verify the B2 file was not corrupted between ingestion
    and transformation.

    WHY FILTER BY source_id AND batch_unit (not run_id):
        The transformation script may run in a separate Prefect
        run from the ingestion script — this always happens for
        manual file sources like Oxford, WIPO, and PWT where
        ingestion is triggered by a file upload and transformation
        runs immediately after. Filtering by run_id would miss
        the ingestion checkpoint from a different run_id.
        Using source_id + batch_unit + most recent complete
        checkpoint finds the correct record regardless of whether
        ingestion and transformation share a run_id.

    WHY FILTER TO 10 DAYS:
        Consistent with get_completed_batches(). Prevents a very
        old ingestion checkpoint from a previous run from being
        used for a new transformation run if the batch_unit
        happens to match.

    Args:
        conn:       Open SQLAlchemy connection.
        run_id:     Current transformation run_id (unused in
                    query — kept for API consistency and logging).
        source_id:  Source being processed.
        batch_unit: Batch unit to look up.

    Returns:
        (checksum, row_count) tuple from the ingestion stage.

    Raises:
        ValueError: if no completed ingestion checkpoint found.
                    Caller treats this as CRITICAL — transformation
                    must not proceed without a verified ingestion
                    checkpoint.
    """
    row = conn.execute(text("""
        SELECT checksum, row_count
        FROM ops.checkpoints
        WHERE source_id  = :source_id
          AND batch_unit = :batch_unit
          AND stage      = 'ingestion_batch'
          AND status     = 'complete'
          AND checkpointed_at >= NOW() - INTERVAL '10 days'
        ORDER BY checkpointed_at DESC
        LIMIT 1
    """), {
        'source_id':  source_id,
        'batch_unit': batch_unit,
    }).fetchone()

    if row is None:
        raise ValueError(
            f"No completed ingestion checkpoint found for "
            f"source='{source_id}', batch_unit='{batch_unit}' "
            f"within the last 10 days. "
            f"Transformation cannot proceed without a verified "
            f"ingestion checkpoint. Re-run ingestion first."
        )

    return row[0], row[1]  # checksum, row_count