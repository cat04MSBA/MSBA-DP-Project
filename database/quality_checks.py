"""
database/quality_checks.py
==========================
Runs all quality checks defined in the pipeline design.
One function per check stage. All results logged to ops.quality_runs.

RESPONSIBILITY:
    Compute checksums, compare counts, validate rows, and log
    results to ops.quality_runs. Raise CriticalCheckError on
    any CRITICAL failure. Return rejected rows on REJECTION
    failures so the caller can accumulate them in
    all_rejections and continue with valid rows.

    This file does not move data, does not write to B2, and
    does not write checkpoints. Those responsibilities belong
    to b2_upload.py, checkpoint.py, base_ingestor.py, and
    base_transformer.py respectively.

CHECK TYPES AND SEVERITIES (from design document Section 8):
    Stage               Check                Severity
    ──────────────────────────────────────────────────────
    ingestion_pre       row_count_match      CRITICAL
    ingestion_post      checksum_match       CRITICAL
                        row_count_match      CRITICAL
    transformation_pre  checksum_match       CRITICAL
                        row_count_match      CRITICAL
    pre_upsert          year_range           REJECTION
                        empty_string         REJECTION
                        fk_validity          REJECTION
                        pk_uniqueness        REJECTION
    transformation_post checksum_match       CRITICAL
                        row_count_match      CRITICAL
    query               metadata_consistency CRITICAL

CRITICAL vs REJECTION (from design document Section 8):
    CRITICAL  → raise CriticalCheckError immediately. Caller
                halts the pipeline, does not update
                last_retrieved, sends immediate email.
    REJECTION → remove invalid rows, log to rejection summary,
                continue pipeline with valid rows. Never halts.

WHY log_check IS PUBLIC (not _log_check):
    metadata_drift.py also writes quality events to
    ops.quality_runs for metadata drift detections. Making
    log_check() public allows metadata_drift.py to import and
    reuse it — one consistent writer for all quality events
    across the entire pipeline. Duplicating the INSERT SQL
    in metadata_drift.py would mean two places to update if
    ops.quality_runs schema ever changes.

WHY A CUSTOM EXCEPTION FOR CRITICAL:
    base_ingestor.py and base_transformer.py catch CRITICAL
    failures specifically and handle them differently from
    unexpected crashes. A custom exception carries structured
    data (source_id, stage, check_name, expected, actual)
    that goes directly into the alert email and
    ops.pipeline_runs.error_message.

WHY CHECKSUMS ARE SHA256 OF SORTED TUPLES:
    SHA256 on sorted (country_iso3, metric_id, year, value)
    tuples. Sorting before hashing makes the checksum
    order-independent — two identical datasets in different
    row orders produce the same checksum. Without sorting,
    a row order change from the API would trigger a false
    CRITICAL alert.

WHY ROW COUNT AT ingestion_pre INSTEAD OF CHECKSUM:
    At ingestion_pre we compare the raw API response (bytes)
    against the parsed DataFrame. These are different data
    structures and cannot produce comparable checksums. Row
    count comparison catches the most realistic parsing
    failure — rows dropped during parsing. The raw file is
    always saved to B2 for human inspection if needed.

WHY pre_upsert CHECKS ARE PRE-CHECKS AND NOT DB CONSTRAINTS:
    - year_range: without a default partition PostgreSQL raises
      an ungraceful error on out-of-range years.
    - pk_uniqueness: a PK violation on a partitioned table
      crashes the entire batch ungracefully.
    - fk_validity: PostgreSQL does not enforce FK constraints
      on partitioned tables — unknown foreign keys would
      silently insert without this check.
    - empty_string: PostgreSQL does not distinguish NULL from
      empty string — empty strings pass NOT NULL silently.

WHY CHECKS ARE CHAINED IN pre_upsert:
    Each check runs only on rows that passed all earlier
    checks. This avoids redundant validation work on rows
    already marked for rejection. Cheapest checks run first:
    year_range and empty_string (vectorized comparisons),
    then fk_validity, then pk_uniqueness (groupby required).

WHY check_metadata_consistency ACCEPTS metric_source_map:
    The Streamlit app calls this on every researcher query.
    Loading the metric→source mapping inside the function
    adds one DB query per researcher query. The caller loads
    the map once at app startup and passes it to every call.

WHY CONN IS ACCEPTED AS PARAMETER:
    The caller already has an open connection. Opening a
    second connection just for logging wastes a NullPool
    connection slot.
"""

import hashlib
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import text


# ═══════════════════════════════════════════════════════════════
# CUSTOM EXCEPTION FOR CRITICAL FAILURES
# ═══════════════════════════════════════════════════════════════
class CriticalCheckError(Exception):
    """
    Raised when a CRITICAL quality check fails.
    Carries structured data for alert emails and
    ops.pipeline_runs.error_message.

    Attributes:
        source_id:   Which source triggered the failure.
        stage:       Which pipeline stage the check ran at.
        check_name:  Which specific check failed.
        expected:    What the check expected to find.
        actual:      What the check actually found.
        batch_unit:  Which batch this check applies to.
        details:     Human-readable explanation for alert email.
    """
    def __init__(self, source_id, stage, check_name,
                 expected, actual, batch_unit, details):
        self.source_id  = source_id
        self.stage      = stage
        self.check_name = check_name
        self.expected   = expected
        self.actual     = actual
        self.batch_unit = batch_unit
        self.details    = details
        super().__init__(details)


# ═══════════════════════════════════════════════════════════════
# CHECKSUM UTILITY
# ═══════════════════════════════════════════════════════════════
def compute_checksum(df: pd.DataFrame) -> str:
    """
    Compute SHA256 checksum on sorted tuples of
    (country_iso3, metric_id, year, value).

    WHY THESE FOUR COLUMNS:
        These define a unique observation. source_id,
        retrieved_at, and period are pipeline metadata —
        we verify the data values survived transmission
        unchanged, not that metadata is consistent.

    WHY SORTED:
        Order-independent — two identical datasets in different
        row orders produce the same checksum.

    Args:
        df: DataFrame with columns country_iso3, metric_id,
            year, value.

    Returns:
        SHA256 hex digest string.
    """
    tuples = sorted(
        zip(
            df['country_iso3'].astype(str),
            df['metric_id'].astype(str),
            df['year'].astype(str),
            df['value'].astype(str),
        )
    )
    content = json.dumps(tuples, separators=(',', ':'))
    return hashlib.sha256(content.encode()).hexdigest()


# ═══════════════════════════════════════════════════════════════
# LOG HELPER — PUBLIC
# ═══════════════════════════════════════════════════════════════
def log_check(conn, run_id, source_id, stage, check_name,
              severity, passed, expected_value, actual_value,
              batch_unit, details):
    """
    Write one quality check result to ops.quality_runs.
    Called by every check function in this file AND by
    metadata_drift.py for metadata governance events.

    WHY PUBLIC (not _log_check):
        metadata_drift.py also writes quality events to
        ops.quality_runs. Making this function public allows
        metadata_drift.py to import and reuse it — one
        consistent writer for all quality events pipeline-wide.
        See module docstring for full justification.

    Args:
        conn:           Open SQLAlchemy connection.
        run_id:         ops.pipeline_runs.run_id.
        source_id:      Which source this check belongs to.
        stage:          Pipeline stage where check ran.
        check_name:     Which specific check this is.
        severity:       'CRITICAL', 'REJECTION', or 'INFORMATIONAL'.
        passed:         True if check passed, False if failed.
        expected_value: What the check expected.
        actual_value:   What the check actually found.
        batch_unit:     Which batch this check applies to.
        details:        Human-readable explanation.
    """
    conn.execute(text("""
        INSERT INTO ops.quality_runs (
            run_id, source_id, stage, check_name, severity,
            passed, expected_value, actual_value,
            batch_unit, details, checked_at
        ) VALUES (
            :run_id, :source_id, :stage, :check_name, :severity,
            :passed, :expected_value, :actual_value,
            :batch_unit, :details, :checked_at
        )
    """), {
        'run_id':         run_id,
        'source_id':      source_id,
        'stage':          stage,
        'check_name':     check_name,
        'severity':       severity,
        'passed':         passed,
        'expected_value': str(expected_value) if expected_value is not None else None,
        'actual_value':   str(actual_value)   if actual_value   is not None else None,
        'batch_unit':     batch_unit,
        'details':        details,
        'checked_at':     datetime.utcnow(),
    })


# ═══════════════════════════════════════════════════════════════
# STAGE 1 — ingestion_pre
# ═══════════════════════════════════════════════════════════════
def check_ingestion_pre(conn, run_id, source_id, batch_unit,
                        raw_row_count: int, df: pd.DataFrame):
    """
    Verify no rows were dropped when parsing the raw API
    response into a DataFrame. Row count only — raw bytes
    and a DataFrame cannot produce comparable checksums.

    Args:
        raw_row_count: Records in raw API response before parsing.
        df:            Parsed DataFrame.

    Raises:
        CriticalCheckError: if row counts do not match.
    """
    actual = len(df)
    passed = (actual == raw_row_count)

    details = (
        f"Row count match: {actual} rows."
        if passed else
        f"CRITICAL: {raw_row_count - actual} rows lost during "
        f"parsing. Raw: {raw_row_count}, parsed: {actual}. "
        f"Source: {source_id}, batch: {batch_unit}."
    )

    log_check(
        conn, run_id, source_id,
        stage='ingestion_pre', check_name='row_count_match',
        severity='CRITICAL', passed=passed,
        expected_value=raw_row_count, actual_value=actual,
        batch_unit=batch_unit, details=details,
    )

    if not passed:
        raise CriticalCheckError(
            source_id, 'ingestion_pre', 'row_count_match',
            raw_row_count, actual, batch_unit, details,
        )


# ═══════════════════════════════════════════════════════════════
# STAGE 2 — ingestion_post
# ═══════════════════════════════════════════════════════════════
def check_ingestion_post(conn, run_id, source_id, batch_unit,
                         df_before: pd.DataFrame,
                         df_after: pd.DataFrame):
    """
    Verify the DataFrame survived the B2 write without
    corruption or data loss. Checks both checksum and row count.

    Args:
        df_before: DataFrame before writing to B2.
        df_after:  DataFrame read back from B2 after writing.

    Returns:
        (verified_checksum, verified_row_count) on success.

    Raises:
        CriticalCheckError: if checksum or row count mismatch.
    """
    checksum_before = compute_checksum(df_before)
    checksum_after  = compute_checksum(df_after)
    count_before    = len(df_before)
    count_after     = len(df_after)

    # ── checksum ──
    checksum_passed  = (checksum_before == checksum_after)
    details_checksum = (
        f"Checksum match: {checksum_after[:16]}..."
        if checksum_passed else
        f"CRITICAL: Checksum mismatch at ingestion_post. "
        f"Before B2 write: {checksum_before[:16]}... "
        f"B2 read-back: {checksum_after[:16]}... "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='ingestion_post', check_name='checksum_match',
        severity='CRITICAL', passed=checksum_passed,
        expected_value=checksum_before, actual_value=checksum_after,
        batch_unit=batch_unit, details=details_checksum,
    )

    # ── row count ──
    count_passed  = (count_before == count_after)
    details_count = (
        f"Row count match: {count_after} rows."
        if count_passed else
        f"CRITICAL: Row count mismatch at ingestion_post. "
        f"Before: {count_before}, after: {count_after}. "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='ingestion_post', check_name='row_count_match',
        severity='CRITICAL', passed=count_passed,
        expected_value=count_before, actual_value=count_after,
        batch_unit=batch_unit, details=details_count,
    )

    if not checksum_passed:
        raise CriticalCheckError(
            source_id, 'ingestion_post', 'checksum_match',
            checksum_before, checksum_after,
            batch_unit, details_checksum,
        )
    if not count_passed:
        raise CriticalCheckError(
            source_id, 'ingestion_post', 'row_count_match',
            count_before, count_after,
            batch_unit, details_count,
        )

    return checksum_after, count_after


# ═══════════════════════════════════════════════════════════════
# STAGE 3 — transformation_pre
# ═══════════════════════════════════════════════════════════════
def check_transformation_pre(conn, run_id, source_id, batch_unit,
                              df: pd.DataFrame,
                              stored_checksum: str,
                              stored_row_count: int):
    """
    Verify the B2 file was not corrupted between ingestion and
    transformation by comparing against the stored checkpoint.

    WHY NO RETRY:
        If the B2 file checksum changed between ingestion and
        transformation, something is genuinely wrong with the
        storage layer. Retrying reads the same corrupted file.
        Immediate CRITICAL halt is the only correct response.

    Raises:
        CriticalCheckError: if checksum or row count mismatch.
    """
    actual_checksum  = compute_checksum(df)
    actual_row_count = len(df)

    # ── checksum ──
    checksum_passed  = (actual_checksum == stored_checksum)
    details_checksum = (
        f"Checksum match: {actual_checksum[:16]}..."
        if checksum_passed else
        f"CRITICAL: B2 file corrupted between ingestion and "
        f"transformation. Stored: {stored_checksum[:16]}... "
        f"Read: {actual_checksum[:16]}... "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='transformation_pre', check_name='checksum_match',
        severity='CRITICAL', passed=checksum_passed,
        expected_value=stored_checksum, actual_value=actual_checksum,
        batch_unit=batch_unit, details=details_checksum,
    )

    # ── row count ──
    count_passed  = (actual_row_count == stored_row_count)
    details_count = (
        f"Row count match: {actual_row_count} rows."
        if count_passed else
        f"CRITICAL: Row count mismatch at transformation_pre. "
        f"Stored: {stored_row_count}, read: {actual_row_count}. "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='transformation_pre', check_name='row_count_match',
        severity='CRITICAL', passed=count_passed,
        expected_value=stored_row_count, actual_value=actual_row_count,
        batch_unit=batch_unit, details=details_count,
    )

    if not checksum_passed:
        raise CriticalCheckError(
            source_id, 'transformation_pre', 'checksum_match',
            stored_checksum, actual_checksum,
            batch_unit, details_checksum,
        )
    if not count_passed:
        raise CriticalCheckError(
            source_id, 'transformation_pre', 'row_count_match',
            stored_row_count, actual_row_count,
            batch_unit, details_count,
        )


# ═══════════════════════════════════════════════════════════════
# STAGE 4 — pre_upsert
# ═══════════════════════════════════════════════════════════════
def check_pre_upsert(conn, run_id, source_id, batch_unit,
                     df: pd.DataFrame,
                     valid_iso3: set,
                     valid_metric_ids: set,
                     valid_source_ids: set) -> tuple:
    """
    Run all four pre-upsert validation checks on the DataFrame
    before any data touches Supabase.

    WHY CHAINED MASKS:
        Each check runs only on rows that passed all earlier
        checks. Cheapest checks first: year_range and
        empty_string (vectorized comparisons), then fk_validity,
        then pk_uniqueness (groupby required).

    Args:
        df:               DataFrame to validate.
        valid_iso3:       Known ISO3 codes from metadata.countries.
        valid_metric_ids: Known metric_ids from metadata.metrics.
        valid_source_ids: Known source_ids from metadata.sources.

    Returns:
        (clean_df, rejections) where clean_df has invalid rows
        removed and rejections is a list of dicts for
        ops.rejection_summary.
    """
    rejections    = []
    rejected_mask = pd.Series(False, index=df.index)

    # ── 1. year_range ──────────────────────────────────────────
    # Without a default partition PostgreSQL crashes ungracefully
    # on out-of-range years. Pre-checking gives controlled
    # rejection with logging instead of an ungraceful crash.
    year_mask  = ~df['year'].between(1950, 2030)
    year_count = year_mask.sum()
    year_passed = (year_count == 0)

    log_check(
        conn, run_id, source_id,
        stage='pre_upsert', check_name='year_range',
        severity='REJECTION', passed=year_passed,
        expected_value='all years within 1950-2030',
        actual_value=f'{year_count} rows outside range',
        batch_unit=batch_unit,
        details=(
            f"year_range: {year_count} rows rejected."
            if not year_passed else
            "year_range: all rows within 1950-2030."
        ),
    )
    if not year_passed:
        rejected_mask |= year_mask
        rejections.append({
            'run_id': run_id, 'source_id': source_id,
            'batch_unit': batch_unit,
            'rejection_reason': 'year_out_of_range',
            'row_count': int(year_count),
        })

    # ── 2. empty_string ────────────────────────────────────────
    # PostgreSQL does not distinguish NULL from empty string.
    # Empty strings pass NOT NULL silently — reject them here.
    empty_mask  = (df['value'].astype(str).str.strip() == '')
    empty_count = empty_mask.sum()
    empty_passed = (empty_count == 0)

    log_check(
        conn, run_id, source_id,
        stage='pre_upsert', check_name='empty_string',
        severity='REJECTION', passed=empty_passed,
        expected_value='0 empty string values',
        actual_value=f'{empty_count} empty string values',
        batch_unit=batch_unit,
        details=(
            f"empty_string: {empty_count} rows rejected."
            if not empty_passed else
            "empty_string: no empty string values found."
        ),
    )
    if not empty_passed:
        rejected_mask |= empty_mask
        rejections.append({
            'run_id': run_id, 'source_id': source_id,
            'batch_unit': batch_unit,
            'rejection_reason': 'empty_string',
            'row_count': int(empty_count),
        })

    # ── 3. fk_validity ─────────────────────────────────────────
    # Runs only on rows that passed year and empty checks.
    # PostgreSQL does not enforce FK constraints on partitioned
    # tables — this pre-check prevents silent corruption.
    surviving       = df[~rejected_mask].copy()
    unknown_country = ~surviving['country_iso3'].isin(valid_iso3)
    unknown_metric  = ~surviving['metric_id'].isin(valid_metric_ids)
    unknown_source  = ~surviving['source_id'].isin(valid_source_ids)

    fk_mask_surviving = unknown_country | unknown_metric | unknown_source
    fk_mask = pd.Series(False, index=df.index)
    fk_mask.loc[surviving[fk_mask_surviving].index] = True

    unknown_country_count = unknown_country.sum()
    unknown_metric_count  = unknown_metric.sum()
    unknown_source_count  = unknown_source.sum()
    fk_passed = not fk_mask.any()

    log_check(
        conn, run_id, source_id,
        stage='pre_upsert', check_name='fk_validity',
        severity='REJECTION', passed=fk_passed,
        expected_value='0 unknown foreign keys',
        actual_value=(
            f'{unknown_country_count} unknown countries, '
            f'{unknown_metric_count} unknown metrics, '
            f'{unknown_source_count} unknown sources'
        ),
        batch_unit=batch_unit,
        details=(
            f"fk_validity: {unknown_country_count} unknown countries, "
            f"{unknown_metric_count} unknown metrics, "
            f"{unknown_source_count} unknown sources rejected."
            if not fk_passed else
            "fk_validity: all foreign keys valid."
        ),
    )
    if not fk_passed:
        rejected_mask |= fk_mask
        if unknown_country_count > 0:
            rejections.append({
                'run_id': run_id, 'source_id': source_id,
                'batch_unit': batch_unit,
                'rejection_reason': 'unknown_country_code',
                'row_count': int(unknown_country_count),
            })
        if unknown_metric_count > 0:
            rejections.append({
                'run_id': run_id, 'source_id': source_id,
                'batch_unit': batch_unit,
                'rejection_reason': 'unknown_metric_id',
                'row_count': int(unknown_metric_count),
            })

    # ── 4. pk_uniqueness ───────────────────────────────────────
    # Most expensive check — requires groupby. Run last and
    # only on rows that passed all earlier checks.
    # A PK violation on a partitioned table crashes the entire
    # batch ungracefully. Pre-checking rejects only duplicates.
    surviving_clean = df[~rejected_mask].copy()
    duplicate_mask  = surviving_clean.duplicated(
        subset=['country_iso3', 'year', 'period', 'metric_id'],
        keep='first'
    )
    duplicate_count = duplicate_mask.sum()
    pk_passed = (duplicate_count == 0)

    log_check(
        conn, run_id, source_id,
        stage='pre_upsert', check_name='pk_uniqueness',
        severity='REJECTION', passed=pk_passed,
        expected_value='0 duplicate primary keys',
        actual_value=f'{duplicate_count} duplicate rows',
        batch_unit=batch_unit,
        details=(
            f"pk_uniqueness: {duplicate_count} duplicate rows "
            f"rejected. First occurrence kept."
            if not pk_passed else
            "pk_uniqueness: no duplicate primary keys found."
        ),
    )
    if not pk_passed:
        duplicate_indices = surviving_clean[duplicate_mask].index
        rejected_mask.loc[duplicate_indices] = True
        rejections.append({
            'run_id': run_id, 'source_id': source_id,
            'batch_unit': batch_unit,
            'rejection_reason': 'duplicate_pk',
            'row_count': int(duplicate_count),
        })

    clean_df = df[~rejected_mask].copy()
    return clean_df, rejections


# ═══════════════════════════════════════════════════════════════
# STAGE 5 — transformation_post
# ═══════════════════════════════════════════════════════════════
def check_transformation_post(conn, run_id, source_id, batch_unit,
                               df_pre: pd.DataFrame,
                               df_post: pd.DataFrame):
    """
    Verify data survived the Supabase upsert without corruption
    by comparing pre-upsert DataFrame against read-back.

    Raises:
        CriticalCheckError: if checksum or row count mismatch.
    """
    checksum_pre  = compute_checksum(df_pre)
    checksum_post = compute_checksum(df_post)
    count_pre     = len(df_pre)
    count_post    = len(df_post)

    # ── checksum ──
    checksum_passed  = (checksum_pre == checksum_post)
    details_checksum = (
        f"Checksum match: {checksum_post[:16]}..."
        if checksum_passed else
        f"CRITICAL: Checksum mismatch at transformation_post. "
        f"Pre-upsert: {checksum_pre[:16]}... "
        f"Read-back: {checksum_post[:16]}... "
        f"Do not update last_retrieved. "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='transformation_post', check_name='checksum_match',
        severity='CRITICAL', passed=checksum_passed,
        expected_value=checksum_pre, actual_value=checksum_post,
        batch_unit=batch_unit, details=details_checksum,
    )

    # ── row count ──
    count_passed  = (count_pre == count_post)
    details_count = (
        f"Row count match: {count_post} rows."
        if count_passed else
        f"CRITICAL: Row count mismatch at transformation_post. "
        f"Pre-upsert: {count_pre}, read-back: {count_post}. "
        f"Source: {source_id}, batch: {batch_unit}."
    )
    log_check(
        conn, run_id, source_id,
        stage='transformation_post', check_name='row_count_match',
        severity='CRITICAL', passed=count_passed,
        expected_value=count_pre, actual_value=count_post,
        batch_unit=batch_unit, details=details_count,
    )

    if not checksum_passed:
        raise CriticalCheckError(
            source_id, 'transformation_post', 'checksum_match',
            checksum_pre, checksum_post,
            batch_unit, details_checksum,
        )
    if not count_passed:
        raise CriticalCheckError(
            source_id, 'transformation_post', 'row_count_match',
            count_pre, count_post,
            batch_unit, details_count,
        )


# ═══════════════════════════════════════════════════════════════
# STAGE 6 — query
# ═══════════════════════════════════════════════════════════════
def check_metadata_consistency(conn, run_id, source_id,
                                df: pd.DataFrame,
                                metric_source_map: pd.DataFrame):
    """
    Before returning results to the Streamlit researcher,
    verify that every source_id in the result set matches
    the source_id registered for that metric_id in
    metadata.metrics.

    WHY metric_source_map IS PASSED IN:
        The Streamlit app calls this on every researcher query.
        Loading the mapping inside the function adds one DB
        query per researcher query. The caller loads it once
        at app startup and passes it to every call.

    Args:
        df:                Query result DataFrame with columns
                           metric_id and source_id.
        metric_source_map: DataFrame with columns metric_id and
                           registered_source_id.

    Raises:
        CriticalCheckError: if any source_id mismatch found.
    """
    merged   = df.merge(metric_source_map, on='metric_id', how='left')
    mismatch = merged[
        merged['source_id'] != merged['registered_source_id']
    ]
    mismatch_count = len(mismatch)
    passed = (mismatch_count == 0)

    details = (
        "metadata_consistency: all source_ids match metadata."
        if passed else
        f"CRITICAL: {mismatch_count} rows have source_id that "
        f"does not match metadata.metrics. Silver layer may be "
        f"corrupted. Mismatched metric_ids: "
        f"{mismatch['metric_id'].unique().tolist()}"
    )

    log_check(
        conn, run_id, source_id,
        stage='query', check_name='metadata_consistency',
        severity='CRITICAL', passed=passed,
        expected_value='0 source_id mismatches',
        actual_value=f'{mismatch_count} mismatches',
        batch_unit=None, details=details,
    )

    if not passed:
        raise CriticalCheckError(
            source_id, 'query', 'metadata_consistency',
            '0 mismatches', str(mismatch_count), None, details,
        )
