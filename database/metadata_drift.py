"""
database/metadata_drift.py
==========================
Metadata drift detection utility for the ingestion pipeline.

WHAT THIS FILE DOES:
    Compares incoming source metadata against what is currently
    stored in metadata.metrics, metadata.metric_codes, and
    metadata.country_codes. Detects changes (drift) and handles
    them in a controlled way instead of silently overwriting.

WHY DRIFT DETECTION EXISTS:
    The seed scripts use ON CONFLICT DO UPDATE which means if a
    source silently changes a metric's unit or a country's code
    mapping between runs, the stored metadata gets overwritten
    without any record of what changed. This is dangerous:

    - A unit change makes historical observations potentially
      uninterpretable. If GDP per capita was in current USD and
      the source switches to constant USD without announcing it,
      every historical row becomes ambiguous.

    - A code mapping change could silently remap thousands of
      historical rows to the wrong country. If WIPO changes its
      code for Lebanon from 'LB' to 'LBN', rows already in the
      silver layer were inserted under the old mapping. A silent
      update would create an inconsistency between old and new rows.

    Drift detection intercepts these changes before they happen
    and routes them to the appropriate handler.

TWO CATEGORIES OF DRIFT:

    SAFE (auto-applied):
        metric_name, description changes only.
        Editorial changes — renaming a metric or updating its
        description does not affect the interpretability of
        historical values. Applied automatically, logged as
        INFORMATIONAL in ops.quality_runs.

    CRITICAL (pending human review):
        unit, frequency changes for metrics.
        Any code mapping change in country_codes or metric_codes.
        These affect how values should be interpreted or which
        entity a row belongs to. Never auto-applied. Logged as
        CRITICAL in ops.quality_runs, rows parked until a human
        reviews and approves via the pipeline admin interface.

DESIGN PRINCIPLES FOLLOWED:
    - No new architectural concepts — extends existing ops tables
    - Every quality event logged to ops.quality_runs via
      log_check() from quality_checks.py — one consistent writer
    - Human review for anything semantically significant
    - Does not send emails — formats the string, caller sends it
    - Row-by-row inserts matching seed script style — volume is
      tiny (handful of drifted metrics per run at most) so
      bulk insert overhead is not justified here

RELATIONSHIP TO EXISTING CODE:
    - Imports log_check() from quality_checks.py to write to
      ops.quality_runs consistently with the rest of the pipeline
    - Called by base_ingestor.py as a pre-step before each run
    - New metrics discovered here feed into the unknown metric
      flow in BaseIngestor._handle_unknown_metrics()
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text

from database.quality_checks import log_check


# ═══════════════════════════════════════════════════════════════
# CHECK METRIC DRIFT
# ═══════════════════════════════════════════════════════════════
def check_metric_drift(conn, engine, source_id: str,
                       incoming_df: pd.DataFrame) -> tuple:
    """
    Compare incoming metric metadata against stored metadata.
    Identifies safe updates (name/description) and critical
    drifts (unit/frequency) for a given source.

    WHY UNIT AND FREQUENCY ARE CRITICAL:
        Unit: a change from 'current USD' to 'constant USD' makes
        every historical observation ambiguous — was it recorded
        under the old or new unit? Researchers cannot tell without
        knowing the exact date of the change.
        Frequency: changes how data should be aggregated. Annual
        data aggregated as quarterly produces nonsense results.
        Both require human verification before metadata is updated.

    WHY NAME AND DESCRIPTION ARE SAFE:
        Editorial changes only. Renaming 'GDP per capita (current
        US$)' to 'GDP per Capita, Current USD' does not change
        what the values mean. Auto-applied and logged as
        INFORMATIONAL — no human action needed.

    WHY NEW METRICS ARE RETURNED SEPARATELY:
        New metrics (not yet in metadata.metrics) go through the
        existing unknown metric flow in BaseIngestor. They are not
        drift — they are new additions. Mixing them with drift
        would pollute the drift log with first-time discoveries.

    Args:
        conn:        Open SQLAlchemy connection.
        engine:      SQLAlchemy engine for read_sql calls.
        source_id:   Source being checked.
        incoming_df: DataFrame with columns: metric_id,
                     metric_name, unit, frequency, description.

    Returns:
        (safe_updates_df, critical_drifts_df, new_metrics_list)
        safe_updates_df:    DataFrame — metric_id, field_changed,
                            old_value, new_value.
        critical_drifts_df: Same columns.
        new_metrics_list:   List of metric_ids not yet in metadata.
    """
    # ── Load stored metrics for this source ────────────────────
    # One query loads all stored metrics for this source.
    # Built into a dict for O(1) field-level comparison below.
    stored_df = pd.read_sql(text("""
        SELECT metric_id, metric_name, unit, frequency, description
        FROM metadata.metrics
        WHERE source_id = :source_id
    """), conn, params={'source_id': source_id})

    # Dict keyed by metric_id for O(1) lookup during comparison.
    # Alternative (DataFrame merge) would be cleaner for bulk
    # operations but dict lookup is more readable at this scale.
    stored = {
        row['metric_id']: row
        for _, row in stored_df.iterrows()
    }
    known_metric_ids = set(stored.keys())

    safe_updates    = []
    critical_drifts = []
    new_metrics     = []

    for _, incoming in incoming_df.iterrows():
        metric_id = incoming['metric_id']

        # ── New metric — goes through unknown metric flow ──────
        # Not drift — first-time discovery. BaseIngestor handles
        # these via _handle_unknown_metrics() which emails the
        # team with auto-fetched metadata for YES/NO approval.
        if metric_id not in known_metric_ids:
            new_metrics.append(metric_id)
            continue

        stored_row = stored[metric_id]

        # ── Compare safe fields — auto-apply if changed ────────
        # metric_name and description are editorial only.
        # Changes here do not affect value interpretability.
        for field in ['metric_name', 'description']:
            incoming_val = str(incoming.get(field, '') or '').strip()
            stored_val   = str(stored_row.get(field, '') or '').strip()

            # Only flag if both values are non-empty and differ.
            # A source that previously had no description now
            # providing one is enrichment, not drift.
            if incoming_val and stored_val and incoming_val != stored_val:
                safe_updates.append({
                    'metric_id':     metric_id,
                    'field_changed': field,
                    'old_value':     stored_val,
                    'new_value':     incoming_val,
                })

        # ── Compare critical fields — never auto-apply ─────────
        # unit and frequency changes require human review.
        # See module docstring for why these are critical.
        for field in ['unit', 'frequency']:
            incoming_val = str(incoming.get(field, '') or '').strip()
            stored_val   = str(stored_row.get(field, '') or '').strip()

            if incoming_val and stored_val and incoming_val != stored_val:
                critical_drifts.append({
                    'metric_id':     metric_id,
                    'field_changed': field,
                    'old_value':     stored_val,
                    'new_value':     incoming_val,
                })

    # Convert to DataFrames. Empty DataFrames preserve column
    # structure so callers can safely check len() without
    # worrying about missing columns.
    safe_df = pd.DataFrame(safe_updates) if safe_updates else \
        pd.DataFrame(columns=[
            'metric_id', 'field_changed', 'old_value', 'new_value'
        ])

    critical_df = pd.DataFrame(critical_drifts) if critical_drifts else \
        pd.DataFrame(columns=[
            'metric_id', 'field_changed', 'old_value', 'new_value'
        ])

    return safe_df, critical_df, new_metrics


# ═══════════════════════════════════════════════════════════════
# APPLY DRIFT RESULTS
# ═══════════════════════════════════════════════════════════════
def apply_drift_results(conn, run_id: int, source_id: str,
                        safe_updates_df: pd.DataFrame,
                        critical_drifts_df: pd.DataFrame) -> list:
    """
    Apply safe updates immediately and park critical drifts for
    human review. Writes all events to ops.metadata_changes and
    ops.quality_runs.

    WHY SAFE UPDATES ARE AUTO-APPLIED:
        Editorial changes (name, description) carry no risk of
        making historical values ambiguous. Requiring human
        approval for every metric rename would create unnecessary
        operational overhead. The change is logged in
        ops.metadata_changes for full auditability.

    WHY CRITICAL DRIFTS ARE NEVER AUTO-APPLIED:
        A unit or frequency change affects value interpretability.
        The human must confirm that the source genuinely changed
        its reporting convention and decide whether historical
        rows need reprocessing. Until approved, the old metadata
        is preserved — historical rows remain interpretable.

    WHY BOTH GO TO ops.quality_runs:
        Every quality event in the pipeline is logged to
        ops.quality_runs regardless of severity. This gives the
        team a single queryable audit trail covering both
        data quality checks and metadata governance events.

    Args:
        conn:               Open SQLAlchemy connection.
        run_id:             ops.pipeline_runs.run_id for this run.
        source_id:          Source being processed.
        safe_updates_df:    DataFrame from check_metric_drift().
        critical_drifts_df: DataFrame from check_metric_drift().

    Returns:
        List of metric_ids with pending_review status — these
        metrics' rows should be parked by the caller (not upserted)
        until a human approves the drift.
    """
    pending_review_metrics = []
    now = datetime.utcnow()

    # ── Handle safe updates — auto-apply ───────────────────────
    for _, row in safe_updates_df.iterrows():
        metric_id     = row['metric_id']
        field_changed = row['field_changed']
        old_value     = row['old_value']
        new_value     = row['new_value']

        # Apply the update to metadata.metrics.
        # Only update the specific field that changed —
        # not a full row update — to avoid touching fields
        # that are managed by other processes.
        conn.execute(text(f"""
            UPDATE metadata.metrics
            SET {field_changed} = :new_value
            WHERE metric_id = :metric_id
        """), {
            'new_value': new_value,
            'metric_id': metric_id,
        })

        # Log to ops.metadata_changes for audit trail.
        _log_metadata_change(
            conn, run_id, source_id,
            entity_type   = 'metric',
            entity_id     = metric_id,
            field_changed = field_changed,
            old_value     = old_value,
            new_value     = new_value,
            action_taken  = 'auto_applied',
        )

        # Log to ops.quality_runs as INFORMATIONAL.
        # Passed=TRUE because auto-applied changes are expected
        # and handled — not a pipeline problem.
        log_check(
            conn, run_id, source_id,
            stage          = 'ingestion_pre',
            check_name     = 'metadata_consistency',
            severity       = 'INFORMATIONAL',
            passed         = True,
            expected_value = old_value,
            actual_value   = new_value,
            batch_unit     = metric_id,
            details        = (
                f"Auto-applied safe metadata update: "
                f"{field_changed} changed from '{old_value}' "
                f"to '{new_value}' for metric {metric_id}."
            ),
        )

        print(
            f"  ✓ Auto-applied {field_changed} update "
            f"for {metric_id}: '{old_value}' → '{new_value}'"
        )

    # ── Handle critical drifts — park for human review ─────────
    for _, row in critical_drifts_df.iterrows():
        metric_id     = row['metric_id']
        field_changed = row['field_changed']
        old_value     = row['old_value']
        new_value     = row['new_value']

        # Do NOT update metadata.metrics — preserve the stored
        # value until a human approves the change.
        # Log to ops.metadata_changes with pending_review status.
        _log_metadata_change(
            conn, run_id, source_id,
            entity_type   = 'metric',
            entity_id     = metric_id,
            field_changed = field_changed,
            old_value     = old_value,
            new_value     = new_value,
            action_taken  = 'pending_review',
        )

        # Log to ops.quality_runs as CRITICAL.
        # Passed=FALSE because the metadata is inconsistent
        # with the source and human action is required.
        log_check(
            conn, run_id, source_id,
            stage          = 'ingestion_pre',
            check_name     = 'metadata_consistency',
            severity       = 'CRITICAL',
            passed         = False,
            expected_value = old_value,
            actual_value   = new_value,
            batch_unit     = metric_id,
            details        = (
                f"CRITICAL metadata drift detected: "
                f"{field_changed} changed from '{old_value}' "
                f"to '{new_value}' for metric {metric_id}. "
                f"Metadata NOT updated. Rows for this metric "
                f"parked pending human review."
            ),
        )

        pending_review_metrics.append(metric_id)

        print(
            f"  ⚠ Critical drift parked for human review: "
            f"{metric_id} — {field_changed} "
            f"'{old_value}' → '{new_value}'"
        )

    return pending_review_metrics


# ═══════════════════════════════════════════════════════════════
# CHECK CODE MAPPING DRIFT
# ═══════════════════════════════════════════════════════════════
def check_code_mapping_drift(conn, engine, source_id: str,
                             incoming_codes_df: pd.DataFrame,
                             mapping_type: str) -> tuple:
    """
    Compare incoming code mappings against stored mappings in
    metadata.country_codes or metadata.metric_codes.

    WHY ALL CODE MAPPING CHANGES ARE CRITICAL (no safe updates):
        A country code change affects which country every historical
        row for that source maps to. If WIPO changes its code for
        Lebanon from 'LB' to 'LBN', rows already in the silver
        layer were inserted with the old mapping. A silent update
        creates an inconsistency — old rows belong to the old
        mapping, new rows to the new mapping, with no way to tell
        them apart. A human must verify the change is genuine and
        decide whether historical rows need reprocessing.

        Metric code changes carry the same risk — all historical
        rows for that metric are associated with the old code.

        There are no "safe" code mapping changes because all of
        them affect the interpretation of existing silver layer rows.

    Args:
        conn:               Open SQLAlchemy connection.
        engine:             SQLAlchemy engine for read_sql.
        source_id:          Source being checked.
        incoming_codes_df:  DataFrame. For country codes: columns
                            iso3, source_id, code. For metric codes:
                            columns metric_id, source_id, code.
        mapping_type:       'country' or 'metric'.

    Returns:
        (critical_drifts_df, new_codes_list)
        critical_drifts_df: DataFrame — entity_id, field_changed,
                            old_value, new_value.
        new_codes_list:     List of entity_ids with no existing
                            mapping — these are new additions,
                            not drift.
    """
    # ── Load stored codes for this source ──────────────────────
    if mapping_type == 'country':
        table     = 'metadata.country_codes'
        id_col    = 'iso3'
        stored_df = pd.read_sql(text("""
            SELECT iso3 AS entity_id, code
            FROM metadata.country_codes
            WHERE source_id = :source_id
        """), conn, params={'source_id': source_id})
    else:  # metric
        table     = 'metadata.metric_codes'
        id_col    = 'metric_id'
        stored_df = pd.read_sql(text("""
            SELECT metric_id AS entity_id, code
            FROM metadata.metric_codes
            WHERE source_id = :source_id
        """), conn, params={'source_id': source_id})

    # Build dict for O(1) lookup: entity_id → stored code.
    stored = {
        row['entity_id']: row['code']
        for _, row in stored_df.iterrows()
    }
    known_entities = set(stored.keys())

    critical_drifts = []
    new_codes       = []

    for _, incoming in incoming_codes_df.iterrows():
        entity_id    = incoming[id_col]
        incoming_code = str(incoming.get('code', '') or '').strip()

        # New mapping — not previously in metadata.
        # Not drift — a new addition to handle separately.
        if entity_id not in known_entities:
            new_codes.append(entity_id)
            continue

        stored_code = str(stored[entity_id] or '').strip()

        # Any code change is critical — see docstring for why.
        if incoming_code and stored_code and incoming_code != stored_code:
            critical_drifts.append({
                'entity_id':     entity_id,
                'field_changed': 'code',
                'old_value':     stored_code,
                'new_value':     incoming_code,
            })

    critical_df = pd.DataFrame(critical_drifts) if critical_drifts else \
        pd.DataFrame(columns=[
            'entity_id', 'field_changed', 'old_value', 'new_value'
        ])

    return critical_df, new_codes


# ═══════════════════════════════════════════════════════════════
# FORMAT DRIFT EMAIL
# ═══════════════════════════════════════════════════════════════
def format_drift_email(source_id: str,
                       critical_drifts_df: pd.DataFrame,
                       affected_row_counts: dict) -> str:
    """
    Format a CRITICAL alert email body for metadata drift events.
    Does NOT send the email — the caller (subflow) handles sending
    via email_utils.send_critical_alert().

    WHY AFFECTED ROW COUNTS ARE INCLUDED:
        The number of silver layer rows affected by a potential
        code mapping change tells the human reviewer how serious
        the impact is. A drift affecting 50,000 rows of Lebanon
        data requires more careful review than one affecting 3 rows.
        The reviewer can make a more informed decision with this
        context.

    WHY EMAIL FORMATTING IS SEPARATED FROM SENDING:
        Following the design principle from the other conversation:
        metadata_drift.py is a utility module. It does not know
        or care about SMTP credentials, recipient lists, or email
        infrastructure. The caller assembles the full alert using
        this formatted string and sends it via email_utils.py.

    Args:
        source_id:          Source where drift was detected.
        critical_drifts_df: DataFrame of critical drifts.
                            Columns: metric_id (or entity_id),
                            field_changed, old_value, new_value.
        affected_row_counts: Dict mapping metric_id → row count
                             in standardized.observations.

    Returns:
        Formatted string for inclusion in a CRITICAL alert email.
    """
    if critical_drifts_df.empty:
        return ""

    lines = [
        f"CRITICAL METADATA DRIFT DETECTED — {source_id}",
        "=" * 60,
        "",
        "The following metadata changes were detected in the source",
        "but have NOT been applied to the database.",
        "Rows for affected metrics have been parked pending your review.",
        "",
        "Action required: for each item below, run:",
        "  python3 database/add_metric.py --metric_id <id> "
        "--source <source> --action approve",
        "  OR",
        "  python3 database/add_metric.py --metric_id <id> "
        "--source <source> --action reject",
        "",
        "─" * 60,
    ]

    # Identify the entity column — could be metric_id or entity_id
    # depending on which check function produced this DataFrame.
    entity_col = 'metric_id' if 'metric_id' in critical_drifts_df.columns \
        else 'entity_id'

    for _, row in critical_drifts_df.iterrows():
        entity_id     = row[entity_col]
        field_changed = row['field_changed']
        old_value     = row['old_value']
        new_value     = row['new_value']
        row_count     = affected_row_counts.get(entity_id, 0)

        lines.extend([
            f"",
            f"Entity:          {entity_id}",
            f"Field changed:   {field_changed}",
            f"Old value:       {old_value}",
            f"New value:       {new_value}",
            f"Rows affected in silver layer: {row_count:,}",
        ])

    lines.extend([
        "",
        "─" * 60,
        "",
        "Until approved or rejected, the old metadata is preserved",
        "and historical rows remain interpretable.",
    ])

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# PRIVATE HELPER
# ═══════════════════════════════════════════════════════════════
def _log_metadata_change(conn, run_id: int, source_id: str,
                         entity_type: str, entity_id: str,
                         field_changed: str, old_value: str,
                         new_value: str, action_taken: str):
    """
    Insert one row into ops.metadata_changes.
    Called after every drift event — both safe and critical.

    WHY A SEPARATE TABLE FROM ops.quality_runs:
        ops.quality_runs stores pass/fail results for pipeline
        quality checks. ops.metadata_changes stores the specific
        before/after values of metadata fields that changed.
        These serve different purposes:
        - ops.quality_runs answers "did this check pass?"
        - ops.metadata_changes answers "what exactly changed
          and what was done about it?"
        Both tables are written for every drift event to maintain
        the complete audit trail from both perspectives.

    Args:
        conn:         Open SQLAlchemy connection.
        run_id:       ops.pipeline_runs.run_id.
        source_id:    Source where drift was detected.
        entity_type:  'metric', 'country_code', 'metric_code',
                      or 'source'.
        entity_id:    The metric_id, iso3, or source_id affected.
        field_changed: Which field drifted.
        old_value:    Value before the potential change.
        new_value:    Value the source now reports.
        action_taken: 'auto_applied' or 'pending_review'.
    """
    conn.execute(text("""
        INSERT INTO ops.metadata_changes (
            run_id, source_id, entity_type, entity_id,
            field_changed, old_value, new_value,
            detected_at, action_taken
        ) VALUES (
            :run_id, :source_id, :entity_type, :entity_id,
            :field_changed, :old_value, :new_value,
            :detected_at, :action_taken
        )
    """), {
        'run_id':        run_id,
        'source_id':     source_id,
        'entity_type':   entity_type,
        'entity_id':     entity_id,
        'field_changed': field_changed,
        'old_value':     old_value,
        'new_value':     new_value,
        'detected_at':   datetime.utcnow(),
        'action_taken':  action_taken,
    })
