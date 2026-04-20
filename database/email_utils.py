"""
database/email_utils.py
=======================
Shared email utilities for the entire pipeline.

WHAT THIS FILE DOES:
    Provides one function to send emails via Gmail SMTP and
    two pre-built alert templates used by both base_ingestor.py
    and base_transformer.py.

WHY A SEPARATE FILE:
    base_ingestor.py and base_transformer.py both need identical
    email logic. Keeping it here means any fix or change to
    email sending applies to the entire pipeline at once.
    Duplicating it in both base classes would mean maintaining
    two copies of the same code.

WHY GMAIL SMTP:
    Zero extra dependency. Works with any Google account using
    an App Password. The pipeline sends at most a few emails
    per month — no need for a paid third-party email service.

WHY MULTIPLE RECIPIENTS VIA COMMA-SEPARATED ENV VAR:
    Storing all recipients in one env var keeps configuration
    simple. The function splits on commas at send time so
    adding or removing a recipient is a one-line .env change
    with no code changes.

WHY EMAIL FAILURE NEVER MASKS THE ORIGINAL ERROR:
    If SMTP credentials are missing or the Gmail server is
    unreachable, the pipeline prints the failure to stdout
    but does not re-raise. The original pipeline error is
    always more important than the alert delivery failure.
    The run is already logged in ops.pipeline_runs regardless
    of whether the email delivered.

ENV VARS REQUIRED:
    SMTP_SENDER     Gmail address that sends alerts
    SMTP_PASSWORD   Gmail App Password (not account password)
                    Enable at: Google Account → Security →
                    2-Step Verification → App Passwords
    SMTP_RECIPIENT  Comma-separated list of recipient addresses.
                    Recipients can be any valid email address —
                    Gmail, university, Outlook, etc.
                    Example: a@gmail.com,b@aub.edu.lb,c@outlook.com
"""

import os
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()


def send_email(subject: str, body: str) -> None:
    """
    Send a plain text email via Gmail SMTP to all recipients
    listed in SMTP_RECIPIENT.

    If SMTP env vars are not configured, prints the email
    content to stdout instead of raising an error. This
    ensures missing email config never masks a pipeline error.

    Args:
        subject: Email subject line.
        body:    Plain text email body.
    """
    sender    = os.getenv("SMTP_SENDER")
    password  = os.getenv("SMTP_PASSWORD")
    recipient = os.getenv("SMTP_RECIPIENT")

    # If any SMTP env var is missing, print to stdout only.
    # This prevents a missing env var from masking the original
    # pipeline error during development or first-time setup.
    if not all([sender, password, recipient]):
        print(
            f"\n[EMAIL NOT SENT — SMTP not configured]\n"
            f"Subject: {subject}\n"
            f"{'─' * 60}\n"
            f"{body}\n"
            f"{'─' * 60}\n"
        )
        return

    # Split comma-separated recipients into a list.
    # Strips whitespace so 'a@x.com, b@x.com' works cleanly.
    recipients = [r.strip() for r in recipient.split(',')]

    msg            = MIMEText(body)
    msg['Subject'] = subject
    msg['From']    = sender
    msg['To']      = ', '.join(recipients)

    try:
        # Port 587 with STARTTLS is Gmail's standard SMTP method.
        # TLS encrypts credentials and email content in transit.
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.ehlo()
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
    except Exception as e:
        # Email failure must never mask the original error.
        # Log to stdout and continue — the run is already
        # logged in ops.pipeline_runs regardless.
        print(f"[EMAIL FAILED: {e}]")


def send_critical_alert(source_id: str, run_id: int,
                        error_text: str,
                        pipeline_stage: str) -> None:
    """
    Send an immediate CRITICAL alert email to the team.
    Called by base_ingestor.py and base_transformer.py when
    a CRITICAL quality check fails or an unexpected crash occurs.

    last_retrieved is NOT updated on CRITICAL failures so the
    team knows the next scheduled run will retry the full window.

    Args:
        source_id:      Which source failed.
        run_id:         ops.pipeline_runs.run_id for this run.
        error_text:     Full error message or traceback.
        pipeline_stage: 'ingestion' or 'transformation'.
    """
    send_email(
        subject=(
            f"[CRITICAL] {source_id} {pipeline_stage} failed "
            f"— action required"
        ),
        body=(
            f"A CRITICAL failure occurred in the {source_id} "
            f"{pipeline_stage} pipeline.\n\n"
            f"last_retrieved has NOT been updated.\n"
            f"The next scheduled run will retry the full window.\n\n"
            f"{'─' * 60}\n"
            f"{error_text}\n"
            f"{'─' * 60}\n\n"
            f"Check ops.pipeline_runs and ops.quality_runs "
            f"for full details.\n"
            f"Run ID: {run_id}"
        )
    )


def send_unknown_metric_alert(source_id: str, run_id: int,
                               metric_id: str, metadata: dict,
                               row_count: int,
                               batch_unit: str) -> None:
    """
    Email the team when an unknown metric is detected during
    ingestion. Includes auto-fetched metadata and instructions
    to register or skip the metric.

    The pipeline continues without waiting for a response.
    Rows for the unknown metric are parked in memory until
    the team runs add_metric.py with --action yes or no.

    WHY THE PIPELINE CONTINUES WITHOUT WAITING:
        The pipeline should never be blocked by a new metric
        that a source added without announcement. The rows
        are parked safely and the team decides asynchronously.
        All other rows in the batch proceed normally.

    Args:
        source_id:  Which source detected the unknown metric.
        run_id:     ops.pipeline_runs.run_id for this run.
        metric_id:  The unknown metric identifier.
        metadata:   Dict with metric_name, unit, category,
                    description, frequency — auto-fetched from
                    the source API by fetch_metric_metadata().
        row_count:  Number of rows parked for this metric.
        batch_unit: Which batch the unknown metric appeared in.
    """
    send_email(
        subject=(
            f"[ACTION REQUIRED] New metric detected: "
            f"{metric_id} ({source_id})"
        ),
        body=(
            f"A new metric was detected in {source_id} that is "
            f"not registered in metadata.metrics.\n\n"
            f"Metric ID:   {metric_id}\n"
            f"Name:        {metadata.get('metric_name', 'unknown')}\n"
            f"Unit:        {metadata.get('unit', 'unknown')}\n"
            f"Category:    {metadata.get('category', 'unknown')}\n"
            f"Frequency:   {metadata.get('frequency', 'annual')}\n\n"
            f"Description:\n{metadata.get('description', 'none')}\n\n"
            f"Affected rows: {row_count} (batch: {batch_unit})\n\n"
            f"{'─' * 60}\n"
            f"To REGISTER this metric and ingest the parked rows:\n"
            f"python3 database/add_metric.py "
            f"--metric_id {metric_id} "
            f"--source {source_id} "
            f"--action yes\n\n"
            f"To PERMANENTLY SKIP this metric:\n"
            f"python3 database/add_metric.py "
            f"--metric_id {metric_id} "
            f"--source {source_id} "
            f"--action no\n"
            f"{'─' * 60}\n\n"
            f"The pipeline has continued with all other rows.\n"
            f"Run ID: {run_id}"
        )
    )
