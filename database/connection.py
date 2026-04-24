"""
database/connection.py
======================
Returns a SQLAlchemy engine connected to Supabase.

Uses NullPool: each script opens a connection, does its work,
and closes it on exit. No persistent pool between runs.
pool_recycle is intentionally omitted — it has no effect with NullPool.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.pool import NullPool

load_dotenv()


def get_engine():
    """
    Returns a SQLAlchemy engine connected to Supabase.
    Uses NullPool: no persistent connection pool between script runs.

    WHY statement_timeout IS SET TO 0 (no timeout):
        Supabase sets a default statement_timeout on the free tier
        (typically 8 seconds). This is appropriate for a web app
        serving interactive requests, but our pipeline scripts run
        long-running bulk operations — a single seed script may
        commit thousands of rows, and transformation scripts run
        GROUP BY queries over millions of rows. Any of these can
        legitimately take longer than 8 seconds on a cold connection.
        Setting statement_timeout=0 removes the limit for our scripts.
        This is safe because our scripts are not user-facing — they
        run in controlled batch jobs with explicit timeouts at the
        Prefect flow level (timeout_seconds parameter per flow).
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError(
            "DATABASE_URL not set. "
            "Copy .env.example to .env and fill in your credentials."
        )

    engine = create_engine(
        url,

        poolclass=NullPool,
        # NullPool instead of a persistent pool: A persistent pool keeps
        # connections open to Supabase between script runs. That makes sense
        # for a web server with continuous traffic, but pipeline scripts run,
        # do work, and exit. NullPool opens a connection when needed and
        # closes it the moment the script exits — no leaked connections.

        pool_pre_ping=True,
        # Before handing a connection, SQLAlchemy sends SELECT 1 to verify
        # the connection is still alive. Without this, a dropped connection
        # causes a cryptic crash. pool_pre_ping detects it and opens a fresh
        # connection instead. Cost is negligible with NullPool.

        connect_args={
            # TCP keepalives prevent the Session Pooler from dropping
            # connections that are idle during long-running executemany()
            # calls (e.g. upserting 1000 rows takes several seconds with
            # no TCP activity). Without keepalives, Supabase's pooler
            # considers the connection idle and closes it, leaving an
            # 'idle in transaction' zombie on the server side and an
            # SSL connection closed unexpectedly error on the client.
            #
            # keepalives=1:        enable TCP keepalives
            # keepalives_idle=10:  send first keepalive after 10s idle
            # keepalives_interval=5: resend every 5s if no response
            # keepalives_count=3:  drop connection after 3 missed probes
            "keepalives":          1,
            "keepalives_idle":     10,
            "keepalives_interval": 5,
            "keepalives_count":    3,
        },
    )

    # Set statement_timeout=0 (no limit) on every new connection.
    # Uses SQLAlchemy's connect event so it fires automatically for
    # every connection opened by this engine — including connections
    # opened internally by pd.read_sql().
    @event.listens_for(engine, "connect")
    def set_statement_timeout(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("SET statement_timeout = 0")
        # Also disable idle-in-transaction timeout.
        # The Session Pooler enforces an idle_in_transaction_session_timeout
        # separately from statement_timeout. During executemany() for large
        # batches (1000 rows), psycopg2 sends rows in internal sub-batches.
        # Between sub-batches the pooler briefly sees the connection as idle
        # and drops the SSL connection, leaving an 'idle in transaction'
        # zombie on the server side. Setting this to 0 disables that timeout.
        cursor.execute("SET idle_in_transaction_session_timeout = 0")
        cursor.close()

    return engine

# WHY pool_recycle IS OMITTED:
#   pool_recycle discards connections older than N seconds to prevent
#   database-side timeout issues. It only does anything when a persistent
#   pool holds connections across calls. With NullPool there are no
#   persistent connections to recycle — every connection is already
#   discarded on close. Including pool_recycle would be dead configuration.

# CRASH SAFETY:
#   With NullPool a mid-script crash closes the connection cleanly on
#   process exit — no connection leak. Crash safety for data integrity
#   lives in the callers: every ingestion and transformation batch wraps
#   all inserts in one explicit transaction. If the crash happens before
#   commit, the transaction rolls back and the batch retries cleanly on
#   the next run. If it happens after commit but before the checkpoint
#   write, ON CONFLICT DO UPDATE handles the retry without corruption.
#   This discipline is enforced in base_ingestor.py and base_transformer.py.