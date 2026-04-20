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
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

load_dotenv()


def get_engine():
    """
    Returns a SQLAlchemy engine connected to Supabase.
    Uses NullPool: no persistent connection pool between script runs.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError(
            "DATABASE_URL not set. "
            "Copy .env.example to .env and fill in your credentials."
        )

    return create_engine(
        url,

        poolclass=NullPool,
        #NullPool instead of a persistent pool: A persistent pool keeps connections open to Supabase between script runs and reuses them accross calls. That makes sense for a web server that handles continuous traffic, but in our case, the Pipeline scripts run, do work, and exit, as we process in batches. Without NullPool, the connection remains open with nothing happening, consuming a free-tier limited connection slot for no benefit. NullPool opens a connection when needed and closes it the moment the script exits: clean exit, no leaked connections.

        pool_pre_ping=True,
        # Before handing a connection SQLAlchemy sends a lightweight SELECT 1 to verify the connection is still alive. Without this, if Supabase dropped the connection on its end (timeout, restart, network blip), we get a dead connection and crash with a confusing error. pool_pre_ping detects the dead connection, opens a fresh one. Cost is negligible: one tiny query per connection open run and NullPool means that only happens once per script run anyway.
    )

#WHY pool_recycle is omitted: pool_recycle discards connections older than N seconds to prevent database-side timeout issues. It only does anything when a persistent pool holds connections across calls. With NullPool there are no persistent connections to recycle: every connection is already discarded on close. Including pool_recycle would be dead configuration.

#CRASH SAFETY: With NullPool a mid-script crash closes the connection cleanly on process exit, no connection leak. But crash safety for data integrity lives in the callers, not here. Every ingestion and transformation batch must wrap all its inserts in a single explicit transaction: open connection, insert all rows, commit, write checkpoint, close. If the crash happens before commit, the transaction rolls back automatically and the batch retries cleanly on the next run (the checkpoint was never written, so restart logic treats it as incomplete). If the crash happens after commit but before the checkpoint write, the upsert ON CONFLICT DO UPDATE handles the retry without corruption. This discipline is enforced in base_ingestor.py and base_transformer.py. connection.py provides the connection. The callers own the transaction boundary.