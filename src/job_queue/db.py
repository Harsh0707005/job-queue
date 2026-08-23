"""Database connection helpers."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection
from psycopg.errors import DeadlockDetected, UniqueViolation
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from job_queue.schema import SCHEMA_SQL

# Stable advisory lock key for concurrent schema bootstrap (multi-coordinator).
_SCHEMA_LOCK_KEY = 874_201_339


def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """Create a connection pool configured for dict rows."""
    return ConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row, "autocommit": False},
        open=True,
    )


def init_schema(conn: Connection) -> None:
    """Create job queue tables and indexes if they do not exist.

    Serialized with a session advisory lock so two coordinators booting against
    the same empty database cannot race CREATE TABLE composite types.
    """
    last_exc: Exception | None = None
    for attempt in range(8):
        try:
            conn.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_LOCK_KEY,))
            try:
                conn.execute(SCHEMA_SQL)
                conn.commit()
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_LOCK_KEY,))
                conn.commit()
            return
        except (UniqueViolation, DeadlockDetected) as exc:
            # Concurrent bootstrap that slipped past IF NOT EXISTS, or a
            # live peer already using tables while ALTER TABLE wants
            # AccessExclusiveLock (three coordinators booting at once).
            last_exc = exc
            conn.rollback()
            time.sleep(0.05 * (attempt + 1))
            continue
        except Exception:
            conn.rollback()
            raise
    if last_exc is not None:
        raise last_exc


@contextmanager
def transaction(pool: ConnectionPool) -> Iterator[Connection]:
    """Yield a connection inside a transaction; commit on success, rollback on error."""
    with pool.connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
