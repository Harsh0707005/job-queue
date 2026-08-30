"""Database connection helpers."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
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


_AFTER_COMMIT_ATTR = "_jq_after_commit"


def after_commit(conn: Connection, fn: Callable[[], None]) -> None:
    """Run *fn* after this connection's transaction commits.

    Used so blob files are not unlinked until the catalog delete is
    durable. A rollback leaves both the row and the file. If *conn* is
    not inside :func:`transaction`, *fn* runs immediately.
    """
    hooks = getattr(conn, _AFTER_COMMIT_ATTR, None)
    if hooks is None:
        fn()
        return
    hooks.append(fn)


@contextmanager
def transaction(pool: ConnectionPool) -> Iterator[Connection]:
    """Yield a connection inside a transaction; commit on success, rollback on error."""
    with pool.connection() as conn:
        hooks: list[Callable[[], None]] = []
        setattr(conn, _AFTER_COMMIT_ATTR, hooks)
        try:
            yield conn
            conn.commit()
        except Exception:
            hooks.clear()
            conn.rollback()
            raise
        else:
            # File unlinks after catalog commit. Failures here must not
            # undo the durable delete; pending-GC collects leftovers.
            for fn in hooks:
                try:
                    fn()
                except Exception:
                    pass
            hooks.clear()
