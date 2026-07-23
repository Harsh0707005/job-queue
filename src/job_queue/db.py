"""Database connection helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from job_queue.schema import SCHEMA_SQL


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
    """Create job queue tables and indexes if they do not exist."""
    conn.execute(SCHEMA_SQL)
    conn.commit()


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
