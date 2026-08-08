"""JobQueue base domain (BaseMixin)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from job_queue.crypto import PayloadCrypto
from job_queue.db import init_schema
from job_queue.queue._common import (
    _SIDE_EFFECT_SELECT,
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
)

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

    from job_queue.queue.core import JobQueue


class BaseMixin:
    _WEBHOOK_DELIVERY_COLUMNS = _WEBHOOK_DELIVERY_COLUMNS

    _WEBHOOK_ENDPOINT_COLUMNS = _WEBHOOK_ENDPOINT_COLUMNS

    _SIDE_EFFECT_SELECT = _SIDE_EFFECT_SELECT

    def __init__(self, pool: ConnectionPool, *, dsn: str | None = None) -> None:
        self._pool = pool
        self._dsn = dsn
        # When True (default), only registered active workers may lease, and
        # only from partitions they own. Unit tests may set False so legacy
        # suites that never join membership still lease freely.
        self.require_worker_registration = True
        self._crypto = PayloadCrypto.from_env()

    @classmethod
    def connect(
        cls,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        require_worker_registration: bool = True,
    ) -> JobQueue:
        """Open a pool, ensure schema exists, return a ready queue."""
        from job_queue.db import create_pool

        pool = create_pool(dsn, min_size=min_size, max_size=max_size)
        with pool.connection() as conn:
            init_schema(conn)
        q = cls(pool, dsn=dsn)
        q.require_worker_registration = bool(require_worker_registration)
        return q

    def close(self) -> None:
        self._pool.close()

    @property
    def dsn(self) -> str:
        if self._dsn:
            return self._dsn
        # psycopg_pool ConnectionPool exposes conninfo
        info = getattr(self._pool, "conninfo", None)
        if info:
            return str(info)
        raise RuntimeError("JobQueue has no DSN; construct via JobQueue.connect")
