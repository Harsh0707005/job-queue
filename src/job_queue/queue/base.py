"""JobQueue base domain (BaseMixin)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import os
from pathlib import Path
import tempfile

from job_queue.blob_store import FilesystemBlobStore
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
        blob_dir = (
            os.environ.get("JOB_QUEUE_BLOB_DIR")
            or str(Path(tempfile.gettempdir()) / "job-queue-blobs")
        )
        try:
            read_retries = int(os.environ.get("JOB_QUEUE_BLOB_READ_RETRIES") or "8")
        except (TypeError, ValueError):
            read_retries = 8
        try:
            read_retry_seconds = float(
                os.environ.get("JOB_QUEUE_BLOB_READ_RETRY_SECONDS") or "0.025"
            )
        except (TypeError, ValueError):
            read_retry_seconds = 0.025
        self._blob_store = FilesystemBlobStore(
            blob_dir,
            read_retries=read_retries,
            read_retry_seconds=read_retry_seconds,
        )
        raw_thr = os.environ.get("JOB_QUEUE_PAYLOAD_BLOB_THRESHOLD") or "32768"
        try:
            self.payload_blob_threshold_bytes = int(raw_thr)
        except (TypeError, ValueError):
            self.payload_blob_threshold_bytes = 32768

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
