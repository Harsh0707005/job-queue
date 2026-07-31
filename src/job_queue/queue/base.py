"""JobQueue base domain (BaseMixin)."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import init_schema, transaction
from job_queue.models import (
    AuditConsistencyReport,
    BulkCancelResult,
    Job,
    JobAuditEvent,
    JobProgressEvent,
    JobRetentionSettings,
    JobSideEffect,
    JobStatus,
    PreemptResult,
    RetentionPurgeResult,
    Schedule,
    ScheduleFire,
    SpawnChildrenResult,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
    _ACTIVE_JOB_STATUSES,
    _CANCELABLE_STATUSES,
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _DEDUPE_LOCK_CLASS,
    _DEFAULT_ESTIMATE_SECONDS,
    _DEPS_SATISFIED_SQL,
    _EWMA_ALPHA,
    _JOB_COLUMNS,
    _MAX_LABELS,
    _MAX_LABEL_LEN,
    _MAX_PREEMPTS_PER_JOB,
    _NOTIFY_MAX_BYTES,
    _SCHEDULE_COLUMNS,
    _SIDE_EFFECT_SELECT,
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
    ApprovalError,
    ApprovalRequested,
    CancelError,
    ChildrenAwaited,
    DependencyError,
    LeaseError,
    MergeError,
    RateLimitError,
    RebuildError,
    ReplayError,
    ScheduleError,
    SpawnError,
    _SkipLeaseCandidate,
    _as_utc,
    _normalize_labels,
    _normalize_status_filter,
    _row_to_job,
    _row_to_schedule,
    _utcnow,
    compute_retry_backoff_seconds,
)

class BaseMixin:
    _WEBHOOK_DELIVERY_COLUMNS = _WEBHOOK_DELIVERY_COLUMNS

    _WEBHOOK_ENDPOINT_COLUMNS = _WEBHOOK_ENDPOINT_COLUMNS

    _SIDE_EFFECT_SELECT = _SIDE_EFFECT_SELECT

    def __init__(self, pool: ConnectionPool, *, dsn: str | None = None) -> None:
        self._pool = pool
        self._dsn = dsn


    @classmethod
    def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 10) -> JobQueue:
        """Open a pool, ensure schema exists, return a ready queue."""
        from job_queue.db import create_pool

        pool = create_pool(dsn, min_size=min_size, max_size=max_size)
        with pool.connection() as conn:
            init_schema(conn)
        return cls(pool, dsn=dsn)


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


