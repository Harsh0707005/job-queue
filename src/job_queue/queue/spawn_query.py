"""JobQueue list children and await timeouts."""
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


class SpawnQueryMixin:
    def list_children(
        self,
        parent_id: UUID,
        *,
        tenant_id: str,
        limit: int = 500,
    ) -> list[Job]:
        """List jobs spawned with ``parent_job_id = parent_id`` for *tenant_id*."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with transaction(self._pool) as conn:
            # Authorize: parent must belong to tenant (or any child).
            parent = conn.execute(
                "SELECT id FROM jobs WHERE id = %s AND tenant_id = %s",
                (parent_id, tenant_id),
            ).fetchone()
            if parent is None:
                # Still allow if children exist under tenant (parent deleted).
                kids = self._list_children_locked(
                    conn, parent_id=parent_id, tenant_id=tenant_id, limit=limit
                )
                if not kids:
                    raise ValueError(
                        f"job {parent_id} not found for tenant {tenant_id!r}"
                    )
                return kids
            return self._list_children_locked(
                conn, parent_id=parent_id, tenant_id=tenant_id, limit=limit
            )

    def process_await_children_timeouts(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """Cancel parents parked past ``await_children_deadline_at``.

        Safe to call from a scheduler loop. The normal ``lease`` /
        ``lease_batch`` path also reaps these opportunistically (same as
        approval timeouts), so an idle fleet that still leases work will
        expire timed-out joins without a dedicated ticker.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            return self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id, limit=limit
            )

    def list_awaiting_children(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List parents parked waiting for subtasks (deadline ascending)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        now = _utcnow()
        with transaction(self._pool) as conn:
            self._expire_await_children_timeouts_locked(
                conn, now=now, tenant_id=tenant_id
            )
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                ORDER BY await_children_deadline_at ASC NULLS LAST, created_at ASC
                LIMIT %s OFFSET %s
                """,
                (
                    tenant_id,
                    JobStatus.AWAITING_CHILDREN.value,
                    limit,
                    offset,
                ),
            ).fetchall()
            return [_row_to_job(r) for r in rows]

    def _list_children_locked(
        self,
        conn: Any,
        *,
        parent_id: UUID,
        tenant_id: str,
        limit: int = 500,
    ) -> list[Job]:
        rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND parent_job_id = %s
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (tenant_id, parent_id, limit),
        ).fetchall()
        return [_row_to_job(r) for r in rows]
