"""JobQueue jobs query domain: get, list, count, results."""
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


class JobsQueryMixin:
    def list_jobs(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs for *tenant_id*, optionally filtered by labels and status.

        *labels*: job must include **all** listed labels (AND). Order is
        newest ``created_at`` first.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        label_list = _normalize_labels(labels)
        status_list = _normalize_status_filter(status)

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status_list:
            clauses.append("status = ANY(%s)")
            params.append(status_list)
        if label_list:
            clauses.append("labels @> %s::text[]")
            params.append(label_list)
        params.extend([limit, offset])
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE {where}
                ORDER BY created_at DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            ).fetchall()
            return [_row_to_job(r) for r in rows]

    def get(self, job_id: UUID, *, tenant_id: str) -> Job | None:
        """Fetch a job by id for *tenant_id* only (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (job_id, tenant_id),
            ).fetchone()
            return _row_to_job(row) if row else None

    def get_result(self, job_id: UUID, *, tenant_id: str) -> Any | None:
        """Return the stashed ack *result* for a job, or None if missing/unset.

        Cross-tenant and unknown ids return None. Incomplete jobs typically
        have ``result is None`` until a successful ``ack(..., result=...)``.
        """
        job = self.get(job_id, tenant_id=tenant_id)
        if job is None:
            return None
        return job.result

    def count(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        status: JobStatus | None = None,
    ) -> int:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status is not None:
            clauses.append("status = %s")
            params.append(status.value)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE {where}",
                params,
            ).fetchone()
            assert row is not None
            return int(row["n"])
