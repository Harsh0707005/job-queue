"""JobQueue request_approval."""
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


class ApprovalRequestMixin:
    def request_approval(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        message: str | None = None,
        timeout_seconds: float | int = 3600,
        progress: Any = None,
    ) -> Job:
        """Park a leased job until tenant-scoped approve/reject.

        Releases the lease (and concurrency slot) immediately while keeping
        the fencing token cleared so the worker cannot finish the job.
        Optional *progress* is written as a checkpoint in the same transaction
        so an approve resumes from that point. *timeout_seconds* sets
        ``approval_deadline_at``; past that, ``process_approval_timeouts``
        auto-rejects.

        Safe to call only under a live lease. Idempotent if the job is already
        ``awaiting_approval`` for the same tenant (returns current row).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if timeout_seconds is None or float(timeout_seconds) <= 0:
            raise ValueError("timeout_seconds must be > 0")
        now = _utcnow()
        deadline = now + timedelta(seconds=float(timeout_seconds))
        msg = (message or "").strip() or None
        if msg is not None:
            msg = msg[:2000]

        with transaction(self._pool) as conn:
            # Expire any timed-out approvals first.
            self._expire_approval_timeouts_locked(conn, now=now, tenant_id=tenant_id)

            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            if row["status"] == JobStatus.AWAITING_APPROVAL.value:
                if row["tenant_id"] != tenant_id:
                    raise ApprovalError(
                        f"job {job_id} not found for tenant {tenant_id!r}"
                    )
                return _row_to_job(row)

            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            if progress is not None:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        checkpoint = %s,
                        checkpointed_at = %s,
                        approval_requested_at = %s,
                        approval_deadline_at = %s,
                        approval_message = %s,
                        approval_outcome = NULL,
                        approval_resolved_at = NULL,
                        last_error = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.AWAITING_APPROVAL.value,
                        Jsonb(progress),
                        now,
                        now,
                        deadline,
                        msg,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
            else:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        approval_requested_at = %s,
                        approval_deadline_at = %s,
                        approval_message = %s,
                        approval_outcome = NULL,
                        approval_resolved_at = NULL,
                        last_error = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.AWAITING_APPROVAL.value,
                        now,
                        deadline,
                        msg,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during request_approval")
            job = _row_to_job(updated)
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="approval_requested",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={
                    "message": msg,
                    "timeout_seconds": float(timeout_seconds),
                },
                now=now,
            )
            return job
