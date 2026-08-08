"""JobQueue request_approval."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    ApprovalError,
    LeaseError,
    _utcnow,
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
                return self._job(row)

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
            job = self._job(updated)
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
