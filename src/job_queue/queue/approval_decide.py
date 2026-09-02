"""JobQueue approve/reject/list."""

from __future__ import annotations

from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    ApprovalError,
    _utcnow,
)


class ApprovalDecideMixin:
    def approve(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
    ) -> Job:
        """Approve a parked job so it becomes claimable again (idempotent).

        * ``awaiting_approval`` → ``pending`` (available immediately; checkpoint kept)
        * Already approved and still active (pending/leased/completed) → no-op return
        * Rejected / timed out / cancelled → :class:`ApprovalError`
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            self._assert_tenant_mutable(
                conn, tenant_id, action="update", error=ApprovalError
            )
            self._expire_approval_timeouts_locked(conn, now=now, tenant_id=tenant_id)
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (job_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ApprovalError(f"job {job_id} not found for tenant {tenant_id!r}")
            job = self._job(row)
            status = job.status

            if status == JobStatus.AWAITING_APPROVAL:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        available_at = %s,
                        approval_outcome = 'approved',
                        approval_resolved_at = %s,
                        approval_deadline_at = NULL,
                        last_error = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.PENDING.value,
                        now,
                        now,
                        now,
                        job_id,
                        tenant_id,
                        JobStatus.AWAITING_APPROVAL.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise ApprovalError(
                        f"job {job_id} approval race",
                        job=job,
                    )
                out = self._job(updated)
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="approved",
                    from_status=JobStatus.AWAITING_APPROVAL.value,
                    actor=None,
                    detail={},
                    now=now,
                )
                return out

            # Idempotent: already approved path.
            if job.approval_outcome == "approved" and status in (
                JobStatus.PENDING,
                JobStatus.LEASED,
                JobStatus.COMPLETED,
            ):
                return job

            if status in (
                JobStatus.CANCELLED,
                JobStatus.FAILED,
                JobStatus.DEAD,
            ):
                raise ApprovalError(
                    f"job {job_id} cannot be approved "
                    f"(status={status.value}, outcome={job.approval_outcome!r})",
                    job=job,
                )
            # pending/leased without prior approval request — treat as no-op only
            # if never waited? Safer to error so callers don't approve wrong jobs.
            if status in (JobStatus.PENDING, JobStatus.LEASED, JobStatus.COMPLETED):
                if job.approval_outcome is None and job.approval_requested_at is None:
                    raise ApprovalError(
                        f"job {job_id} is not awaiting approval "
                        f"(status={status.value})",
                        job=job,
                    )
                return job
            raise ApprovalError(
                f"job {job_id} cannot be approved (status={status.value})",
                job=job,
            )

    def reject(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        reason: str | None = None,
    ) -> Job:
        """Reject a parked job (idempotent). Terminal: ``cancelled``.

        Already rejected / timed-out cancelled jobs return the current row.
        Approving then rejecting an in-flight job is not allowed.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        err = (reason or "approval rejected").strip()[:2000] or "approval rejected"
        with transaction(self._pool) as conn:
            self._assert_tenant_mutable(
                conn, tenant_id, action="update", error=ApprovalError
            )
            self._expire_approval_timeouts_locked(conn, now=now, tenant_id=tenant_id)
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (job_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ApprovalError(f"job {job_id} not found for tenant {tenant_id!r}")
            job = self._job(row)

            if job.status == JobStatus.AWAITING_APPROVAL:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        approval_outcome = 'rejected',
                        approval_resolved_at = %s,
                        approval_deadline_at = NULL,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        now,
                        err,
                        now,
                        job_id,
                        tenant_id,
                        JobStatus.AWAITING_APPROVAL.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise ApprovalError(f"job {job_id} reject race", job=job)
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="rejected",
                    from_status=JobStatus.AWAITING_APPROVAL.value,
                    actor=None,
                    detail={"reason": err},
                    now=now,
                )
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=job_id,
                    tenant_id=tenant_id,
                    reason=f"parent job {job_id} rejected",
                    now=now,
                )
                return self._job(updated)

            # Idempotent reject after timeout/reject.
            if job.status == JobStatus.CANCELLED and job.approval_outcome in (
                "rejected",
                "timeout",
            ):
                return job

            raise ApprovalError(
                f"job {job_id} is not awaiting approval "
                f"(status={job.status.value}, outcome={job.approval_outcome!r})",
                job=job,
            )

    def list_awaiting_approvals(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs parked for approval for *tenant_id* (deadline ascending)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        now = _utcnow()
        with transaction(self._pool) as conn:
            self._assert_tenant_mutable(
                conn, tenant_id, action="update", error=ApprovalError
            )
            self._expire_approval_timeouts_locked(conn, now=now, tenant_id=tenant_id)
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                ORDER BY approval_deadline_at ASC NULLS LAST, created_at ASC
                LIMIT %s OFFSET %s
                """,
                (
                    tenant_id,
                    JobStatus.AWAITING_APPROVAL.value,
                    limit,
                    offset,
                ),
            ).fetchall()
            return [self._job(r) for r in rows]
