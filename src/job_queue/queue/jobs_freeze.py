"""Operator hold of a single job: freeze, unfreeze, or dead-letter."""

from __future__ import annotations

from uuid import UUID

from job_queue.db import transaction
from job_queue.models import Job, JobStatus, WebhookEvent
from job_queue.queue._common import (
    _JOB_COLUMNS,
    FreezeError,
    _utcnow,
)


_FREEZEABLE = (
    JobStatus.PENDING,
    JobStatus.LEASED,
    JobStatus.AWAITING_SKILLS,
)


class JobsFreezeMixin:
    def freeze(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        reason: str | None = None,
    ) -> Job:
        """Hold one job so no worker can lease it. Other tenant jobs continue.

        Pending / awaiting-skills jobs stay parked. A leased job is fenced
        (lease token cleared) so the holder cannot ack / nack / heartbeat.
        Already-frozen jobs are returned as-is. Terminal jobs raise
        :class:`FreezeError`. Cross-tenant ids are not found.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        err = (reason or "frozen by operator").strip() or "frozen by operator"
        with transaction(self._pool) as conn:
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
                raise FreezeError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row)
            if job.status == JobStatus.FROZEN:
                return job
            if job.status not in _FREEZEABLE:
                raise FreezeError(
                    f"job {job_id} cannot be frozen "
                    f"(status={job.status.value}; only pending, leased, "
                    f"or awaiting_skills jobs freeze)",
                    job=job,
                )
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status IN (%s, %s, %s)
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.FROZEN.value,
                    err[:2000],
                    now,
                    job_id,
                    tenant_id,
                    JobStatus.PENDING.value,
                    JobStatus.LEASED.value,
                    JobStatus.AWAITING_SKILLS.value,
                ),
            ).fetchone()
            if updated is None:
                current = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
                    (job_id, tenant_id),
                ).fetchone()
                current_job = self._job(current) if current else None
                raise FreezeError(
                    f"job {job_id} cannot be frozen (status changed during freeze)",
                    job=current_job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="frozen",
                    from_status=job.status.value,
                    actor=None,
                    detail={"reason": err, "was_leased": job.status == JobStatus.LEASED},
                    now=now,
                )
            if job.status == JobStatus.LEASED and hasattr(
                self, "_record_lease_end_usage"
            ):
                self._record_lease_end_usage(conn, row, now=now)
            if hasattr(self, "_record_metric"):
                self._record_metric(
                    conn, tenant_id=tenant_id, metric="freeze", count=1, now=now
                )
            return self._job(updated)

    def unfreeze(self, job_id: UUID, *, tenant_id: str) -> Job:
        """Release a frozen job back to pending so it can be leased again."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
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
                raise FreezeError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row)
            if job.status != JobStatus.FROZEN:
                raise FreezeError(
                    f"job {job_id} is not frozen (status={job.status.value})",
                    job=job,
                )
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    available_at = %s,
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
                    job_id,
                    tenant_id,
                    JobStatus.FROZEN.value,
                ),
            ).fetchone()
            if updated is None:
                current = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
                    (job_id, tenant_id),
                ).fetchone()
                current_job = self._job(current) if current else None
                raise FreezeError(
                    f"job {job_id} cannot be unfrozen (status changed)",
                    job=current_job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="unfrozen",
                    from_status=JobStatus.FROZEN.value,
                    actor=None,
                    detail={},
                    now=now,
                )
            return self._job(updated)

    def dead_letter_frozen(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        error: str | None = None,
    ) -> Job:
        """Dead-letter a frozen job. Dependents and spawn parents are notified."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        err = (error or "dead-lettered from freeze").strip() or "dead-lettered from freeze"
        with transaction(self._pool) as conn:
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
                raise FreezeError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row)
            if job.status != JobStatus.FROZEN:
                raise FreezeError(
                    f"job {job_id} is not frozen (status={job.status.value})",
                    job=job,
                )
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    last_error = %s,
                    completed_at = COALESCE(completed_at, %s),
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.DEAD.value,
                    err[:2000],
                    now,
                    now,
                    job_id,
                    tenant_id,
                    JobStatus.FROZEN.value,
                ),
            ).fetchone()
            if updated is None:
                current = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
                    (job_id, tenant_id),
                ).fetchone()
                current_job = self._job(current) if current else None
                raise FreezeError(
                    f"job {job_id} cannot be dead-lettered (status changed)",
                    job=current_job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="dead_lettered",
                    from_status=JobStatus.FROZEN.value,
                    actor=None,
                    detail={"reason": err, "from_freeze": True},
                    now=now,
                )
            if hasattr(self, "_record_metric"):
                self._record_metric(
                    conn, tenant_id=tenant_id, metric="dead", count=1, now=now
                )
            if hasattr(self, "_cascade_cancel_dependents"):
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=job_id,
                    tenant_id=tenant_id,
                    reason=f"parent job {job_id} dead-lettered from freeze: {err}",
                    now=now,
                )
            if updated.get("parent_job_id") is not None and hasattr(
                self, "_maybe_wake_parent_on_join_locked"
            ):
                self._maybe_wake_parent_on_join_locked(
                    conn,
                    parent_id=updated["parent_job_id"],
                    tenant_id=tenant_id,
                    now=now,
                )
            if hasattr(self, "_on_workflow_step_terminal"):
                self._on_workflow_step_terminal(
                    conn, updated, success=False, error=err, now=now
                )
            out = self._job(updated)
            if hasattr(self, "_enqueue_job_webhooks"):
                self._enqueue_job_webhooks(
                    conn, job=out, event=WebhookEvent.JOB_DEAD, now=now
                )
            return out
