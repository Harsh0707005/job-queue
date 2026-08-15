"""Postpone a pending job without cancelling or cloning it."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    PostponeError,
    _JOB_COLUMNS,
    _as_utc,
    _utcnow,
)

_MAX_DELAY_SECONDS = 365 * 24 * 3600


class JobsPostponeMixin:
    def postpone_job(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
    ) -> Job:
        """Push a pending job's ``available_at`` so it is not leased yet.

        Same row, same id, same history. Pass *available_at* or
        *delay_seconds* (exactly one). ``delay_seconds=0`` makes it
        claimable immediately. Leased, parked, and terminal jobs are
        refused. A run time at or after ``expires_at`` is refused so the
        job cannot be delayed into a TTL death.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if (available_at is None) == (delay_seconds is None):
            raise ValueError("pass exactly one of available_at or delay_seconds")
        if delay_seconds is not None:
            delay = float(delay_seconds)
            if delay < 0:
                raise ValueError("delay_seconds must be >= 0")
            if delay > _MAX_DELAY_SECONDS:
                raise ValueError(
                    f"delay_seconds must be <= {_MAX_DELAY_SECONDS}"
                )
        now = _utcnow()
        if available_at is not None:
            run_at = _as_utc(available_at)
        else:
            run_at = now + timedelta(seconds=float(delay_seconds))
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
                raise PostponeError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row, conn=conn)
            if job.status != JobStatus.PENDING:
                raise PostponeError(
                    f"job {job_id} cannot be postponed "
                    f"(status={job.status.value})",
                    job=job,
                )
            if job.expires_at is not None and run_at >= _as_utc(job.expires_at):
                raise PostponeError(
                    f"job {job_id} cannot be postponed until {run_at.isoformat()}: "
                    f"that is at or after expires_at "
                    f"{_as_utc(job.expires_at).isoformat()}",
                    job=job,
                )
            current = _as_utc(job.available_at)
            if current == run_at:
                return job
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET available_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (run_at, now, job_id, tenant_id, JobStatus.PENDING.value),
            ).fetchone()
            if updated is None:
                current_row = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (job_id, tenant_id),
                ).fetchone()
                current_job = (
                    self._job(current_row, conn=conn) if current_row else None
                )
                raise PostponeError(
                    f"job {job_id} cannot be postponed (status changed)",
                    job=current_job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="postponed",
                    from_status=updated["status"],
                    actor=None,
                    detail={
                        "from_available_at": current.isoformat(),
                        "to_available_at": run_at.isoformat(),
                    },
                    now=now,
                )
            return self._job(updated, conn=conn)
