"""Change priority on an already-enqueued job (tenant-scoped)."""

from __future__ import annotations

from uuid import UUID

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    _CANCELABLE_STATUSES,
    _JOB_COLUMNS,
    PriorityError,
    _utcnow,
)

# Postgres INTEGER.
_MIN_PRIORITY = -2_147_483_648
_MAX_PRIORITY = 2_147_483_647
_MUTABLE = tuple(s.value for s in _CANCELABLE_STATUSES)


class JobsPriorityMixin:
    def set_job_prefer_last_worker(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> Job:
        """Mark one live job to prefer the last successful similar worker."""
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
                raise PriorityError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row)
            if job.status not in _CANCELABLE_STATUSES:
                raise PriorityError(
                    f"job {job_id} cannot change prefer_last_worker "
                    f"(status={job.status.value})",
                    job=job,
                )
            flag = bool(enabled)
            if bool(row.get("prefer_last_worker") or False) == flag:
                return job
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET prefer_last_worker = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (flag, now, job_id, tenant_id),
            ).fetchone()
            if updated is None:
                raise PriorityError(
                    f"job {job_id} cannot change prefer_last_worker",
                    job=job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="prefer_last_worker_changed",
                    from_status=updated["status"],
                    actor=None,
                    detail={"prefer_last_worker": flag},
                    now=now,
                )
            return self._job(updated)

    def set_job_priority(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        priority: int | None = None,
        delta: int | None = None,
    ) -> Job:
        """Raise or lower a live job's priority so the next fair claim sees it.

        Pass *priority* to set an absolute value, or *delta* to add (negative
        drops the job). Terminal jobs cannot change. Cross-tenant ids are
        not found. Fair lease still ranks ``priority DESC`` before tenant
        weight, so a bump jumps the lane without starving other tenants
        inside that lane.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if (priority is None) == (delta is None):
            raise ValueError("pass exactly one of priority or delta")
        if priority is not None:
            priority = int(priority)
            self._check_priority(priority)
        else:
            delta = int(delta)
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
                raise PriorityError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row)
            if job.status not in _CANCELABLE_STATUSES:
                raise PriorityError(
                    f"job {job_id} cannot change priority "
                    f"(status={job.status.value})",
                    job=job,
                )
            current = int(row["priority"] or 0)
            if priority is None:
                next_pri = current + int(delta)
                self._check_priority(next_pri)
            else:
                next_pri = priority
            if next_pri == current:
                return job
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET priority = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = ANY(%s)
                RETURNING {_JOB_COLUMNS}
                """,
                (next_pri, now, job_id, tenant_id, list(_MUTABLE)),
            ).fetchone()
            if updated is None:
                current_row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
                    (job_id, tenant_id),
                ).fetchone()
                current_job = self._job(current_row) if current_row else None
                raise PriorityError(
                    f"job {job_id} cannot change priority (status changed)",
                    job=current_job,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="priority_changed",
                    from_status=updated["status"],
                    actor=None,
                    detail={
                        "from_priority": current,
                        "to_priority": next_pri,
                    },
                    now=now,
                )
            return self._job(updated)

    @staticmethod
    def _check_priority(value: int) -> None:
        if value < _MIN_PRIORITY or value > _MAX_PRIORITY:
            raise ValueError(
                f"priority must be {_MIN_PRIORITY}..{_MAX_PRIORITY}"
            )
