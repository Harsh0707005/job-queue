"""Move a live job to another queue for the same tenant (in place)."""

from __future__ import annotations

from uuid import UUID

from psycopg.errors import UniqueViolation

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    QueueMoveError,
    _CANCELABLE_STATUSES,
    _JOB_COLUMNS,
    _utcnow,
)
from job_queue.queue.tenants_drain import _normalize_queue_name

# Leased jobs stay on the queue they were claimed from. Everything else
# that is still live can be re-homed without cloning.
_MOVABLE_STATUSES = tuple(
    s for s in _CANCELABLE_STATUSES if s is not JobStatus.LEASED
)
_MOVABLE = tuple(s.value for s in _MOVABLE_STATUSES)


class JobsQueueMoveMixin:
    def move_job_queue(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        queue_name: str,
    ) -> Job:
        """Move *job_id* to *queue_name* on the same tenant, same row.

        Pending (and other unleased live) jobs keep their id, audit stream,
        ``cloned_from_job_id``, parent/depends links, labels, skills, and
        retry policy. Leased and terminal jobs are refused. A drained
        destination is refused; leaving a drained source is allowed.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        dest = _normalize_queue_name(queue_name)
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise QueueMoveError(
                    f"tenant {tenant_id!r} is offboarding; move is rejected"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise QueueMoveError(
                    f"tenant {tenant_id!r} is restoring; move is rejected"
                )
            self._assert_tenant_mutable(
                conn, tenant_id, action="move", error=QueueMoveError
            )
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
                raise QueueMoveError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row, conn=conn)
            if job.queue_name == dest:
                return job
            if job.status not in _MOVABLE_STATUSES:
                raise QueueMoveError(
                    f"job {job_id} cannot move queue "
                    f"(status={job.status.value})",
                    job=job,
                )
            if hasattr(self, "_assert_queue_accepts_enqueue"):
                self._assert_queue_accepts_enqueue(conn, tenant_id, dest)
            if job.dedupe_key is not None:
                self._lock_dedupe_key(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=dest,
                    dedupe_key=job.dedupe_key,
                )
                conflict = self._find_active_dedupe(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=dest,
                    dedupe_key=job.dedupe_key,
                )
                if conflict is not None and conflict["id"] != job.id:
                    raise QueueMoveError(
                        f"job {job_id} cannot move to {dest!r}: "
                        f"dedupe_key {job.dedupe_key!r} is already active "
                        f"on job {conflict['id']}",
                        job=job,
                    )
            try:
                with conn.transaction():
                    updated = conn.execute(
                        f"""
                        UPDATE jobs
                        SET queue_name = %s,
                            updated_at = %s
                        WHERE id = %s
                          AND tenant_id = %s
                          AND status = ANY(%s)
                        RETURNING {_JOB_COLUMNS}
                        """,
                        (dest, now, job_id, tenant_id, list(_MOVABLE)),
                    ).fetchone()
            except UniqueViolation as exc:
                raise QueueMoveError(
                    f"job {job_id} cannot move to {dest!r}: "
                    f"dedupe_key {job.dedupe_key!r} is already active",
                    job=job,
                ) from exc
            if updated is None:
                current_row = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (job_id, tenant_id),
                ).fetchone()
                current = self._job(current_row, conn=conn) if current_row else None
                raise QueueMoveError(
                    f"job {job_id} cannot move queue (status changed)",
                    job=current,
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="queue_moved",
                    from_status=updated["status"],
                    actor=None,
                    detail={
                        "from_queue": job.queue_name,
                        "to_queue": dest,
                    },
                    now=now,
                )
            return self._job(updated, conn=conn)
