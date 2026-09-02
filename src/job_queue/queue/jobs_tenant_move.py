"""Move a pending job to another tenant in place (same id and history)."""

from __future__ import annotations

from uuid import UUID

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    TenantMoveError,
    _CANCELABLE_STATUSES,
    _JOB_COLUMNS,
    _utcnow,
)

_LIVE = tuple(s.value for s in _CANCELABLE_STATUSES)


class JobsTenantMoveMixin:
    def move_job_tenant(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        to_tenant_id: str,
    ) -> Job:
        """Transfer ownership of a pending job to *to_tenant_id*.

        Same row, same id, same audit stream. The source tenant can no
        longer get, cancel, or lease it. Payload is re-sealed under the
        destination tenant. Graph links stay tenant-scoped: a job that
        depends on parents, is a spawn child, or still has live dependents
        / children on the source tenant is refused so leftovers cannot
        stall forever.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        dest = (to_tenant_id or "").strip()
        if not dest:
            raise ValueError("to_tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            for tid in sorted((tenant_id, dest)):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (tid,),
                )
            for tid, label in ((tenant_id, "source"), (dest, "destination")):
                if self._is_tenant_offboarding(conn, tid):
                    raise TenantMoveError(
                        f"{label} tenant {tid!r} is offboarding; move is rejected"
                    )
                if self._is_tenant_restoring(conn, tid):
                    raise TenantMoveError(
                        f"{label} tenant {tid!r} is restoring; move is rejected"
                    )
                self._assert_tenant_mutable(
                    conn, tid, action="move", error=TenantMoveError
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
                raise TenantMoveError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            job = self._job(row, conn=conn)
            if dest == tenant_id:
                return job
            if job.status != JobStatus.PENDING:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant "
                    f"(status={job.status.value})",
                    job=job,
                )
            if job.depends_on:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant: it depends on "
                    f"{len(job.depends_on)} parent(s) on {tenant_id!r}",
                    job=job,
                )
            if job.parent_job_id is not None:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant: it is a child of "
                    f"{job.parent_job_id} on {tenant_id!r}",
                    job=job,
                )
            if job.waiting_on:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant: it is waiting on children",
                    job=job,
                )
            held = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE tenant_id = %s
                  AND id <> %s
                  AND status = ANY(%s)
                  AND (
                      depends_on @> ARRAY[%s]::uuid[]
                      OR parent_job_id = %s
                      OR waiting_on @> ARRAY[%s]::uuid[]
                  )
                ORDER BY id
                LIMIT 1
                """,
                (tenant_id, job_id, list(_LIVE), job_id, job_id, job_id),
            ).fetchone()
            if held is not None:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant: "
                    f"job {held['id']} on {tenant_id!r} still depends on it",
                    job=job,
                )
            step = conn.execute(
                """
                SELECT 1
                FROM workflow_step_runs
                WHERE job_id = %s
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            if step is not None:
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant: it belongs to a workflow",
                    job=job,
                )
            if hasattr(self, "_assert_queue_accepts_enqueue"):
                self._assert_queue_accepts_enqueue(conn, dest, job.queue_name)
            try:
                self._assert_pending_capacity(conn, dest)
            except ValueError as exc:
                raise TenantMoveError(str(exc), job=job) from exc
            if job.dedupe_key is not None:
                self._lock_dedupe_key(
                    conn,
                    tenant_id=dest,
                    queue_name=job.queue_name,
                    dedupe_key=job.dedupe_key,
                )
                conflict = self._find_active_dedupe(
                    conn,
                    tenant_id=dest,
                    queue_name=job.queue_name,
                    dedupe_key=job.dedupe_key,
                )
                if conflict is not None:
                    raise TenantMoveError(
                        f"job {job_id} cannot move to tenant {dest!r}: "
                        f"dedupe_key {job.dedupe_key!r} is already active "
                        f"on job {conflict['id']}",
                        job=job,
                    )
            old_payload_blob = (
                self._blob_id_of(row.get("payload"))
                if hasattr(self, "_blob_id_of")
                else None
            )
            sealed, payload_key_id = self._seal_job_payload(
                conn,
                tenant_id=dest,
                job_id=job.id,
                payload=job.payload,
            )
            if hasattr(self, "_orphan_blob"):
                new_payload_blob = self._blob_id_of(sealed)
                if old_payload_blob and old_payload_blob != new_payload_blob:
                    self._orphan_blob(conn, old_payload_blob)
            part_id = self._compute_partition_id(
                conn, tenant_id=dest, job_id=job.id
            )
            try:
                with conn.transaction():
                    updated = conn.execute(
                        f"""
                        UPDATE jobs
                        SET tenant_id = %s,
                            payload = %s,
                            payload_key_id = %s,
                            partition_id = %s,
                            schedule_id = NULL,
                            lease_tenant_id = NULL,
                            updated_at = %s
                        WHERE id = %s
                          AND tenant_id = %s
                          AND status = %s
                        RETURNING {_JOB_COLUMNS}
                        """,
                        (
                            dest,
                            Jsonb(sealed),
                            payload_key_id,
                            part_id,
                            now,
                            job_id,
                            tenant_id,
                            JobStatus.PENDING.value,
                        ),
                    ).fetchone()
            except UniqueViolation as exc:
                raise TenantMoveError(
                    f"job {job_id} cannot move to tenant {dest!r}: "
                    f"unique constraint conflict",
                    job=job,
                ) from exc
            if updated is None:
                current_row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (job_id,),
                ).fetchone()
                current = (
                    self._job(current_row, conn=conn) if current_row else None
                )
                raise TenantMoveError(
                    f"job {job_id} cannot move tenant (status or owner changed)",
                    job=current,
                )
            conn.execute(
                """
                UPDATE job_side_effects
                SET tenant_id = %s
                WHERE job_id = %s
                """,
                (dest, job_id),
            )
            if hasattr(self, "_rehome_job_audit_events"):
                self._rehome_job_audit_events(
                    conn,
                    from_tenant=tenant_id,
                    to_tenant=dest,
                    job_ids=[job_id],
                )
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="tenant_moved",
                    from_status=updated["status"],
                    actor=None,
                    detail={
                        "from_tenant": tenant_id,
                        "to_tenant": dest,
                    },
                    now=now,
                )
            return self._job(updated, conn=conn)
