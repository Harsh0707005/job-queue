"""Move a live schedule to another tenant without delete/recreate."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.crypto import result_aad
from job_queue.db import transaction
from job_queue.models import JobStatus, Schedule
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    ScheduleError,
    _utcnow,
)


class SchedulesMoveMixin:
    def move_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        to_tenant_id: str,
    ) -> Schedule:
        """Transfer a live schedule to *to_tenant_id* in place.

        Same schedule id. The in-flight tick (one job or a whole DAG, plus
        spawn children that close the graph) moves with it. Payloads and
        results are re-sealed under the destination tenant so dest can
        decrypt. ``mutex_group`` stays on the row: exclusivity is
        ``(tenant, group)``, so dest peers still serialize and the source
        group is no longer blocked by this member.

        Serializes with ``process_due`` / ``fire`` via ``FOR UPDATE`` on
        the schedule row. Dual schedulers cannot enqueue a second tick
        during the move (``SKIP LOCKED``). If dest already has an inflight
        member of the same group, the move is refused.
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
                    raise ScheduleError(
                        f"{label} tenant {tid!r} is offboarding; move is refused"
                    )
                if self._is_tenant_restoring(conn, tid):
                    raise ScheduleError(
                        f"{label} tenant {tid!r} is restoring; move is refused"
                    )
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            if dest == tenant_id:
                return self._schedule(row)

            qname = str(row["queue_name"])
            if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
                conn, dest, qname
            ):
                raise ScheduleError(f"queue {qname!r} is draining")

            group = row.get("mutex_group")
            if group:
                if not self._try_lock_schedule_mutex_group(
                    conn, dest, str(group)
                ) or self._schedule_mutex_blocked(
                    conn, tenant_id=dest, mutex_group=str(group)
                ):
                    raise ScheduleError(
                        f"schedule {schedule_id} cannot move to tenant "
                        f"{dest!r}: mutex group {group!r} already has "
                        "in-flight work"
                    )

            job_ids = self._schedule_graph_job_ids(
                conn, tenant_id=tenant_id, schedule_id=schedule_id
            )
            jobs = []
            if job_ids:
                jobs = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND id = ANY(%s)
                    ORDER BY id
                    FOR UPDATE
                    """,
                    (tenant_id, job_ids),
                ).fetchall()
                if len(jobs) != len(job_ids):
                    raise ScheduleError(
                        f"schedule {schedule_id} cannot move: graph changed "
                        "during lock"
                    )
                wf = conn.execute(
                    """
                    SELECT job_id
                    FROM workflow_step_runs
                    WHERE job_id = ANY(%s)
                    LIMIT 1
                    """,
                    (job_ids,),
                ).fetchone()
                if wf is not None:
                    raise ScheduleError(
                        f"schedule {schedule_id} cannot move: job "
                        f"{wf['job_id']} belongs to a workflow"
                    )
                pending_extra = sum(
                    1
                    for j in jobs
                    if str(j["status"]) == JobStatus.PENDING.value
                )
                if pending_extra and hasattr(self, "_read_pending_limit"):
                    limit = self._read_pending_limit(conn, dest)
                    if limit is not None:
                        have = self._count_pending(conn, dest)
                        if have + pending_extra > int(limit):
                            raise ScheduleError(
                                f"destination tenant {dest!r} pending job "
                                f"cap would be exceeded "
                                f"({have}+{pending_extra} > {limit})"
                            )
                for jrow in jobs:
                    job = self._job(jrow, conn=conn)
                    if job.dedupe_key is None:
                        continue
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
                    if conflict is not None and conflict["id"] != job.id:
                        raise ScheduleError(
                            f"schedule {schedule_id} cannot move to "
                            f"{dest!r}: dedupe_key {job.dedupe_key!r} "
                            f"is already active on job {conflict['id']}"
                        )

            sealed, payload_key_id = self._seal_schedule_payload(
                conn,
                tenant_id=dest,
                schedule_id=schedule_id,
                payload=self._open_schedule_payload(
                    row["payload"],
                    tenant_id=tenant_id,
                    schedule_id=schedule_id,
                    conn=conn,
                ),
            )
            try:
                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET tenant_id = %s,
                        payload = %s,
                        payload_key_id = %s,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        dest,
                        Jsonb(sealed),
                        payload_key_id,
                        now,
                        schedule_id,
                        tenant_id,
                    ),
                ).fetchone()
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule name {row.get('name')!r} already exists "
                    f"for tenant {dest!r}"
                ) from exc
            if updated is None:
                raise ScheduleError(
                    f"schedule {schedule_id} cannot move (owner changed)"
                )

            for jrow in jobs:
                job = self._job(jrow, conn=conn)
                j_sealed, j_key = self._seal_job_payload(
                    conn,
                    tenant_id=dest,
                    job_id=job.id,
                    payload=job.payload,
                )
                sealed_result = None
                if job.result is not None:
                    sealed_result, _ = self._seal_json_value(
                        conn,
                        tenant_id=dest,
                        value=job.result,
                        aad=result_aad(job.id),
                    )
                part_id = self._compute_partition_id(
                    conn, tenant_id=dest, job_id=job.id
                )
                lease_tid = jrow.get("lease_tenant_id")
                if (
                    job.status == JobStatus.LEASED
                    and lease_tid is None
                ):
                    lease_tid = tenant_id
                moved_job = conn.execute(
                    f"""
                    UPDATE jobs
                    SET tenant_id = %s,
                        payload = %s,
                        result = %s,
                        payload_key_id = %s,
                        partition_id = %s,
                        lease_tenant_id = %s,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        dest,
                        Jsonb(j_sealed),
                        None if sealed_result is None else Jsonb(sealed_result),
                        j_key,
                        part_id,
                        lease_tid,
                        now,
                        job.id,
                        tenant_id,
                    ),
                ).fetchone()
                if moved_job is None:
                    raise ScheduleError(
                        f"schedule {schedule_id} cannot move: job "
                        f"{job.id} raced"
                    )
                conn.execute(
                    """
                    UPDATE job_side_effects
                    SET tenant_id = %s
                    WHERE job_id = %s
                    """,
                    (dest, job.id),
                )
                if hasattr(self, "_append_job_audit"):
                    self._append_job_audit(
                        conn,
                        job_row=moved_job,
                        event_type="schedule_tenant_moved",
                        from_status=moved_job["status"],
                        actor=None,
                        detail={
                            "from_tenant": tenant_id,
                            "to_tenant": dest,
                            "schedule_id": str(schedule_id),
                        },
                        now=now,
                    )

            conn.execute(
                """
                UPDATE job_dag_batches
                SET tenant_id = %s
                WHERE schedule_id = %s AND tenant_id = %s
                """,
                (dest, schedule_id, tenant_id),
            )
            conn.execute(
                """
                UPDATE schedule_blackouts
                SET tenant_id = %s,
                    updated_at = %s
                WHERE schedule_id = %s AND tenant_id = %s
                """,
                (dest, now, schedule_id, tenant_id),
            )
            return self._schedule(updated)

    @staticmethod
    def _schedule_graph_job_ids(
        conn: Any, *, tenant_id: str, schedule_id: UUID
    ) -> list[UUID]:
        """Jobs that must move with the schedule to keep a closed graph."""
        seed = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE tenant_id = %s AND schedule_id = %s
            """,
            (tenant_id, schedule_id),
        ).fetchall()
        ids = {row["id"] for row in seed}
        if not ids:
            return []
        while True:
            extra = conn.execute(
                """
                SELECT id
                FROM jobs
                WHERE tenant_id = %s
                  AND NOT (id = ANY(%s))
                  AND (
                    parent_job_id = ANY(%s)
                    OR depends_on && %s::uuid[]
                    OR waiting_on && %s::uuid[]
                    OR id = ANY(
                        SELECT UNNEST(COALESCE(depends_on, '{}'::uuid[]))
                        FROM jobs
                        WHERE id = ANY(%s)
                    )
                    OR id = ANY(
                        SELECT UNNEST(COALESCE(waiting_on, '{}'::uuid[]))
                        FROM jobs
                        WHERE id = ANY(%s)
                    )
                    OR id IN (
                        SELECT parent_job_id
                        FROM jobs
                        WHERE id = ANY(%s)
                          AND parent_job_id IS NOT NULL
                    )
                  )
                """,
                (
                    tenant_id,
                    list(ids),
                    list(ids),
                    list(ids),
                    list(ids),
                    list(ids),
                    list(ids),
                    list(ids),
                ),
            ).fetchall()
            found = {row["id"] for row in extra}
            if not found:
                break
            ids |= found
        return sorted(ids)
