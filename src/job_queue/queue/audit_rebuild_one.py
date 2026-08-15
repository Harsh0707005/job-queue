"""JobQueue rebuild_job_from_audit and validation."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobAuditEvent,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    RebuildError,
    _as_utc,
    _utcnow,
)


class AuditRebuildOneMixin:
    def rebuild_job_from_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str | None = None,
    ) -> Job:
        """Reconstruct a missing job row from its immutable audit stream.

        Use after a bad migrate / partial write lost the ``jobs`` row while
        ``job_audit_events`` remain. Rebuild:

        1. Loads **all** events for *job_id* (any event.tenant_id — covers merge).
        2. Validates contiguous ``seq`` from 1, valid transitions, and that
           every event embeds a ``detail.job`` snapshot (required for rebuild).
        3. Uses the **last** snapshot as the reconstructed row (includes
           checkpoint / approval / lease fields as of that event).
        4. Refuses to insert on any inconsistency (raises :class:`RebuildError`).
        5. Refuses if a job row already exists for this id.
        6. Authorizes: last snapshot's ``tenant_id`` must match *tenant_id*
           when provided (current owner after merges).

        Appends a ``rebuilt`` audit event with the restored snapshot.
        """
        with transaction(self._pool) as conn:
            existing = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s FOR UPDATE",
                (job_id,),
            ).fetchone()
            if existing is not None:
                raise RebuildError(
                    f"job {job_id} already exists "
                    f"(tenant={existing['tenant_id']!r}, "
                    f"status={existing['status']!r}); refuse to overwrite",
                    job_id=job_id,
                )

            rows = conn.execute(
                """
                SELECT
                    event_id, tenant_id, job_id, seq, event_type,
                    from_status, to_status, actor, detail, created_at
                FROM job_audit_events
                WHERE job_id = %s
                ORDER BY seq ASC
                FOR UPDATE
                """,
                (job_id,),
            ).fetchall()
            events = [self._row_to_audit(r) for r in rows]
            snap = self._validate_audit_for_rebuild(job_id, events)

            if tenant_id is not None and snap["tenant_id"] != tenant_id:
                raise RebuildError(
                    f"job {job_id} rebuild owner is {snap['tenant_id']!r}, "
                    f"not {tenant_id!r}",
                    job_id=job_id,
                    events=events,
                )

            now = _utcnow()
            self._insert_job_from_snapshot(conn, snap)
            inserted = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (job_id,),
            ).fetchone()
            assert inserted is not None
            self._append_job_audit(
                conn,
                job_row=inserted,
                event_type="rebuilt",
                from_status=inserted["status"],
                actor="system",
                detail={"source": "audit_rebuild", "events": len(events)},
                now=now,
            )
            return self._job(inserted)

    def rebuild_jobs_from_audit(
        self,
        job_ids: list[UUID],
        *,
        tenant_id: str | None = None,
    ) -> list[Job]:
        """Rebuild multiple missing jobs; stops on first :class:`RebuildError`."""
        out: list[Job] = []
        for jid in job_ids:
            out.append(self.rebuild_job_from_audit(jid, tenant_id=tenant_id))
        return out

    def _validate_audit_for_rebuild(
        self,
        job_id: UUID,
        events: list[JobAuditEvent],
    ) -> dict[str, Any]:
        """Validate event stream; return last job snapshot or raise RebuildError."""
        if not events:
            raise RebuildError(
                f"job {job_id} has no audit events",
                job_id=job_id,
            )
        for i, ev in enumerate(events, start=1):
            if ev.seq != i:
                raise RebuildError(
                    f"job {job_id} audit seq gap: expected {i}, got {ev.seq}",
                    job_id=job_id,
                    events=events,
                )
            if ev.job_id != job_id:
                raise RebuildError(
                    f"job {job_id} audit event {ev.seq} has wrong job_id {ev.job_id}",
                    job_id=job_id,
                    events=events,
                )
        if events[0].event_type != "enqueued":
            raise RebuildError(
                f"job {job_id} first audit event must be enqueued, "
                f"got {events[0].event_type!r}",
                job_id=job_id,
                events=events,
            )
        if events[0].from_status is not None:
            raise RebuildError(
                f"job {job_id} enqueued event must have from_status=null",
                job_id=job_id,
                events=events,
            )

        prev_status: str | None = None
        last_snap: dict[str, Any] | None = None
        for ev in events:
            snap = (ev.detail or {}).get("job")
            if not isinstance(snap, dict):
                raise RebuildError(
                    f"job {job_id} audit seq={ev.seq} event_type={ev.event_type!r} "
                    f"missing detail.job snapshot (cannot rebuild safely)",
                    job_id=job_id,
                    events=events,
                )
            if str(snap.get("id")) != str(job_id):
                raise RebuildError(
                    f"job {job_id} snapshot id mismatch at seq={ev.seq}",
                    job_id=job_id,
                    events=events,
                )
            snap_status = snap.get("status")
            if snap_status != ev.to_status:
                raise RebuildError(
                    f"job {job_id} seq={ev.seq}: snapshot status {snap_status!r} "
                    f"!= event to_status {ev.to_status!r}",
                    job_id=job_id,
                    events=events,
                )
            if prev_status is not None and ev.from_status != prev_status:
                if not (
                    ev.from_status == ev.to_status and ev.from_status == prev_status
                ):
                    raise RebuildError(
                        f"job {job_id} seq={ev.seq}: from_status {ev.from_status!r} "
                        f"does not continue previous to_status {prev_status!r}",
                        job_id=job_id,
                        events=events,
                    )
            if ev.event_type in ("tenant_merged", "tenant_moved", "tenant_split"):
                ft = (ev.detail or {}).get("from_tenant")
                tt = (ev.detail or {}).get("to_tenant")
                if not ft or not tt:
                    raise RebuildError(
                        f"job {job_id} {ev.event_type} seq={ev.seq} missing "
                        f"from_tenant/to_tenant",
                        job_id=job_id,
                        events=events,
                    )
                if snap.get("tenant_id") != tt:
                    raise RebuildError(
                        f"job {job_id} {ev.event_type} seq={ev.seq}: snapshot "
                        f"tenant {snap.get('tenant_id')!r} != to_tenant {tt!r}",
                        job_id=job_id,
                        events=events,
                    )
            self._assert_snapshot_status_fields(job_id, snap, events)
            prev_status = ev.to_status
            last_snap = snap

        assert last_snap is not None
        return last_snap

    def _assert_snapshot_status_fields(
        self,
        job_id: UUID,
        snap: dict[str, Any],
        events: list[JobAuditEvent],
    ) -> None:
        """Reject snapshots that violate lease/status invariants."""
        status = snap.get("status")
        try:
            JobStatus(status)
        except ValueError as exc:
            raise RebuildError(
                f"job {job_id} invalid status {status!r} in snapshot",
                job_id=job_id,
                events=events,
            ) from exc

        leased_by = snap.get("leased_by")
        lease_token = snap.get("lease_token")
        if status == JobStatus.LEASED.value:
            if not leased_by or not lease_token:
                raise RebuildError(
                    f"job {job_id} leased snapshot missing leased_by/lease_token",
                    job_id=job_id,
                    events=events,
                )
        elif status in (
            JobStatus.PENDING.value,
            JobStatus.AWAITING_APPROVAL.value,
            JobStatus.AWAITING_SKILLS.value,
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
        ):
            if lease_token is not None or leased_by is not None:
                raise RebuildError(
                    f"job {job_id} status={status!r} snapshot still has "
                    f"lease fields set",
                    job_id=job_id,
                    events=events,
                )
        if status == JobStatus.COMPLETED.value and not snap.get("completed_at"):
            raise RebuildError(
                f"job {job_id} completed snapshot missing completed_at",
                job_id=job_id,
                events=events,
            )
        if not snap.get("tenant_id") or not snap.get("queue_name"):
            raise RebuildError(
                f"job {job_id} snapshot missing tenant_id/queue_name",
                job_id=job_id,
                events=events,
            )
        if not isinstance(snap.get("payload"), dict):
            raise RebuildError(
                f"job {job_id} snapshot payload must be an object",
                job_id=job_id,
                events=events,
            )

    def _insert_job_from_snapshot(
        self,
        conn: Any,
        snap: dict[str, Any],
    ) -> None:
        """Insert a jobs row from a validated audit snapshot."""

        def _parse_dt(key: str) -> datetime | None:
            raw = snap.get(key)
            if raw is None:
                return None
            if isinstance(raw, datetime):
                return _as_utc(raw)
            return _as_utc(datetime.fromisoformat(str(raw).replace("Z", "+00:00")))

        def _parse_uuid(key: str) -> UUID | None:
            raw = snap.get(key)
            if raw is None:
                return None
            return UUID(str(raw))

        deps_raw = snap.get("depends_on") or []
        deps = [UUID(str(d)) for d in deps_raw]
        waiting_raw = snap.get("waiting_on") or []
        waiting = [UUID(str(d)) for d in waiting_raw]
        labels_raw = snap.get("labels") or []
        labels = [str(x) for x in labels_raw]
        skills_raw = snap.get("required_skills") or []
        required_skills = [str(x) for x in skills_raw]
        payload = snap["payload"]
        if not isinstance(payload, dict):
            payload = {"_value": payload}
        result = snap.get("result")
        checkpoint = snap.get("checkpoint")

        job_uuid = UUID(str(snap["id"]))
        tenant = str(snap["tenant_id"])
        if snap.get("partition_id") is not None:
            part_id = int(snap["partition_id"])
        elif hasattr(self, "_compute_partition_id"):
            part_id = self._compute_partition_id(
                conn, tenant_id=tenant, job_id=job_uuid
            )
        else:
            part_id = 0
        sealed, payload_key_id = self._seal_job_payload(
            conn, tenant_id=tenant, job_id=job_uuid, payload=payload
        )
        conn.execute(
            """
            INSERT INTO jobs (
                id, tenant_id, queue_name, payload, status, priority,
                attempts, max_attempts, visibility_timeout_seconds,
                max_runtime_seconds,
                retry_backoff_base_seconds, retry_backoff_max_seconds,
                available_at, dedupe_key, schedule_id, depends_on,
                parent_job_id, waiting_on, labels, required_skills,
                leased_by, lease_token, lease_tenant_id,
                lease_expires_at, lease_started_at,
                created_at, updated_at, completed_at, result,
                checkpoint, checkpointed_at,
                deadline_at, estimated_duration_seconds, preemptible,
                preempt_count, last_preempted_at,
                approval_requested_at, approval_deadline_at,
                approval_message, approval_outcome, approval_resolved_at,
                await_children_started_at, await_children_deadline_at,
                await_children_outcome, await_children_resolved_at,
                last_error, partition_id, payload_key_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s
            )
            """,
            (
                job_uuid,
                tenant,
                snap["queue_name"],
                Jsonb(sealed),
                snap["status"],
                int(snap.get("priority") or 0),
                int(snap.get("attempts") or 0),
                int(snap.get("max_attempts") or 3),
                int(snap.get("visibility_timeout_seconds") or 30),
                int(snap.get("max_runtime_seconds") or 3600),
                int(snap.get("retry_backoff_base_seconds") or 1),
                int(snap.get("retry_backoff_max_seconds") or 300),
                _parse_dt("available_at") or _utcnow(),
                snap.get("dedupe_key"),
                _parse_uuid("schedule_id"),
                deps,
                _parse_uuid("parent_job_id"),
                waiting,
                labels,
                required_skills,
                snap.get("leased_by"),
                _parse_uuid("lease_token"),
                snap.get("lease_tenant_id"),
                _parse_dt("lease_expires_at"),
                _parse_dt("lease_started_at"),
                _parse_dt("created_at") or _utcnow(),
                _parse_dt("updated_at") or _utcnow(),
                _parse_dt("completed_at"),
                Jsonb(result) if result is not None else None,
                Jsonb(checkpoint) if checkpoint is not None else None,
                _parse_dt("checkpointed_at"),
                _parse_dt("deadline_at"),
                (
                    None
                    if snap.get("estimated_duration_seconds") is None
                    else int(snap["estimated_duration_seconds"])
                ),
                bool(snap.get("preemptible", True)),
                int(snap.get("preempt_count") or 0),
                _parse_dt("last_preempted_at"),
                _parse_dt("approval_requested_at"),
                _parse_dt("approval_deadline_at"),
                snap.get("approval_message"),
                snap.get("approval_outcome"),
                _parse_dt("approval_resolved_at"),
                _parse_dt("await_children_started_at"),
                _parse_dt("await_children_deadline_at"),
                snap.get("await_children_outcome"),
                _parse_dt("await_children_resolved_at"),
                snap.get("last_error"),
                part_id,
                payload_key_id,
            ),
        )
