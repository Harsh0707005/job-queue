"""JobQueue audit append: snapshots, append, progress notify."""

from __future__ import annotations

import json

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.models import (
    JobAuditEvent,
    JobStatus,
)
from job_queue.crypto import audit_event_aad
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
    _AUDIT_SEQ_LOCK_CLASS,
    _AUDIT_TENANT_SEQ_LOCK_CLASS,
    _NOTIFY_MAX_BYTES,
    _as_utc,
    _utcnow,
)


class AuditAppendMixin:
    @staticmethod
    def _job_snapshot(row: dict[str, Any]) -> dict[str, Any]:
        """JSON-serializable full job row for rebuild-from-audit."""

        def _dt(v: Any) -> str | None:
            if v is None:
                return None
            if isinstance(v, datetime):
                return _as_utc(v).isoformat()
            return str(v)

        def _uuid(v: Any) -> str | None:
            if v is None:
                return None
            return str(v)

        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            payload = {"_value": payload}

        result = row.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                pass

        checkpoint = row.get("checkpoint")
        if isinstance(checkpoint, str):
            try:
                checkpoint = json.loads(checkpoint)
            except json.JSONDecodeError:
                pass

        deps = row.get("depends_on") or []
        if isinstance(deps, str):
            deps = []
        dep_out = [str(d) for d in deps]

        return {
            "id": str(row["id"]),
            "tenant_id": row["tenant_id"],
            "queue_name": row["queue_name"],
            "payload": payload,
            "status": row["status"],
            "priority": int(row["priority"] or 0),
            "attempts": int(row["attempts"] or 0),
            "max_attempts": int(row["max_attempts"] or 3),
            "visibility_timeout_seconds": int(row["visibility_timeout_seconds"] or 30),
            "max_runtime_seconds": int(row.get("max_runtime_seconds") or 3600),
            "retry_backoff_base_seconds": int(
                row.get("retry_backoff_base_seconds") or 1
            ),
            "retry_backoff_max_seconds": int(
                row.get("retry_backoff_max_seconds") or 300
            ),
            "available_at": _dt(row.get("available_at")),
            "dedupe_key": row.get("dedupe_key"),
            "coalesce_key": row.get("coalesce_key"),
            "schedule_id": _uuid(row.get("schedule_id")),
            "depends_on": dep_out,
            "labels": list(row.get("labels") or []),
            "required_skills": list(row.get("required_skills") or []),
            "resource_cost": dict(row.get("resource_cost") or {}),
            "leased_by": row.get("leased_by"),
            "lease_token": _uuid(row.get("lease_token")),
            "lease_tenant_id": row.get("lease_tenant_id"),
            "lease_expires_at": _dt(row.get("lease_expires_at")),
            "lease_started_at": _dt(row.get("lease_started_at")),
            "created_at": _dt(row.get("created_at")),
            "updated_at": _dt(row.get("updated_at")),
            "completed_at": _dt(row.get("completed_at")),
            "result": result,
            "checkpoint": checkpoint,
            "checkpointed_at": _dt(row.get("checkpointed_at")),
            "deadline_at": _dt(row.get("deadline_at")),
            "estimated_duration_seconds": (
                None
                if row.get("estimated_duration_seconds") is None
                else int(row["estimated_duration_seconds"])
            ),
            "preemptible": (
                True if row.get("preemptible") is None else bool(row["preemptible"])
            ),
            "preempt_count": int(row.get("preempt_count") or 0),
            "last_preempted_at": _dt(row.get("last_preempted_at")),
            "approval_requested_at": _dt(row.get("approval_requested_at")),
            "approval_deadline_at": _dt(row.get("approval_deadline_at")),
            "approval_message": row.get("approval_message"),
            "approval_outcome": row.get("approval_outcome"),
            "approval_resolved_at": _dt(row.get("approval_resolved_at")),
            "last_error": row.get("last_error"),
            "payload_key_id": _uuid(row.get("payload_key_id")),
            "expires_at": _dt(row.get("expires_at")),
            "expiry_action": row.get("expiry_action") or "dead",
        }

    def _append_job_audit(
        self,
        conn: Any,
        *,
        job_id: UUID | None = None,
        tenant_id: str | None = None,
        event_type: str,
        from_status: str | None,
        to_status: str | None = None,
        actor: str | None = None,
        detail: dict[str, Any] | None = None,
        now: datetime | None = None,
        job_row: dict[str, Any] | None = None,
    ) -> JobAuditEvent:
        """Append one immutable audit row (same transaction as the mutation).

        When *job_row* is provided (preferred), embeds a full ``job`` snapshot
        in ``detail`` so lost rows can be rebuilt. Caller must serialize job
        updates (``FOR UPDATE``) so ``seq`` has no gaps.
        """
        ts = now or _utcnow()
        detail_obj = dict(detail or {})
        if job_row is not None:
            job_id = job_row["id"] if job_id is None else job_id
            tenant_id = job_row["tenant_id"] if tenant_id is None else tenant_id
            to_status = job_row["status"] if to_status is None else to_status
            detail_obj["job"] = self._job_snapshot(job_row)
        if job_id is None or tenant_id is None:
            raise ValueError("job_id and tenant_id are required for audit")
        if to_status is None:
            raise ValueError("to_status is required for audit")
        # Serialize MAX(seq)+1 for this job. Callers that FOR UPDATE the job
        # row already exclude other mutators; this lock also covers appends
        # that only touch the audit stream (clone-of-source).
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_AUDIT_SEQ_LOCK_CLASS, str(job_id)),
        )
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_AUDIT_TENANT_SEQ_LOCK_CLASS, str(tenant_id)),
        )
        event_id = uuid4()
        stored_detail: dict[str, Any] = detail_obj
        payload_key_id = None
        if hasattr(self, "_seal_dict"):
            stored_detail, payload_key_id = self._seal_dict(
                conn,
                tenant_id=str(tenant_id),
                plaintext=detail_obj,
                aad=audit_event_aad(event_id),
                job_id=job_id,
                blob_kind="other",
            )
        row = conn.execute(
            """
            INSERT INTO job_audit_events (
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at,
                tenant_seq, payload_key_id
            )
            SELECT
                %s, %s, %s,
                COALESCE((
                    SELECT MAX(e.seq) FROM job_audit_events e WHERE e.job_id = %s
                ), 0) + 1,
                %s, %s, %s, %s, %s, %s,
                COALESCE((
                    SELECT MAX(t.tenant_seq)
                    FROM job_audit_events t
                    WHERE t.tenant_id = %s
                ), 0) + 1,
                %s
            RETURNING
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at,
                tenant_seq, payload_key_id
            """,
            (
                event_id,
                tenant_id,
                job_id,
                job_id,
                event_type,
                from_status,
                to_status,
                actor,
                Jsonb(stored_detail),
                ts,
                tenant_id,
                payload_key_id,
            ),
        ).fetchone()
        assert row is not None
        # Live progress: same transaction as mutation so NOTIFY is ordered.
        if job_row is not None:
            self._notify_job_progress(
                conn,
                job_row=job_row,
                event_type=event_type,
                from_status=from_status,
                actor=actor,
                now=ts,
            )
        return self._row_to_audit(row, conn=conn, reader_tenant=str(tenant_id))

    def _notify_job_progress(
        self,
        conn: Any,
        *,
        job_row: dict[str, Any],
        event_type: str,
        from_status: str | None,
        actor: str | None,
        now: datetime,
    ) -> None:
        """pg_notify compact progress payload for SSE subscribers."""
        labs = job_row.get("labels") or []
        if isinstance(labs, str):
            labs = []
        # Keep payload under NOTIFY size limit. Secrets stay in the sealed
        # durable row, not on the notify bus.
        payload: dict[str, Any] = {
            "v": 1,
            "event": event_type,
            "tenant_id": job_row["tenant_id"],
            "job_id": str(job_row["id"]),
            "status": job_row["status"],
            "from_status": from_status,
            "attempts": int(job_row.get("attempts") or 0),
            "labels": list(labs)[:32],
            "checkpoint": None,
            "result": None,
            "last_error": None,
            "actor": actor,
            "ts": now.isoformat(),
            "snapshot": False,
        }
        raw = json.dumps(payload, default=str, separators=(",", ":"))
        if len(raw.encode("utf-8")) > _NOTIFY_MAX_BYTES:
            payload["labels"] = []
            raw = json.dumps(payload, default=str, separators=(",", ":"))
        try:
            conn.execute(
                "SELECT pg_notify(%s, %s)",
                (JOB_EVENTS_CHANNEL, raw),
            )
        except Exception:  # noqa: BLE001 — never fail mutation for notify
            logger = __import__("logging").getLogger(__name__)
            logger.exception("pg_notify job progress failed")

    def _rehome_job_audit_events(
        self,
        conn: Any,
        *,
        from_tenant: str,
        to_tenant: str,
        job_ids: list[UUID] | None = None,
    ) -> int:
        """Move sealed history for transferred jobs onto *to_tenant*.

        Offboarding the source tenant deletes ``job_audit_events`` stamped
        with that tenant_id. Events for jobs that now live elsewhere must
        be restamped (and resealed under the dest DEK) first so dest
        list/get survive the source wipe. ``tenant_seq`` is reassigned
        after dest's current max so the dest cursor stays unique.
        """
        if not from_tenant or not to_tenant or from_tenant == to_tenant:
            return 0
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_AUDIT_TENANT_SEQ_LOCK_CLASS, str(to_tenant)),
        )
        if job_ids is not None and not job_ids:
            return 0
        if job_ids is None:
            rows = conn.execute(
                """
                SELECT event_id, tenant_id, job_id, detail, created_at
                FROM job_audit_events
                WHERE tenant_id = %s
                ORDER BY tenant_seq ASC NULLS LAST, created_at ASC, event_id ASC
                FOR UPDATE
                """,
                (from_tenant,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT event_id, tenant_id, job_id, detail, created_at
                FROM job_audit_events
                WHERE tenant_id = %s
                  AND job_id = ANY(%s)
                ORDER BY tenant_seq ASC NULLS LAST, created_at ASC, event_id ASC
                FOR UPDATE
                """,
                (from_tenant, list(job_ids)),
            ).fetchall()
        if not rows:
            return 0
        conn.execute(
            """
            UPDATE job_audit_events
            SET tenant_seq = NULL
            WHERE event_id = ANY(%s)
            """,
            ([r["event_id"] for r in rows],),
        )
        max_row = conn.execute(
            """
            SELECT COALESCE(MAX(tenant_seq), 0)::bigint AS n
            FROM job_audit_events
            WHERE tenant_id = %s
            """,
            (to_tenant,),
        ).fetchone()
        nxt = int(max_row["n"] if max_row else 0)
        moved = 0
        for row in rows:
            nxt += 1
            opened = self._open_audit_detail(
                row.get("detail") or {},
                event_id=row["event_id"],
                event_tenant=str(row["tenant_id"]),
                reader_tenant=to_tenant,
                conn=conn,
            )
            stored = opened
            kid = None
            if hasattr(self, "_seal_dict"):
                stored, kid = self._seal_dict(
                    conn,
                    tenant_id=to_tenant,
                    plaintext=opened if isinstance(opened, dict) else {},
                    aad=audit_event_aad(row["event_id"]),
                    job_id=row["job_id"],
                    blob_kind="other",
                    force_new=True,
                )
            conn.execute(
                """
                UPDATE job_audit_events
                SET tenant_id = %s,
                    tenant_seq = %s,
                    detail = %s,
                    payload_key_id = %s
                WHERE event_id = %s
                """,
                (
                    to_tenant,
                    nxt,
                    Jsonb(stored) if isinstance(stored, dict) else Jsonb({}),
                    kid,
                    row["event_id"],
                ),
            )
            moved += 1
        return moved
