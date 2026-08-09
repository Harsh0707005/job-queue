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
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
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
            "schedule_id": _uuid(row.get("schedule_id")),
            "depends_on": dep_out,
            "labels": list(row.get("labels") or []),
            "required_skills": list(row.get("required_skills") or []),
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
        event_id = uuid4()
        row = conn.execute(
            """
            INSERT INTO job_audit_events (
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at
            )
            SELECT
                %s, %s, %s,
                COALESCE((
                    SELECT MAX(e.seq) FROM job_audit_events e WHERE e.job_id = %s
                ), 0) + 1,
                %s, %s, %s, %s, %s, %s
            RETURNING
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at
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
                Jsonb(detail_obj),
                ts,
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
        return self._row_to_audit(row)

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
        cp = job_row.get("checkpoint")
        result = job_row.get("result")
        # Keep payload under NOTIFY size limit.
        payload: dict[str, Any] = {
            "v": 1,
            "event": event_type,
            "tenant_id": job_row["tenant_id"],
            "job_id": str(job_row["id"]),
            "status": job_row["status"],
            "from_status": from_status,
            "attempts": int(job_row.get("attempts") or 0),
            "labels": list(labs)[:32],
            "checkpoint": cp,
            "result": result
            if job_row["status"] == JobStatus.COMPLETED.value
            else None,
            "last_error": ((job_row.get("last_error") or "")[:500] or None),
            "actor": actor,
            "ts": now.isoformat(),
            "snapshot": False,
        }
        raw = json.dumps(payload, default=str, separators=(",", ":"))
        if len(raw.encode("utf-8")) > _NOTIFY_MAX_BYTES:
            # Drop large fields first.
            payload["checkpoint"] = {"_truncated": True} if cp is not None else None
            payload["result"] = (
                {"_truncated": True} if payload.get("result") is not None else None
            )
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
