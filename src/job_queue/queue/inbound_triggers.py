"""Tenant-scoped signed inbound HTTP triggers."""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from job_queue.db import transaction
from job_queue.models import InboundTrigger
from job_queue.queue._common import _utcnow


TRIGGER_KINDS = ("job", "dag", "workflow", "signal")
_TRIGGER_COLUMNS = (
    "id, tenant_id, name, secret, kinds, enabled, created_at, updated_at"
)
_NONCE_TTL_SECONDS = 600


def _normalize_kinds(kinds: Sequence[str] | None) -> list[str]:
    allowed = set(TRIGGER_KINDS)
    if kinds is None:
        return list(TRIGGER_KINDS)
    out: list[str] = []
    seen: set[str] = set()
    for raw in kinds:
        k = str(raw or "").strip().lower()
        if k not in allowed:
            raise ValueError(f"unsupported trigger kind {raw!r}")
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    if not out:
        raise ValueError("kinds must be non-empty")
    return out


def _row_to_trigger(row: Any) -> InboundTrigger:
    kinds = row["kinds"] or []
    if isinstance(kinds, str):
        kinds = [kinds]
    return InboundTrigger(
        id=row["id"],
        tenant_id=row["tenant_id"],
        name=row["name"],
        secret=row["secret"],
        kinds=tuple(kinds),
        enabled=bool(row["enabled"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class InboundTriggersMixin:
    def create_inbound_trigger(
        self,
        *,
        tenant_id: str,
        name: str,
        secret: str | None = None,
        kinds: Sequence[str] | None = None,
        enabled: bool = True,
        trigger_id: UUID | None = None,
    ) -> InboundTrigger:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        name = str(name or "").strip()
        if not name or len(name) > 128:
            raise ValueError("name must be 1..128 characters")
        kind_list = _normalize_kinds(kinds)
        if secret is None or not str(secret).strip():
            secret = secrets.token_urlsafe(32)
        secret = str(secret)
        if len(secret) < 16:
            raise ValueError("secret must be at least 16 characters")
        now = _utcnow()
        tid = trigger_id or uuid4()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; trigger create rejected"
                )
            try:
                row = conn.execute(
                    f"""
                    INSERT INTO inbound_triggers (
                        id, tenant_id, name, secret, kinds, enabled,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING {_TRIGGER_COLUMNS}
                    """,
                    (
                        tid,
                        tenant_id,
                        name,
                        secret,
                        kind_list,
                        bool(enabled),
                        now,
                        now,
                    ),
                ).fetchone()
            except Exception as exc:
                msg = str(exc).lower()
                if "unique" in msg or "duplicate" in msg:
                    raise ValueError(
                        f"trigger name {name!r} already exists for tenant {tenant_id!r}"
                    ) from exc
                raise
        assert row is not None
        return _row_to_trigger(row)

    def get_inbound_trigger(
        self, trigger_id: UUID, *, tenant_id: str | None = None
    ) -> InboundTrigger | None:
        with self._pool.connection() as conn:
            if tenant_id:
                row = conn.execute(
                    f"""
                    SELECT {_TRIGGER_COLUMNS}
                    FROM inbound_triggers
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (trigger_id, tenant_id),
                ).fetchone()
            else:
                row = conn.execute(
                    f"""
                    SELECT {_TRIGGER_COLUMNS}
                    FROM inbound_triggers WHERE id = %s
                    """,
                    (trigger_id,),
                ).fetchone()
        return _row_to_trigger(row) if row else None

    def list_inbound_triggers(self, *, tenant_id: str) -> list[InboundTrigger]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_TRIGGER_COLUMNS}
                FROM inbound_triggers
                WHERE tenant_id = %s
                ORDER BY created_at ASC, id ASC
                """,
                (tenant_id,),
            ).fetchall()
        return [_row_to_trigger(r) for r in rows]

    def rotate_inbound_trigger_secret(
        self,
        trigger_id: UUID,
        *,
        tenant_id: str,
        secret: str | None = None,
    ) -> InboundTrigger:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if secret is None or not str(secret).strip():
            secret = secrets.token_urlsafe(32)
        secret = str(secret)
        if len(secret) < 16:
            raise ValueError("secret must be at least 16 characters")
        now = _utcnow()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; rotate rejected"
                )
            row = conn.execute(
                f"""
                UPDATE inbound_triggers
                SET secret = %s, updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_TRIGGER_COLUMNS}
                """,
                (secret, now, trigger_id, tenant_id),
            ).fetchone()
        if row is None:
            raise ValueError("trigger not found")
        return _row_to_trigger(row)

    def set_inbound_trigger_enabled(
        self, trigger_id: UUID, *, tenant_id: str, enabled: bool
    ) -> InboundTrigger:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE inbound_triggers
                SET enabled = %s, updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_TRIGGER_COLUMNS}
                """,
                (bool(enabled), now, trigger_id, tenant_id),
            ).fetchone()
        if row is None:
            raise ValueError("trigger not found")
        return _row_to_trigger(row)

    def delete_inbound_trigger(
        self, trigger_id: UUID, *, tenant_id: str
    ) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM inbound_triggers
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (trigger_id, tenant_id),
            ).fetchone()
        return row is not None

    def process_inbound_nonces(self, *, limit: int = 500) -> int:
        """Drop used nonces older than the replay window."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        cutoff = _utcnow() - timedelta(seconds=_NONCE_TTL_SECONDS)
        with transaction(self._pool) as conn:
            rows = conn.execute(
                """
                DELETE FROM inbound_trigger_nonces
                WHERE seen_at < %s
                AND ctid IN (
                    SELECT ctid FROM inbound_trigger_nonces
                    WHERE seen_at < %s
                    LIMIT %s
                )
                RETURNING trigger_id
                """,
                (cutoff, cutoff, int(limit)),
            ).fetchall()
        return len(rows)

    def fire_inbound_trigger(
        self,
        trigger_id: UUID,
        body: dict[str, Any],
        *,
        nonce: str,
    ) -> dict[str, Any]:
        """Execute a verified inbound POST. Tenant comes from the trigger.

        The nonce row and the job / DAG / workflow / signal are written in
        the same Postgres transaction. A crash after the nonce insert and
        before the work is created rolls both back, so a retry is not
        treated as a replay.
        """
        action = str(body.get("action") or body.get("kind") or "").strip().lower()
        if action not in TRIGGER_KINDS:
            raise ValueError("action must be one of job, dag, workflow, signal")
        nonce = str(nonce or "").strip()
        if not nonce or len(nonce) > 128:
            raise ValueError("nonce must be 1..128 characters")
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {_TRIGGER_COLUMNS}
                FROM inbound_triggers
                WHERE id = %s
                FOR UPDATE
                """,
                (trigger_id,),
            ).fetchone()
            if row is None:
                raise LookupError("trigger not found")
            trig = _row_to_trigger(row)
            if not trig.enabled:
                raise PermissionError("trigger is disabled")
            if self._is_tenant_offboarding(conn, trig.tenant_id):
                raise ValueError(
                    f"tenant {trig.tenant_id!r} is offboarding; inbound rejected"
                )
            if action not in trig.kinds:
                raise PermissionError(f"trigger does not allow action {action!r}")
            body_tenant = body.get("tenant_id")
            if body_tenant is not None and str(body_tenant) != trig.tenant_id:
                raise PermissionError("tenant_id does not match trigger")
            inserted = conn.execute(
                """
                INSERT INTO inbound_trigger_nonces (trigger_id, nonce, seen_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (trigger_id, nonce) DO NOTHING
                RETURNING nonce
                """,
                (trigger_id, nonce, _utcnow()),
            ).fetchone()
            if inserted is None:
                raise FileExistsError("replayed nonce")
            out = self._dispatch_inbound_action_on_conn(
                conn, action, tenant_id=trig.tenant_id, body=body
            )
        if out.get("action") == "workflow" and out.get("run") is not None:
            run = out["run"]
            out["steps"] = self.list_workflow_steps(
                run.id, tenant_id=run.tenant_id
            )
        return out

    def _dispatch_inbound_action_on_conn(
        self,
        conn: Any,
        action: str,
        *,
        tenant_id: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        if action == "job":
            job = self._inbound_enqueue_job_on_conn(
                conn, tenant_id=tenant_id, body=body
            )
            return {"action": "job", "job": job}
        if action == "dag":
            jobs = list(body.get("jobs") or [])
            batch_key = str(body.get("batch_key") or "")
            queue_name = str(body.get("queue_name") or "default")
            if not batch_key.strip():
                raise ValueError("batch_key is required")
            if not jobs:
                raise ValueError("jobs must be a non-empty list")
            specs = self._normalize_dag_specs(jobs)
            result = self._enqueue_dag_on_conn(
                conn,
                tenant_id=tenant_id,
                queue_name=queue_name,
                batch_key=batch_key.strip(),
                specs=specs,
                now=_utcnow(),
            )
            return {"action": "dag", "dag": result}
        if action == "workflow":
            steps = body.get("steps")
            if not isinstance(steps, list) or not steps:
                raise ValueError("steps must be a non-empty list")
            from job_queue.queue.workflows import DEFAULT_MAX_COMPENSATE_RETRIES

            normalized = self._expand_and_normalize_steps(steps)
            self._apply_workflow_ttl_defaults(
                normalized,
                ttl_seconds=None,
                expires_at=None,
                expiry_action=None,
            )
            row = self._start_workflow_on_conn(
                conn,
                tenant_id=tenant_id,
                normalized=normalized,
                name=body.get("name"),
                queue_name=str(body.get("queue_name") or "default"),
                max_attempts=int(body.get("max_attempts") or 3),
                visibility_timeout_seconds=int(
                    body.get("visibility_timeout_seconds") or 30
                ),
                max_compensate_retries=DEFAULT_MAX_COMPENSATE_RETRIES,
                run_id=uuid4(),
                now=_utcnow(),
                lock_tenant=True,
            )
            return {
                "action": "workflow",
                "run": self._row_to_workflow_run(row),
            }
        if action == "signal":
            raw_run = body.get("run_id")
            if not raw_run:
                raise ValueError("run_id is required")
            name = body.get("signal") or body.get("name")
            if name is None or not str(name).strip():
                raise ValueError("signal name is required")
            from job_queue.queue.workflows_signals import normalize_signal_name

            payload = body.get("signal_payload")
            if payload is None:
                payload = body.get("payload")
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                raise ValueError("payload must be an object")
            out = self._send_workflow_signal_on_conn(
                conn,
                UUID(str(raw_run)),
                tenant_id=tenant_id,
                name=normalize_signal_name(name),
                payload=payload,
                now=_utcnow(),
            )
            return {"action": "signal", **out}
        raise ValueError(f"unsupported action {action!r}")

    def _inbound_enqueue_job_on_conn(
        self, conn: Any, *, tenant_id: str, body: dict[str, Any]
    ) -> Any:
        from job_queue.queue._common import (
            _normalize_labels,
            _normalize_skills,
            _utcnow,
        )
        from uuid import uuid4 as _uuid4

        payload = dict(body.get("payload") or {})
        now = _utcnow()
        return self._enqueue_on_conn(
            conn,
            tenant_id=tenant_id,
            payload=payload,
            queue_name=str(body.get("queue_name") or "default"),
            priority=int(body.get("priority") or 0),
            max_attempts=int(body.get("max_attempts") or 3),
            visibility_timeout_seconds=int(
                body.get("visibility_timeout_seconds") or 30
            ),
            max_runtime_seconds=int(body.get("max_runtime_seconds") or 3600),
            retry_backoff_base_seconds=int(
                body.get("retry_backoff_base_seconds") or 1
            ),
            retry_backoff_max_seconds=int(
                body.get("retry_backoff_max_seconds") or 300
            ),
            run_at=now,
            now=now,
            jid=_uuid4(),
            dedupe_key=None,
            dep_ids=[],
            label_list=_normalize_labels(body.get("labels")),
            skill_list=_normalize_skills(body.get("required_skills")),
            cost_map=None,
            deadline=None,
            estimated_duration_seconds=None,
            preemptible=True,
            ttl_seconds=None,
            expires_at=None,
            expiry_action=None,
            prefer_lw=False,
            payload_kind=self._payload_kind(payload),
            cloned_from_job_id=None,
            max_worker_hops=None,
            quarantine_action=None,
            coal_key=(
                str(body["coalesce_key"]).strip()
                if body.get("coalesce_key")
                else None
            ),
        )
