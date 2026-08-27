"""Named workflow signals: early latch, wait steps, deadline fail+undo.

A signal is unique per ``(run_id, name)``. The first send wins; a later
``wait_signal`` step still sees it. Every read/write is tenant-scoped so
one tenant cannot latch or consume another tenant's signals. A wait that
misses ``deadline_at`` fails the forward step and starts reverse compensate.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import workflow_signal_aad
from job_queue.db import transaction
from job_queue.models import (
    WorkflowRunStatus,
    WorkflowSignal,
    WorkflowStepDirection,
    WorkflowStepStatus,
)
from job_queue.queue._common import _as_utc, _utcnow
from job_queue.queue.workflows import _RUN_COLUMNS, _STEP_COLUMNS


def normalize_signal_name(raw: Any, *, label: str = "signal") -> str:
    name = str(raw or "").strip()
    if not name:
        raise ValueError(f"{label} is required")
    if len(name) > 128:
        raise ValueError(f"{label} must be at most 128 characters")
    if any(ord(c) < 32 for c in name):
        raise ValueError(f"{label} must not contain control characters")
    return name


_SIGNAL_COLUMNS = """
    id, run_id, tenant_id, name, payload, payload_key_id, created_at
"""


class WorkflowSignalsMixin:
    def send_workflow_signal(
        self,
        run_id: UUID,
        *,
        tenant_id: str,
        name: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Latch a named signal for this tenant's run.

        Idempotent: the first payload for ``(run, name)`` sticks. If a
        ``wait_signal`` step is already parked, it completes in this
        transaction. A send from the wrong tenant is not-found (no leak).
        """
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        name = normalize_signal_name(name)
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        now = _utcnow()
        with transaction(self._pool) as conn:
            return self._send_workflow_signal_on_conn(
                conn,
                run_id,
                tenant_id=tenant_id,
                name=name,
                payload=payload,
                now=now,
            )

    def _send_workflow_signal_on_conn(
        self,
        conn: Any,
        run_id: UUID,
        *,
        tenant_id: str,
        name: str,
        payload: dict[str, Any],
        now: Any,
    ) -> dict[str, Any]:
        run = conn.execute(
            f"""
            SELECT {_RUN_COLUMNS}
            FROM workflow_runs
            WHERE id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (run_id, tenant_id),
        ).fetchone()
        if run is None:
            raise ValueError(
                f"workflow run {run_id} not found for tenant {tenant_id!r}"
            )
        sealed, key_id = self._seal_signal_payload(
            conn,
            tenant_id=tenant_id,
            run_id=run_id,
            name=name,
            payload=dict(payload),
        )
        inserted = conn.execute(
            f"""
            INSERT INTO workflow_signals (
                id, run_id, tenant_id, name, payload, payload_key_id,
                created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (run_id, name) DO NOTHING
            RETURNING {_SIGNAL_COLUMNS}
            """,
            (
                uuid4(),
                run_id,
                tenant_id,
                name,
                Jsonb(sealed) if sealed is not None else None,
                key_id,
                now,
            ),
        ).fetchone()
        created = inserted is not None
        row = inserted or conn.execute(
            f"""
            SELECT {_SIGNAL_COLUMNS}
            FROM workflow_signals
            WHERE run_id = %s AND tenant_id = %s AND name = %s
            """,
            (run_id, tenant_id, name),
        ).fetchone()
        assert row is not None
        unblocked = 0
        if run["status"] == WorkflowRunStatus.RUNNING.value:
            unblocked = self._complete_waiters_for_signal(
                conn,
                run=run,
                name=name,
                signal_row=row,
                now=now,
            )
        return {
            "signal": self._row_to_workflow_signal(row, conn=conn),
            "created": created,
            "unblocked": unblocked,
        }

    def list_workflow_signals(
        self, run_id: UUID, *, tenant_id: str
    ) -> list[WorkflowSignal]:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        with self._pool.connection() as conn:
            run = conn.execute(
                "SELECT id FROM workflow_runs WHERE id = %s AND tenant_id = %s",
                (run_id, tenant_id),
            ).fetchone()
            if run is None:
                raise ValueError(
                    f"workflow run {run_id} not found for tenant {tenant_id!r}"
                )
            rows = conn.execute(
                f"""
                SELECT {_SIGNAL_COLUMNS}
                FROM workflow_signals
                WHERE run_id = %s AND tenant_id = %s
                ORDER BY created_at ASC, name ASC
                """,
                (run_id, tenant_id),
            ).fetchall()
        return [self._row_to_workflow_signal(r) for r in rows]

    def get_workflow_signal(
        self, run_id: UUID, *, tenant_id: str, name: str
    ) -> WorkflowSignal | None:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        name = normalize_signal_name(name)
        with self._pool.connection() as conn:
            run = conn.execute(
                "SELECT id FROM workflow_runs WHERE id = %s AND tenant_id = %s",
                (run_id, tenant_id),
            ).fetchone()
            if run is None:
                raise ValueError(
                    f"workflow run {run_id} not found for tenant {tenant_id!r}"
                )
            row = conn.execute(
                f"""
                SELECT {_SIGNAL_COLUMNS}
                FROM workflow_signals
                WHERE run_id = %s AND tenant_id = %s AND name = %s
                """,
                (run_id, tenant_id, name),
            ).fetchone()
        return self._row_to_workflow_signal(row) if row else None

    def process_workflow_signal_timeouts(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Fail parked wait_signal steps whose deadline has passed.

        Each miss is its own transaction so one bad run cannot roll back
        the rest. Missed deadline marks the wait failed and starts reverse
        compensation of completed forwards.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        params: list[Any] = [WorkflowStepStatus.AWAITING_SIGNAL.value]
        tenant_clause = ""
        if tenant_id is not None:
            tenant_clause = "AND s.tenant_id = %s"
            params.append(str(tenant_id).strip())
        params.append(limit)
        with self._pool.connection() as conn:
            ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT s.id
                    FROM workflow_step_runs s
                    WHERE s.status = %s
                      AND s.direction = 'forward'
                      AND s.deadline_at IS NOT NULL
                      AND s.deadline_at <= NOW()
                    {tenant_clause}
                    ORDER BY s.deadline_at ASC
                    LIMIT %s
                    """,
                    tuple(params),
                ).fetchall()
            ]
        fixed = 0
        for sid in ids:
            if fixed >= limit:
                break
            try:
                if self._timeout_one_wait_signal_step(sid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                continue
        return fixed

    def _timeout_one_wait_signal_step(self, step_id: UUID) -> bool:
        now = _utcnow()
        with transaction(self._pool) as conn:
            step = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (step_id,),
            ).fetchone()
            if step is None:
                return False
            if step["status"] != WorkflowStepStatus.AWAITING_SIGNAL.value:
                return False
            if step.get("deadline_at") is None or _as_utc(step["deadline_at"]) > now:
                return False
            run = conn.execute(
                f"""
                SELECT {_RUN_COLUMNS}
                FROM workflow_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (step["run_id"],),
            ).fetchone()
            if run is None:
                return False
            if run["status"] != WorkflowRunStatus.RUNNING.value:
                return False
            steps_def = self._parse_steps_def(run, conn)
            idx = int(step["step_index"])
            sdef = steps_def[idx] if idx < len(steps_def) else {}
            sig_name = str(sdef.get("signal") or step["name"] or "signal")
            self._fail_forward_step(
                conn,
                run=run,
                step=step,
                error=f"signal {sig_name!r} deadline exceeded",
                now=now,
            )
            return True

    def _park_or_complete_wait_signal(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        step_row: dict[str, Any],
        steps_def: list[dict[str, Any]],
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        now: Any,
    ) -> dict[str, Any]:
        """Park a wait_signal step, or complete it if the latch is already set."""
        idx = int(step_row["step_index"])
        sdef = steps_def[idx]
        name = normalize_signal_name(sdef.get("signal"), label="wait_signal.signal")
        deadline = self._wait_signal_deadline(sdef, now=now)
        if step_row["status"] in (
            WorkflowStepStatus.COMPLETED.value,
            WorkflowStepStatus.FAILED.value,
            WorkflowStepStatus.SKIPPED.value,
        ):
            return step_row
        updated = conn.execute(
            f"""
            UPDATE workflow_step_runs
            SET status = %s,
                started_at = COALESCE(started_at, %s),
                deadline_at = %s,
                updated_at = %s
            WHERE id = %s
              AND status IN (%s, %s, %s)
            RETURNING {_STEP_COLUMNS}
            """,
            (
                WorkflowStepStatus.AWAITING_SIGNAL.value,
                now,
                deadline,
                now,
                step_row["id"],
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.AWAITING_SIGNAL.value,
                WorkflowStepStatus.RUNNING.value,
            ),
        ).fetchone()
        step = updated or step_row
        run = conn.execute(
            f"""
            SELECT {_RUN_COLUMNS}
            FROM workflow_runs
            WHERE id = %s
            FOR UPDATE
            """,
            (run_id,),
        ).fetchone()
        if run is None:
            return step
        if run["status"] != WorkflowRunStatus.RUNNING.value:
            return step
        sig = conn.execute(
            f"""
            SELECT {_SIGNAL_COLUMNS}
            FROM workflow_signals
            WHERE run_id = %s AND tenant_id = %s AND name = %s
            """,
            (run_id, tenant_id, name),
        ).fetchone()
        if sig is not None:
            opened = self._open_signal_payload(
                sig.get("payload"),
                tenant_id=tenant_id,
                run_id=run_id,
                name=name,
                conn=conn,
            )
            received = sig.get("created_at")
            self._complete_forward_step(
                conn,
                run=run,
                step=step,
                result={
                    "signal": name,
                    "payload": opened,
                    "received_at": (
                        received.isoformat() if hasattr(received, "isoformat") else received
                    ),
                },
                now=now,
            )
            done = conn.execute(
                f"SELECT {_STEP_COLUMNS} FROM workflow_step_runs WHERE id = %s",
                (step["id"],),
            ).fetchone()
            return done or step
        if deadline is not None and _as_utc(deadline) <= _as_utc(now):
            self._fail_forward_step(
                conn,
                run=run,
                step=step,
                error=f"signal {name!r} deadline exceeded",
                now=now,
            )
            done = conn.execute(
                f"SELECT {_STEP_COLUMNS} FROM workflow_step_runs WHERE id = %s",
                (step["id"],),
            ).fetchone()
            return done or step
        _ = (queue_name, max_attempts, visibility_timeout_seconds)
        return step

    def _complete_waiters_for_signal(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        name: str,
        signal_row: dict[str, Any],
        now: Any,
    ) -> int:
        steps_def = self._parse_steps_def(run, conn)
        waiters = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND tenant_id = %s
              AND direction = %s
              AND status = %s
            FOR UPDATE
            """,
            (
                run["id"],
                run["tenant_id"],
                WorkflowStepDirection.FORWARD.value,
                WorkflowStepStatus.AWAITING_SIGNAL.value,
            ),
        ).fetchall()
        opened = self._open_signal_payload(
            signal_row.get("payload"),
            tenant_id=str(run["tenant_id"]),
            run_id=run["id"],
            name=name,
            conn=conn,
        )
        received = signal_row.get("created_at")
        result = {
            "signal": name,
            "payload": opened,
            "received_at": (
                received.isoformat() if hasattr(received, "isoformat") else received
            ),
        }
        n = 0
        for step in waiters:
            idx = int(step["step_index"])
            sdef = steps_def[idx] if idx < len(steps_def) else {}
            if str(sdef.get("kind") or "") != "wait_signal":
                continue
            try:
                want = normalize_signal_name(sdef.get("signal"))
            except ValueError:
                continue
            if want != name:
                continue
            self._complete_forward_step(
                conn, run=run, step=step, result=result, now=now
            )
            n += 1
        return n

    @staticmethod
    def _wait_signal_deadline(sdef: dict[str, Any], *, now: Any) -> Any:
        deadline = None
        if sdef.get("timeout_seconds") is not None:
            deadline = now + timedelta(seconds=float(sdef["timeout_seconds"]))
        raw = sdef.get("deadline_at")
        if raw is None:
            return deadline
        if hasattr(raw, "isoformat"):
            abs_dl = _as_utc(raw)
        else:
            s = str(raw).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            from datetime import datetime

            abs_dl = _as_utc(datetime.fromisoformat(s))
        if deadline is None:
            return abs_dl
        return min(_as_utc(deadline), abs_dl)

    def _seal_signal_payload(
        self,
        conn: Any,
        *,
        tenant_id: str,
        run_id: UUID,
        name: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, UUID | None]:
        if not payload:
            return None, None
        sealed, key_id = self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext=payload,
            aad=workflow_signal_aad(run_id, name),
        )
        return sealed, key_id

    def _open_signal_payload(
        self,
        payload: Any,
        *,
        tenant_id: str,
        run_id: UUID,
        name: str,
        conn: Any | None = None,
    ) -> dict[str, Any]:
        if payload is None:
            return {}
        opened = self._open_dict(
            payload,
            tenant_id=tenant_id,
            aad=workflow_signal_aad(run_id, name),
            conn=conn,
        )
        return dict(opened) if isinstance(opened, dict) else {}

    def _row_to_workflow_signal(
        self, row: dict[str, Any], *, conn: Any | None = None
    ) -> WorkflowSignal:
        opened = self._open_signal_payload(
            row.get("payload"),
            tenant_id=str(row["tenant_id"]),
            run_id=row["run_id"],
            name=str(row["name"]),
            conn=conn,
        )
        return WorkflowSignal(
            id=row["id"],
            run_id=row["run_id"],
            tenant_id=str(row["tenant_id"]),
            name=str(row["name"]),
            payload=opened,
            created_at=row["created_at"],
        )
