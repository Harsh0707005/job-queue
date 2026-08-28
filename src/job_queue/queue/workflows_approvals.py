"""Workflow approval steps: park until an operator approves or rejects.

An ``approval`` step does not enqueue a worker job. The wait lives on the
step row, so a coordinator death or a sibling lease reclaim cannot lose
it or run it twice. Approve completes the step and continues; reject (or
a missed deadline) fails the step and starts reverse compensate.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar
from uuid import UUID

from psycopg.errors import DeadlockDetected
from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    WorkflowRunStatus,
    WorkflowStepDirection,
    WorkflowStepStatus,
)
from job_queue.queue._common import ApprovalError, _as_utc, _utcnow
from job_queue.queue.workflows import _RUN_COLUMNS, _STEP_COLUMNS


APPROVAL_KINDS = frozenset({"approval", "wait_approval", "approve"})

_T = TypeVar("_T")
_DEADLOCK_RETRIES = 8


def _run_avoiding_deadlock(op: Callable[[], _T]) -> _T:
    """Retry a txn that hit 40P01 so the operator never sees a raw DB error."""
    last: DeadlockDetected | None = None
    for attempt in range(_DEADLOCK_RETRIES):
        try:
            return op()
        except DeadlockDetected as exc:
            last = exc
            time.sleep(0.01 * (attempt + 1))
    raise ApprovalError(
        "approval lost a lock race; retry the decision"
    ) from last


def apply_approval_fields(
    out: dict[str, Any], raw: dict[str, Any], *, label: str
) -> None:
    if raw.get("message") is not None:
        out["message"] = str(raw["message"])
    if raw.get("timeout_seconds") is not None:
        try:
            timeout = float(raw["timeout_seconds"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label}.timeout_seconds must be a number") from exc
        if timeout < 0:
            raise ValueError(f"{label}.timeout_seconds must be >= 0")
        out["timeout_seconds"] = timeout
    if raw.get("deadline_at") is not None:
        out["deadline_at"] = raw["deadline_at"]


def approval_deadline(sdef: dict[str, Any], *, now: Any) -> Any:
    """Optional wake instant. ``None`` means wait until a human decides."""
    seconds = None
    if sdef.get("timeout_seconds") is not None:
        try:
            seconds = float(sdef["timeout_seconds"])
        except (TypeError, ValueError) as exc:
            raise ValueError("approval.timeout_seconds must be a number") from exc
        if seconds < 0:
            raise ValueError("approval.timeout_seconds must be >= 0")
    abs_dl = None
    if sdef.get("deadline_at") is not None:
        raw = sdef["deadline_at"]
        if hasattr(raw, "isoformat"):
            abs_dl = _as_utc(raw)
        else:
            s = str(raw).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            abs_dl = _as_utc(datetime.fromisoformat(s))
    if seconds is None and abs_dl is None:
        return None
    rel = None if seconds is None else now + timedelta(seconds=seconds)
    if rel is None:
        return abs_dl
    if abs_dl is None:
        return rel
    return min(_as_utc(rel), _as_utc(abs_dl))


class WorkflowsApprovalsMixin:
    def approve_workflow(
        self,
        run_id: UUID,
        *,
        tenant_id: str,
        step: str | int | None = None,
        step_id: UUID | None = None,
        decided_by: str | None = None,
        comment: str | None = None,
    ) -> dict[str, Any]:
        """Approve a parked approval step so the saga continues (idempotent)."""
        return self._decide_workflow_approval(
            run_id,
            tenant_id=tenant_id,
            decision="approved",
            step=step,
            step_id=step_id,
            decided_by=decided_by,
            comment=comment,
        )

    def reject_workflow(
        self,
        run_id: UUID,
        *,
        tenant_id: str,
        step: str | int | None = None,
        step_id: UUID | None = None,
        reason: str | None = None,
        decided_by: str | None = None,
    ) -> dict[str, Any]:
        """Reject a parked approval step and start reverse compensate."""
        return self._decide_workflow_approval(
            run_id,
            tenant_id=tenant_id,
            decision="rejected",
            step=step,
            step_id=step_id,
            decided_by=decided_by,
            reason=reason,
        )

    def _decide_workflow_approval(
        self,
        run_id: UUID,
        *,
        tenant_id: str,
        decision: str,
        step: str | int | None = None,
        step_id: UUID | None = None,
        decided_by: str | None = None,
        comment: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if decision not in ("approved", "rejected", "timeout"):
            raise ValueError("decision must be approved, rejected, or timeout")

        def _once() -> dict[str, Any]:
            now = _utcnow()
            with transaction(self._pool) as conn:
                # Always lock the run first, then the step. The timeout tick
                # uses the same order so a simultaneous human decide cannot
                # deadlock (40P01) against process_workflow_approval_timeouts.
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
                step_row = self._lock_approval_step(
                    conn,
                    run=run,
                    step=step,
                    step_id=step_id,
                )
                return self._apply_approval_decision_locked(
                    conn,
                    run=run,
                    step=step_row,
                    decision=decision,
                    decided_by=decided_by,
                    comment=comment,
                    reason=reason,
                    now=now,
                )

        return _run_avoiding_deadlock(_once)

    def list_awaiting_workflow_approvals(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Any]:
        """List parked approval steps for *tenant_id* (deadline first)."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        tenant_id = str(tenant_id).strip()
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE tenant_id = %s
                  AND direction = %s
                  AND status = %s
                ORDER BY deadline_at ASC NULLS LAST, created_at ASC
                LIMIT %s OFFSET %s
                """,
                (
                    tenant_id,
                    WorkflowStepDirection.FORWARD.value,
                    WorkflowStepStatus.AWAITING_APPROVAL.value,
                    limit,
                    offset,
                ),
            ).fetchall()
        return [self._row_to_workflow_step(r) for r in rows]

    def process_workflow_approval_timeouts(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Reject parked approval steps whose deadline has passed.

        Each miss is its own transaction so one bad run cannot roll back
        the rest. A late tick after a coordinator death still rejects once.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        params: list[Any] = [WorkflowStepStatus.AWAITING_APPROVAL.value]
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
                if self._timeout_one_approval_step(sid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                continue
        return fixed

    def _timeout_one_approval_step(self, step_id: UUID) -> bool:
        def _once() -> bool:
            now = _utcnow()
            with transaction(self._pool) as conn:
                peek = conn.execute(
                    """
                    SELECT run_id, status, deadline_at
                    FROM workflow_step_runs
                    WHERE id = %s
                    """,
                    (step_id,),
                ).fetchone()
                if peek is None:
                    return False
                if peek["status"] != WorkflowStepStatus.AWAITING_APPROVAL.value:
                    return False
                if (
                    peek.get("deadline_at") is None
                    or _as_utc(peek["deadline_at"]) > now
                ):
                    return False
                # Run first, then step — same order as approve_workflow.
                run = conn.execute(
                    f"""
                    SELECT {_RUN_COLUMNS}
                    FROM workflow_runs
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (peek["run_id"],),
                ).fetchone()
                if run is None:
                    return False
                if run["status"] != WorkflowRunStatus.RUNNING.value:
                    return False
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
                if step["status"] != WorkflowStepStatus.AWAITING_APPROVAL.value:
                    return False
                if (
                    step.get("deadline_at") is None
                    or _as_utc(step["deadline_at"]) > now
                ):
                    return False
                self._apply_approval_decision_locked(
                    conn,
                    run=run,
                    step=step,
                    decision="timeout",
                    reason="approval deadline exceeded",
                    now=now,
                )
                return True

        try:
            return _run_avoiding_deadlock(_once)
        except ApprovalError:
            # Next control tick will retry; do not fail the whole sweep.
            return False

    def _park_approval_step(
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
        """Park an approval step, or reject immediately if already due."""
        _ = tenant_id
        idx = int(step_row["step_index"])
        sdef = steps_def[idx]
        deadline = approval_deadline(sdef, now=now)
        if step_row["status"] in (
            WorkflowStepStatus.COMPLETED.value,
            WorkflowStepStatus.FAILED.value,
            WorkflowStepStatus.SKIPPED.value,
        ):
            return step_row
        message = sdef.get("message")
        parked_result = {
            "kind": "approval",
            "awaiting": True,
        }
        if message is not None:
            parked_result["message"] = str(message)
        updated = conn.execute(
            f"""
            UPDATE workflow_step_runs
            SET status = %s,
                started_at = COALESCE(started_at, %s),
                deadline_at = %s,
                result = %s,
                updated_at = %s
            WHERE id = %s
              AND status IN (%s, %s, %s)
            RETURNING {_STEP_COLUMNS}
            """,
            (
                WorkflowStepStatus.AWAITING_APPROVAL.value,
                now,
                deadline,
                Jsonb(parked_result),
                now,
                step_row["id"],
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.AWAITING_APPROVAL.value,
                WorkflowStepStatus.RUNNING.value,
            ),
        ).fetchone()
        step = updated or step_row
        if deadline is None or _as_utc(deadline) > _as_utc(now):
            _ = (queue_name, max_attempts, visibility_timeout_seconds)
            return step
        # Caller already holds the run (enqueue locks it first). Re-read
        # under that lock; do not lock the step then the run.
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
        return self._apply_approval_decision_locked(
            conn,
            run=run,
            step=step,
            decision="timeout",
            reason="approval deadline exceeded",
            now=now,
        )["step_row"]

    def _apply_approval_decision_locked(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: dict[str, Any],
        decision: str,
        decided_by: str | None = None,
        comment: str | None = None,
        reason: str | None = None,
        now: Any,
    ) -> dict[str, Any]:
        status = str(step["status"])
        existing = step.get("result")
        if isinstance(existing, str):
            import json

            try:
                existing = json.loads(existing)
            except json.JSONDecodeError:
                existing = None
        existing_decision = (
            existing.get("decision") if isinstance(existing, dict) else None
        )
        if status == WorkflowStepStatus.COMPLETED.value:
            if existing_decision == "approved" and decision == "approved":
                return {
                    "run": self._row_to_workflow_run(run, conn=conn),
                    "step": self._row_to_workflow_step(step),
                    "step_row": step,
                }
            raise ApprovalError(
                f"workflow step {step['name']!r} is already completed"
            )
        if status == WorkflowStepStatus.FAILED.value:
            if existing_decision in ("rejected", "timeout") and decision in (
                "rejected",
                "timeout",
            ):
                return {
                    "run": self._row_to_workflow_run(run, conn=conn),
                    "step": self._row_to_workflow_step(step),
                    "step_row": step,
                }
            raise ApprovalError(
                f"workflow step {step['name']!r} is already failed"
            )
        if status == WorkflowStepStatus.SKIPPED.value:
            raise ApprovalError(
                f"workflow step {step['name']!r} was skipped"
            )
        if status != WorkflowStepStatus.AWAITING_APPROVAL.value:
            raise ApprovalError(
                f"workflow step {step['name']!r} is not awaiting approval "
                f"(status={status})"
            )
        if run["status"] != WorkflowRunStatus.RUNNING.value:
            raise ApprovalError(
                f"workflow run {run['id']} is {run['status']}"
            )

        result: dict[str, Any] = {
            "kind": "approval",
            "decision": decision,
        }
        if decided_by:
            result["decided_by"] = str(decided_by)
        if comment:
            result["comment"] = str(comment)
        if reason:
            result["reason"] = str(reason)
        if isinstance(existing, dict) and existing.get("message"):
            result["message"] = existing["message"]

        if decision == "approved":
            self._complete_forward_step(
                conn, run=run, step=step, result=result, now=now
            )
        else:
            conn.execute(
                """
                UPDATE workflow_step_runs
                SET result = %s, updated_at = %s
                WHERE id = %s
                """,
                (Jsonb(result), now, step["id"]),
            )
            err = (
                reason
                or (
                    "approval deadline exceeded"
                    if decision == "timeout"
                    else "approval rejected"
                )
            )
            self._fail_forward_step(conn, run=run, step=step, error=str(err), now=now)

        fresh_run = conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
            (run["id"],),
        ).fetchone()
        fresh_step = conn.execute(
            f"SELECT {_STEP_COLUMNS} FROM workflow_step_runs WHERE id = %s",
            (step["id"],),
        ).fetchone()
        assert fresh_run is not None and fresh_step is not None
        return {
            "run": self._row_to_workflow_run(fresh_run, conn=conn),
            "step": self._row_to_workflow_step(fresh_step),
            "step_row": fresh_step,
        }

    def _lock_approval_step(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: str | int | None,
        step_id: UUID | None,
    ) -> dict[str, Any]:
        run_id = run["id"]
        if step_id is not None:
            row = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE id = %s AND run_id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (step_id, run_id, run["tenant_id"]),
            ).fetchone()
            if row is None:
                raise ApprovalError(
                    f"workflow step {step_id} not found on run {run_id}"
                )
            return row
        if step is not None and not isinstance(step, bool):
            if isinstance(step, int) or (
                isinstance(step, str) and step.strip().lstrip("-").isdigit()
            ):
                idx = int(step)
                row = conn.execute(
                    f"""
                    SELECT {_STEP_COLUMNS}
                    FROM workflow_step_runs
                    WHERE run_id = %s
                      AND step_index = %s
                      AND direction = %s
                    FOR UPDATE
                    """,
                    (run_id, idx, WorkflowStepDirection.FORWARD.value),
                ).fetchone()
                if row is None:
                    raise ApprovalError(
                        f"workflow step index {idx} not found on run {run_id}"
                    )
                return row
            name = str(step).strip()
            rows = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND name = %s
                  AND direction = %s
                ORDER BY step_index ASC
                FOR UPDATE
                """,
                (run_id, name, WorkflowStepDirection.FORWARD.value),
            ).fetchall()
            if not rows:
                raise ApprovalError(
                    f"workflow step {name!r} not found on run {run_id}"
                )
            if len(rows) > 1:
                awaiting = [
                    r
                    for r in rows
                    if r["status"] == WorkflowStepStatus.AWAITING_APPROVAL.value
                ]
                if len(awaiting) == 1:
                    return awaiting[0]
                raise ApprovalError(
                    f"workflow step name {name!r} is ambiguous on run {run_id}"
                )
            return rows[0]
        rows = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = %s
              AND status = %s
            ORDER BY step_index ASC
            FOR UPDATE
            """,
            (
                run_id,
                WorkflowStepDirection.FORWARD.value,
                WorkflowStepStatus.AWAITING_APPROVAL.value,
            ),
        ).fetchall()
        if len(rows) == 1:
            return rows[0]
        if len(rows) > 1:
            raise ApprovalError(
                f"workflow run {run_id} has {len(rows)} parked approvals; "
                "pass step or step_id"
            )
        # Idempotent re-decide after the gate already resolved.
        steps_def = self._parse_steps_def(run, conn)
        approval_idxs = [
            i
            for i, sdef in enumerate(steps_def)
            if str(sdef.get("kind") or "") in APPROVAL_KINDS
        ]
        if len(approval_idxs) == 1:
            row = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND step_index = %s
                  AND direction = %s
                FOR UPDATE
                """,
                (
                    run_id,
                    approval_idxs[0],
                    WorkflowStepDirection.FORWARD.value,
                ),
            ).fetchone()
            if row is not None:
                return row
        raise ApprovalError(
            f"workflow run {run_id} has no step awaiting approval"
        )
