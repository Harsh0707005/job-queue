"""Durable workflow sleep and timer steps.

Sleep parks until ``deadline_at`` then completes and continues. Timer parks
until ``deadline_at`` then fails the forward step and starts reverse
compensate. Neither enqueues a worker job, so a coordinator death or a
lease reclaim cannot lose the wait or run it twice. The wake time lives on
the step row; the control leader (or a lease poll) fires due rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    WorkflowRunStatus,
    WorkflowStepDirection,
    WorkflowStepStatus,
)
from job_queue.queue._common import _as_utc, _utcnow
from job_queue.queue.workflows import _RUN_COLUMNS, _STEP_COLUMNS


SLEEP_KINDS = frozenset({"sleep", "delay"})
TIMER_KINDS = frozenset({"timer", "timeout"})


def parse_wait_seconds(raw: Any, *, label: str) -> float:
    try:
        seconds = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    if seconds < 0:
        raise ValueError(f"{label} must be >= 0")
    return seconds


def parse_absolute_time(raw: Any, *, label: str) -> datetime:
    if hasattr(raw, "isoformat"):
        return _as_utc(raw)
    s = str(raw).strip()
    if not s:
        raise ValueError(f"{label} is required")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return _as_utc(datetime.fromisoformat(s))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp") from exc


def wait_deadline(sdef: dict[str, Any], *, now: Any, kind: str) -> datetime:
    """Wake / timeout instant from duration and/or absolute time.

    When both are set, the earlier instant wins so a timeout cannot miss.
    """
    seconds = None
    for key in ("timeout_seconds", "seconds", "sleep_seconds", "duration_seconds"):
        if sdef.get(key) is not None:
            seconds = parse_wait_seconds(sdef[key], label=f"{kind}.{key}")
            break
    abs_dl = None
    for key in ("deadline_at", "until", "wake_at"):
        if sdef.get(key) is not None:
            abs_dl = parse_absolute_time(sdef[key], label=f"{kind}.{key}")
            break
    if seconds is None and abs_dl is None:
        raise ValueError(f"{kind} requires seconds or an absolute deadline")
    rel = None if seconds is None else now + timedelta(seconds=seconds)
    if rel is None:
        return abs_dl
    if abs_dl is None:
        return rel
    return min(_as_utc(rel), _as_utc(abs_dl))


class WorkflowsTimersMixin:
    def process_workflow_timers(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Complete due sleeps and fail due timers.

        Each row is its own transaction so one bad run cannot roll back the
        rest. A missed sleep completes the step; a missed timer fails it
        and starts reverse compensation.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        params: list[Any] = [
            WorkflowStepStatus.AWAITING_SLEEP.value,
            WorkflowStepStatus.AWAITING_TIMER.value,
        ]
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
                    WHERE s.status IN (%s, %s)
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
        fired = 0
        for sid in ids:
            if fired >= limit:
                break
            try:
                if self._fire_one_wait_step(sid):
                    fired += 1
            except Exception:  # noqa: BLE001
                continue
        return fired

    def _fire_one_wait_step(self, step_id: UUID) -> bool:
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
            status = str(step["status"])
            if status not in (
                WorkflowStepStatus.AWAITING_SLEEP.value,
                WorkflowStepStatus.AWAITING_TIMER.value,
            ):
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
            if status == WorkflowStepStatus.AWAITING_SLEEP.value:
                wake = step.get("deadline_at")
                self._complete_forward_step(
                    conn,
                    run=run,
                    step=step,
                    result={
                        "kind": "sleep",
                        "slept_until": (
                            wake.isoformat() if hasattr(wake, "isoformat") else wake
                        ),
                    },
                    now=now,
                )
                return True
            self._fail_forward_step(
                conn,
                run=run,
                step=step,
                error="timer deadline exceeded",
                now=now,
            )
            return True

    def _park_or_complete_sleep_or_timer(
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
        """Park a sleep/timer, or fire it immediately if already due."""
        _ = tenant_id
        idx = int(step_row["step_index"])
        sdef = steps_def[idx]
        kind = str(sdef.get("kind") or "").strip().lower()
        if kind in TIMER_KINDS:
            park_status = WorkflowStepStatus.AWAITING_TIMER.value
        else:
            park_status = WorkflowStepStatus.AWAITING_SLEEP.value
        deadline = wait_deadline(sdef, now=now, kind=kind or "sleep")
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
              AND status IN (%s, %s, %s, %s)
            RETURNING {_STEP_COLUMNS}
            """,
            (
                park_status,
                now,
                deadline,
                now,
                step_row["id"],
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.AWAITING_SLEEP.value,
                WorkflowStepStatus.AWAITING_TIMER.value,
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
        if _as_utc(deadline) > _as_utc(now):
            _ = (queue_name, max_attempts, visibility_timeout_seconds)
            return step
        if park_status == WorkflowStepStatus.AWAITING_SLEEP.value:
            self._complete_forward_step(
                conn,
                run=run,
                step=step,
                result={
                    "kind": "sleep",
                    "slept_until": (
                        deadline.isoformat()
                        if hasattr(deadline, "isoformat")
                        else deadline
                    ),
                },
                now=now,
            )
        else:
            self._fail_forward_step(
                conn,
                run=run,
                step=step,
                error="timer deadline exceeded",
                now=now,
            )
        done = conn.execute(
            f"SELECT {_STEP_COLUMNS} FROM workflow_step_runs WHERE id = %s",
            (step["id"],),
        ).fetchone()
        return done or step
