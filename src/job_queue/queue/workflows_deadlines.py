"""Workflow run deadlines: cancel leftovers and undo only completed work.

When ``workflow_runs.deadline_at`` hits, leftover forwards (pending jobs,
parked sleeps / signals / timers / approvals, in-flight leases) are
cancelled and skipped. Reverse compensate runs only for forwards that
already completed. The instant lives on the run row so a coordinator
death or a sibling lease reclaim cannot lose the deadline or undo twice.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import WorkflowRunStatus
from job_queue.queue._common import _as_utc, _utcnow
from job_queue.queue.workflows import _RUN_COLUMNS


DEADLINE_REASON = "cancelled: run deadline exceeded"


def parse_run_deadline(
    *,
    deadline_seconds: Any = None,
    deadline_at: Any = None,
    now: Any,
) -> datetime | None:
    """Absolute run deadline. Earlier of relative and absolute wins."""
    seconds = None
    if deadline_seconds is not None:
        try:
            seconds = float(deadline_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("deadline_seconds must be a number") from exc
        if seconds < 0:
            raise ValueError("deadline_seconds must be >= 0")
    abs_dl = None
    if deadline_at is not None:
        if hasattr(deadline_at, "isoformat"):
            abs_dl = _as_utc(deadline_at)
        else:
            s = str(deadline_at).strip()
            if not s:
                raise ValueError("deadline_at is required")
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                abs_dl = _as_utc(datetime.fromisoformat(s))
            except ValueError as exc:
                raise ValueError("deadline_at must be an ISO-8601 timestamp") from exc
    if seconds is None and abs_dl is None:
        return None
    rel = None if seconds is None else now + timedelta(seconds=seconds)
    if rel is None:
        return abs_dl
    if abs_dl is None:
        return rel
    return min(_as_utc(rel), _as_utc(abs_dl))


class WorkflowsDeadlinesMixin:
    def process_workflow_run_deadlines(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Cancel due running workflows and compensate completed work.

        Each run is its own transaction so one bad row cannot roll back
        the rest. A late tick after a coordinator death still fires once.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        params: list[Any] = [WorkflowRunStatus.RUNNING.value]
        tenant_clause = ""
        if tenant_id is not None:
            tenant_clause = "AND tenant_id = %s"
            params.append(str(tenant_id).strip())
        params.append(limit)
        with self._pool.connection() as conn:
            ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT id
                    FROM workflow_runs
                    WHERE status = %s
                      AND deadline_at IS NOT NULL
                      AND deadline_at <= NOW()
                    {tenant_clause}
                    ORDER BY deadline_at ASC
                    LIMIT %s
                    """,
                    tuple(params),
                ).fetchall()
            ]
        fired = 0
        for rid in ids:
            if fired >= limit:
                break
            try:
                if self._fire_one_run_deadline(rid):
                    fired += 1
            except Exception:  # noqa: BLE001
                continue
        return fired

    def _fire_one_run_deadline(self, run_id: UUID) -> bool:
        now = _utcnow()
        with transaction(self._pool) as conn:
            peek = conn.execute(
                """
                SELECT id, status, deadline_at
                FROM workflow_runs
                WHERE id = %s
                """,
                (run_id,),
            ).fetchone()
            if peek is None:
                return False
            if peek["status"] != WorkflowRunStatus.RUNNING.value:
                return False
            if peek.get("deadline_at") is None or _as_utc(peek["deadline_at"]) > now:
                return False
            # Run first, then cancel steps — same order as human cancel.
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
                return False
            if run["status"] != WorkflowRunStatus.RUNNING.value:
                return False
            if run.get("deadline_at") is None or _as_utc(run["deadline_at"]) > now:
                return False
            self._cancel_workflow_locked(
                conn,
                run,
                reason=DEADLINE_REASON,
                release_in_flight=True,
                compensate=True,
                now=now,
            )
            return True
