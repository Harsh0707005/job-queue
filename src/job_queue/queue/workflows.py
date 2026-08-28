"""Multi-step saga workflows with reverse-order compensation.

Each step is a normal queue job (single-worker lease). Workflows may be linear
or include ``type: parallel`` branch blocks that fan-out and join, or
``type: map`` blocks that spawn one child step per item in a predecessor
result (capped) and join before continuing. On terminal forward failure the
orchestrator settles in-flight sibling branches (does not kill workers
mid-step), then enqueues compensate jobs for completed steps in reverse step
order. Durable step_run rows + unique (run, index, direction) prevent double
compensation under reclaim or dual coordinators.
"""

from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.crypto import workflow_steps_aad
from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    WorkflowRun,
    WorkflowRunStatus,
    WorkflowStepDirection,
    WorkflowStepRun,
    WorkflowStepStatus,
)
from job_queue.queue._common import (
    CancelError,
    QueueDrainError,
    ScheduleError,
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    _as_utc,
    _normalize_skills,
    _utcnow,
)
from job_queue.queue.workflows_branch import (
    WorkflowsBranchMixin,
    _is_if_block,
    if_block_group,
    normalize_if_cases,
)
from job_queue.queue.workflows_map import (
    DEFAULT_MAP_MAX_ITEMS,
    WorkflowsMapMixin,
    _is_map_block,
)

WORKFLOW_STEP_KIND = "workflow.step"

_RUN_COLUMNS = """
    id, tenant_id, name, status, steps_def, current_index,
    fail_step_index, fail_reason, max_compensate_retries,
    created_at, updated_at, completed_at, payload_key_id,
    schedule_id, tick_iteration
"""

_STEP_COLUMNS = """
    id, run_id, tenant_id, step_index, name, direction, status, job_id,
    result, error, idempotency_key, retry_count, started_at, completed_at,
    created_at, updated_at, deadline_at
"""

# Default redrives when a compensate job dies (crash / timeout / nack).
DEFAULT_MAX_COMPENSATE_RETRIES = 5

# Forward steps parked without a worker job (signal / sleep / timer).
_PARKED_FORWARD_STATUSES = (
    WorkflowStepStatus.AWAITING_SIGNAL.value,
    WorkflowStepStatus.AWAITING_SLEEP.value,
    WorkflowStepStatus.AWAITING_TIMER.value,
    WorkflowStepStatus.AWAITING_APPROVAL.value,
)


class WorkflowsMixin(WorkflowsBranchMixin, WorkflowsMapMixin):
    def start_workflow(
        self,
        *,
        tenant_id: str,
        steps: list[dict[str, Any]],
        name: str | None = None,
        queue_name: str = "default",
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        max_compensate_retries: int = DEFAULT_MAX_COMPENSATE_RETRIES,
        run_id: UUID | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: Any = None,
        expiry_action: str | None = None,
        schedule_id: UUID | None = None,
        tick_iteration: int | None = None,
    ) -> WorkflowRun:
        """Start a multi-step workflow and enqueue the first forward step job.

        *ttl_seconds* / *expires_at* / *expiry_action* apply to every step
        job unless the step itself sets those fields. When omitted, the
        tenant default job TTL is used so saga steps cannot sit forever.
        """
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if not steps or not isinstance(steps, list):
            raise ValueError("steps must be a non-empty list")
        if int(max_compensate_retries) < 0:
            raise ValueError("max_compensate_retries must be >= 0")
        normalized = self._expand_and_normalize_steps(steps)
        self._apply_workflow_ttl_defaults(
            normalized,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            expiry_action=expiry_action,
        )
        now = _utcnow()
        rid = run_id or uuid4()
        with transaction(self._pool) as conn:
            row = self._start_workflow_on_conn(
                conn,
                tenant_id=tenant_id,
                normalized=normalized,
                name=name,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                max_compensate_retries=max_compensate_retries,
                run_id=rid,
                now=now,
                schedule_id=schedule_id,
                tick_iteration=tick_iteration,
                lock_tenant=True,
            )
        assert row is not None
        return self._row_to_workflow_run(row)

    def _start_workflow_on_conn(
        self,
        conn: Any,
        *,
        tenant_id: str,
        normalized: list[dict[str, Any]],
        name: str | None,
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        max_compensate_retries: int,
        run_id: UUID,
        now: Any,
        schedule_id: UUID | None = None,
        tick_iteration: int | None = None,
        lock_tenant: bool = True,
    ) -> Any:
        if lock_tenant:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
        if hasattr(self, "_assert_queue_accepts_enqueue"):
            self._assert_queue_accepts_enqueue(conn, tenant_id, queue_name)
        sealed_steps, steps_key_id = self._seal_dict(
            conn,
            tenant_id=tenant_id,
            plaintext={"steps": normalized},
            aad=workflow_steps_aad(run_id),
        )
        conn.execute(
            f"""
            INSERT INTO workflow_runs (
                id, tenant_id, name, status, steps_def, current_index,
                fail_step_index, fail_reason, max_compensate_retries,
                created_at, updated_at, completed_at, payload_key_id,
                schedule_id, tick_iteration
            ) VALUES (
                %s, %s, %s, %s, %s, 0,
                NULL, NULL, %s,
                %s, %s, NULL, %s,
                %s, %s
            )
            """,
            (
                run_id,
                tenant_id,
                str(name).strip() if name else None,
                WorkflowRunStatus.RUNNING.value,
                Jsonb(sealed_steps),
                int(max_compensate_retries),
                now,
                now,
                steps_key_id,
                schedule_id,
                int(tick_iteration) if tick_iteration else None,
            ),
        )
        self._enqueue_ready_forward_steps(
            conn,
            run_id=run_id,
            tenant_id=tenant_id,
            steps_def=normalized,
            queue_name=queue_name,
            max_attempts=max_attempts,
            visibility_timeout_seconds=visibility_timeout_seconds,
            now=now,
        )
        return conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
            (run_id,),
        ).fetchone()

    def get_workflow(self, run_id: UUID, *, tenant_id: str) -> WorkflowRun | None:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_RUN_COLUMNS}
                FROM workflow_runs
                WHERE id = %s AND tenant_id = %s
                """,
                (run_id, str(tenant_id).strip()),
            ).fetchone()
        return self._row_to_workflow_run(row) if row else None

    def list_workflow_steps(
        self, run_id: UUID, *, tenant_id: str
    ) -> list[WorkflowStepRun]:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        with self._pool.connection() as conn:
            run = conn.execute(
                "SELECT id FROM workflow_runs WHERE id = %s AND tenant_id = %s",
                (run_id, tenant_id),
            ).fetchone()
            if run is None:
                raise ValueError(f"workflow run {run_id} not found for tenant")
            rows = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE run_id = %s AND tenant_id = %s
                ORDER BY
                    CASE direction WHEN 'forward' THEN 0 ELSE 1 END,
                    step_index ASC,
                    created_at ASC
                """,
                (run_id, tenant_id),
            ).fetchall()
        return [self._row_to_workflow_step(r) for r in rows]

    def cancel_workflow(
        self,
        run_id: UUID,
        *,
        tenant_id: str,
        reason: str | None = None,
        release_in_flight: bool = False,
        compensate: bool = True,
    ) -> dict[str, Any]:
        """Cancel a running (or compensating) workflow.

        * Always stops **pending** step jobs (never leased) and marks those
          step_runs ``skipped`` (forward always; compensate steps only when
          *compensate* is False).
        * **In-flight** (leased) steps:
            - ``release_in_flight=False`` (default): leave the lease alone so
              the worker can finish; no new forward fan-out.
            - ``release_in_flight=True``: cancel the job (clear fencing token)
              and mark the step ``skipped``.
        * **Completed** forward steps:
            - ``compensate=True`` (default): reverse-order undo like a failure
              (run goes ``compensating`` then ends as ``cancelled``).
            - ``compensate=False``: leave completed steps as-is; run ends
              ``cancelled`` immediately.

        Idempotent when already ``cancelled``. Terminal ``completed`` /
        ``failed`` raise :class:`CancelError`.
        """
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        now = _utcnow()
        want_compensate = bool(compensate)
        want_release = bool(release_in_flight)
        raw_reason = (
            str(reason).strip()
            if reason is not None and str(reason).strip()
            else "workflow cancelled"
        )
        # Prefix so terminal-after-undo can mark CANCELLED instead of FAILED.
        cancel_reason = (
            raw_reason
            if raw_reason.startswith("cancelled")
            else f"cancelled: {raw_reason}"
        )
        with transaction(self._pool) as conn:
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
                raise CancelError(
                    f"workflow run {run_id} not found for tenant {tenant_id!r}"
                )
            status = str(run["status"])
            if status == WorkflowRunStatus.CANCELLED.value:
                self._fail_schedule_generation_for_workflow_run(conn, run)
                return self._cancel_workflow_result(
                    conn,
                    run=run,
                    cancelled_pending=0,
                    released_in_flight=0,
                    left_in_flight=self._count_in_flight_steps(conn, run_id),
                    release_in_flight=want_release,
                    compensate=want_compensate,
                    reason=run.get("fail_reason") or cancel_reason,
                )
            if status in (
                WorkflowRunStatus.COMPLETED.value,
                WorkflowRunStatus.FAILED.value,
            ):
                raise CancelError(
                    f"workflow run {run_id} cannot be cancelled "
                    f"(status={status})"
                )
            if status not in (
                WorkflowRunStatus.RUNNING.value,
                WorkflowRunStatus.COMPENSATING.value,
            ):
                raise CancelError(
                    f"workflow run {run_id} cannot be cancelled "
                    f"(status={status})"
                )
            return self._cancel_workflow_locked(
                conn,
                run,
                reason=cancel_reason,
                release_in_flight=want_release,
                compensate=want_compensate,
                now=now,
            )

    def _cancel_workflow_locked(
        self,
        conn: Any,
        run: Any,
        *,
        reason: str,
        release_in_flight: bool,
        compensate: bool,
        now: Any | None = None,
    ) -> dict[str, Any]:
        """Cancel a locked running/compensating run on *conn*."""
        now = now if now is not None else _utcnow()
        run_id = run["id"]
        cancel_reason = (
            reason
            if str(reason).startswith("cancelled")
            else f"cancelled: {reason}"
        )
        # When undoing, stop new forward work but keep compensate chain.
        # Without undo, freeze as cancelled immediately.
        if compensate:
            target_status = WorkflowRunStatus.COMPENSATING.value
            completed_at = None
        else:
            target_status = WorkflowRunStatus.CANCELLED.value
            completed_at = now

        conn.execute(
            """
            UPDATE workflow_runs
            SET status = %s,
                fail_reason = %s,
                completed_at = COALESCE(%s, completed_at),
                updated_at = %s
            WHERE id = %s
            """,
            (target_status, cancel_reason, completed_at, now, run_id),
        )
        run = dict(run)
        run["status"] = target_status
        run["fail_reason"] = cancel_reason
        if completed_at is not None:
            run["completed_at"] = completed_at
        # A cancelled tick must close its generation as failed. Leaving
        # it pending forever makes waiters/joins treat the tick as live.
        self._fail_schedule_generation_for_workflow_run(conn, run)

        # Drop pending forwards always. Pending compensate undos only when
        # caller does not want reverse compensation.
        only_dirs = (
            (WorkflowStepDirection.FORWARD.value,) if compensate else None
        )
        cancelled_pending, released, left = self._cancel_workflow_steps_locked(
            conn,
            run=run,
            release_in_flight=release_in_flight,
            reason=cancel_reason,
            now=now,
            only_directions=only_dirs,
        )

        if compensate:
            steps_def = self._parse_steps_def(run, conn)
            self._try_start_compensate_if_settled(
                conn,
                run=run,
                steps_def=list(steps_def),
                queue_name="default",
                max_attempts=3,
                visibility_timeout_seconds=30,
                now=now,
            )
            # If nothing to undo and no in-flight left, settle to cancelled.
            row_chk = conn.execute(
                f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
                (run_id,),
            ).fetchone()
            if row_chk is not None:
                run = dict(row_chk)

        return self._cancel_workflow_result(
            conn,
            run=run,
            cancelled_pending=cancelled_pending,
            released_in_flight=released,
            left_in_flight=left,
            release_in_flight=release_in_flight,
            compensate=compensate,
            reason=cancel_reason,
        )

    def _cancel_open_workflow_ticks_for_schedule(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        tenant_id: str,
    ) -> int:
        """Cancel live ticks for this schedule so delete does not leave them.

        No reverse compensation: the schedule is going away. Leased steps
        are released so workers cannot keep a deleted tick alive.
        """
        rows = conn.execute(
            f"""
            SELECT {_RUN_COLUMNS}
            FROM workflow_runs
            WHERE schedule_id = %s
              AND tenant_id = %s
              AND status IN (%s, %s)
            FOR UPDATE
            """,
            (
                schedule_id,
                tenant_id,
                WorkflowRunStatus.RUNNING.value,
                WorkflowRunStatus.COMPENSATING.value,
            ),
        ).fetchall()
        n = 0
        for row in rows:
            self._cancel_workflow_locked(
                conn,
                row,
                reason="schedule deleted",
                release_in_flight=True,
                compensate=False,
            )
            n += 1
        if hasattr(self, "_fail_schedule_generation"):
            # Belt: any leftover pending generation is failed even if the
            # run row was already gone.
            self._fail_schedule_generation(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                force=False,
            )
        return n

    def _fail_schedule_generation_for_workflow_run(
        self, conn: Any, run: dict[str, Any]
    ) -> None:
        """Fail the schedule generation for this tick, if the run is one."""
        if not run or not run.get("schedule_id"):
            return
        if not hasattr(self, "_fail_schedule_generation"):
            return
        self._fail_schedule_generation(
            conn,
            schedule_id=run["schedule_id"],
            tenant_id=str(run["tenant_id"]),
            iteration=(
                None
                if run.get("tick_iteration") is None
                else int(run["tick_iteration"])
            ),
            force=True,
        )

    def _cancel_workflow_result(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        cancelled_pending: int,
        released_in_flight: int,
        left_in_flight: int,
        release_in_flight: bool,
        compensate: bool,
        reason: str,
    ) -> dict[str, Any]:
        row = conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
            (run["id"],),
        ).fetchone()
        assert row is not None
        return {
            "run": self._row_to_workflow_run(row),
            "cancelled_pending": int(cancelled_pending),
            "released_in_flight": int(released_in_flight),
            "left_in_flight": int(left_in_flight),
            "release_in_flight": bool(release_in_flight),
            "compensate": bool(compensate),
            "reason": reason,
        }

    @staticmethod
    def _is_cancel_compensate_reason(fail_reason: Any) -> bool:
        return str(fail_reason or "").startswith("cancelled")

    def _compensate_done_status(self, run: dict[str, Any]) -> str:
        """Terminal status after reverse chain: cancelled vs failed."""
        if self._is_cancel_compensate_reason(run.get("fail_reason")):
            return WorkflowRunStatus.CANCELLED.value
        return WorkflowRunStatus.FAILED.value

    def _count_in_flight_steps(self, conn: Any, run_id: UUID) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM workflow_step_runs s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.run_id = %s
              AND s.status IN ('pending', 'running')
              AND j.status = 'leased'
            """,
            (run_id,),
        ).fetchone()
        return int(row["n"] if row else 0)

    def _cancel_workflow_steps_locked(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        release_in_flight: bool,
        reason: str,
        now: Any,
        only_directions: tuple[str, ...] | None = None,
    ) -> tuple[int, int, int]:
        """Cancel pending step jobs; optionally release leased ones.

        Returns ``(cancelled_pending, released_in_flight, left_in_flight)``.
        """
        dir_sql = ""
        params: list[Any] = [
            run["id"],
            WorkflowStepStatus.PENDING.value,
            WorkflowStepStatus.RUNNING.value,
            *_PARKED_FORWARD_STATUSES,
        ]
        if only_directions:
            dir_sql = "AND direction = ANY(%s)"
            params.append(list(only_directions))
        parked_ph = ", ".join(["%s"] * len(_PARKED_FORWARD_STATUSES))
        steps = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND status IN (%s, %s, {parked_ph})
              {dir_sql}
            FOR UPDATE
            """,
            tuple(params),
        ).fetchall()
        cancelled_pending = 0
        released = 0
        left = 0
        err = f"cancelled: {reason}"
        for srow in steps:
            job = None
            if srow.get("job_id") is not None:
                peek = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (srow["job_id"],),
                ).fetchone()
                if (
                    peek is not None
                    and peek["status"] == JobStatus.LEASED.value
                    and not release_in_flight
                ):
                    left += 1
                    continue
                job = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE id = %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (srow["job_id"],),
                ).fetchone()
                if job is None:
                    # Locked by a concurrent ack/nack — leave in-flight.
                    left += 1
                    continue

            # Placeholder compensate step with no job yet.
            if job is None:
                conn.execute(
                    """
                    UPDATE workflow_step_runs
                    SET status = %s,
                        error = %s,
                        completed_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        WorkflowStepStatus.SKIPPED.value,
                        err,
                        now,
                        now,
                        srow["id"],
                    ),
                )
                cancelled_pending += 1
                continue

            jstatus = str(job["status"])
            if jstatus == JobStatus.PENDING.value:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        completed_at = %s,
                        updated_at = %s,
                        last_error = COALESCE(last_error, %s)
                    WHERE id = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        now,
                        now,
                        err,
                        srow["job_id"],
                        JobStatus.PENDING.value,
                    ),
                )
                conn.execute(
                    """
                    UPDATE workflow_step_runs
                    SET status = %s,
                        error = %s,
                        completed_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        WorkflowStepStatus.SKIPPED.value,
                        err,
                        now,
                        now,
                        srow["id"],
                    ),
                )
                cancelled_pending += 1
                continue

            if jstatus == JobStatus.LEASED.value:
                if release_in_flight:
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = %s,
                            leased_by = NULL,
                            lease_token = NULL,
                            lease_tenant_id = NULL,
                            lease_expires_at = NULL,
                            lease_started_at = NULL,
                            completed_at = %s,
                            updated_at = %s,
                            last_error = %s
                        WHERE id = %s
                          AND status = %s
                        """,
                        (
                            JobStatus.CANCELLED.value,
                            now,
                            now,
                            "cancelled while running (workflow cancel)",
                            srow["job_id"],
                            JobStatus.LEASED.value,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE workflow_step_runs
                        SET status = %s,
                            error = %s,
                            completed_at = %s,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            WorkflowStepStatus.SKIPPED.value,
                            err,
                            now,
                            now,
                            srow["id"],
                        ),
                    )
                    released += 1
                else:
                    left += 1
                continue

            # Job already terminal while step still pending/running — skip step.
            if jstatus in (
                JobStatus.CANCELLED.value,
                JobStatus.COMPLETED.value,
                JobStatus.DEAD.value,
                JobStatus.FAILED.value,
            ):
                conn.execute(
                    """
                    UPDATE workflow_step_runs
                    SET status = %s,
                        error = COALESCE(error, %s),
                        completed_at = COALESCE(completed_at, %s),
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        WorkflowStepStatus.SKIPPED.value,
                        err,
                        now,
                        now,
                        srow["id"],
                    ),
                )
                cancelled_pending += 1
        return cancelled_pending, released, left

    def _on_workflow_step_terminal(
        self,
        conn: Any,
        job_row: dict[str, Any],
        *,
        success: bool,
        result: Any = None,
        error: str | None = None,
        now: Any = None,
    ) -> None:
        """Advance or compensate after a workflow step job reaches a terminal state.

        Must run inside the caller's transaction (ack/nack).
        """
        from job_queue.blob_store import BlobStoreError

        try:
            payload = self._open_job_payload(
                job_row.get("payload"),
                tenant_id=str(job_row["tenant_id"]),
                job_id=job_row["id"],
                conn=conn,
            )
        except BlobStoreError:
            payload = {}
        if payload.get("kind") != WORKFLOW_STEP_KIND:
            # Regular jobs exit here. A workflow step whose blob is gone
            # still has a step_run row — fail that step closed.
            probe = conn.execute(
                "SELECT 1 FROM workflow_step_runs WHERE job_id = %s LIMIT 1",
                (job_row["id"],),
            ).fetchone()
            if probe is None:
                return
        ts = now or _utcnow()
        job_id = job_row["id"]
        step = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE job_id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if step is None:
            return
        # Already terminal — dual-coord / re-entry safety.
        if step["status"] in (
            WorkflowStepStatus.COMPLETED.value,
            WorkflowStepStatus.FAILED.value,
            WorkflowStepStatus.COMPENSATED.value,
            WorkflowStepStatus.SKIPPED.value,
        ):
            return

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
            return

        direction = step["direction"]
        if success:
            if direction == WorkflowStepDirection.FORWARD.value:
                self._complete_forward_step(
                    conn, run=run, step=step, result=result, now=ts
                )
            else:
                self._complete_compensate_step(
                    conn, run=run, step=step, result=result, now=ts
                )
        else:
            if direction == WorkflowStepDirection.FORWARD.value:
                self._fail_forward_step(
                    conn, run=run, step=step, error=error, now=ts
                )
            else:
                # Compensate job died (nack/dead, lease expiry, max-runtime).
                # Redrive with a new job + same idempotency key so undo is not
                # stuck in compensating; after retry budget, continue reverse chain.
                self._fail_or_redrive_compensate_step(
                    conn,
                    run=run,
                    step=step,
                    error=error or "compensate failed",
                    now=ts,
                )

    def process_workflow_compensation(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Repair stuck compensating runs (dead compensate jobs, missing next).

        Each repair runs in its **own** transaction so one failure cannot roll
        back unrelated fixes. Only treats a step as stuck when its linked job
        is terminal (or missing) — never when the job is still pending/leased.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        fixed = 0

        # Phase 1: collect candidate step ids (read-only snapshot).
        params: list[Any] = [
            WorkflowStepDirection.COMPENSATE.value,
            WorkflowStepStatus.PENDING.value,
            WorkflowStepStatus.RUNNING.value,
            JobStatus.DEAD.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.COMPLETED.value,
        ]
        tenant_clause = ""
        if tenant_id is not None:
            tenant_clause = "AND s.tenant_id = %s"
            params.append(str(tenant_id).strip())
        params.append(limit)
        with self._pool.connection() as conn:
            stuck_ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT s.id
                    FROM workflow_step_runs s
                    JOIN jobs j ON j.id = s.job_id
                    WHERE s.direction = %s
                      AND s.status IN (%s, %s)
                      AND j.status IN (%s, %s, %s, %s)
                    {tenant_clause}
                    ORDER BY s.updated_at ASC
                    LIMIT %s
                    """,
                    tuple(params),
                ).fetchall()
            ]
            # Also: step has job_id but job row is gone.
            params2: list[Any] = [
                WorkflowStepDirection.COMPENSATE.value,
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.RUNNING.value,
            ]
            t2 = ""
            if tenant_id is not None:
                t2 = "AND s.tenant_id = %s"
                params2.append(str(tenant_id).strip())
            params2.append(limit)
            missing_job_ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT s.id
                    FROM workflow_step_runs s
                    WHERE s.direction = %s
                      AND s.status IN (%s, %s)
                      AND s.job_id IS NOT NULL
                      AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = s.job_id)
                    {t2}
                    ORDER BY s.updated_at ASC
                    LIMIT %s
                    """,
                    tuple(params2),
                ).fetchall()
            ]
            rparams: list[Any] = [WorkflowRunStatus.COMPENSATING.value]
            rtenant = ""
            if tenant_id is not None:
                rtenant = "AND tenant_id = %s"
                rparams.append(str(tenant_id).strip())
            rparams.append(limit)
            orphan_run_ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT id
                    FROM workflow_runs
                    WHERE status = %s
                    {rtenant}
                    ORDER BY updated_at ASC
                    LIMIT %s
                    """,
                    tuple(rparams),
                ).fetchall()
            ]

        for sid in stuck_ids + missing_job_ids:
            if fixed >= limit:
                break
            try:
                if self._repair_one_stuck_compensate_step(sid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                # Isolate failures — never abort the whole sweep.
                continue

        for rid in orphan_run_ids:
            if fixed >= limit:
                break
            try:
                if self._repair_one_compensate_run(rid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                continue

        # Resume forward fan-out that drain skipped (step ack must not raise).
        remaining = limit - fixed
        if remaining > 0:
            fixed += self._redrive_ready_forward_runs(
                tenant_id=tenant_id, limit=remaining
            )
        return fixed

    def _redrive_ready_forward_runs(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Enqueue skipped next-forward steps once the queue is not draining."""
        params: list[Any] = [WorkflowRunStatus.RUNNING.value]
        tclause = ""
        if tenant_id is not None:
            tclause = "AND tenant_id = %s"
            params.append(str(tenant_id).strip())
        params.append(limit)
        with self._pool.connection() as conn:
            run_ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT id
                    FROM workflow_runs
                    WHERE status = %s
                    {tclause}
                    ORDER BY updated_at ASC
                    LIMIT %s
                    """,
                    tuple(params),
                ).fetchall()
            ]
        fixed = 0
        for rid in run_ids:
            if fixed >= limit:
                break
            try:
                if self._redrive_one_ready_forward_run(rid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                continue
        return fixed

    def _redrive_one_ready_forward_run(self, run_id: UUID) -> bool:
        now = _utcnow()
        with transaction(self._pool) as conn:
            run = conn.execute(
                f"""
                SELECT {_RUN_COLUMNS}
                FROM workflow_runs
                WHERE id = %s
                FOR UPDATE
                """,
                (run_id,),
            ).fetchone()
            if run is None or run["status"] != WorkflowRunStatus.RUNNING.value:
                return False
            qn = self._workflow_queue_name(conn, run_id) or "default"
            if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
                conn, str(run["tenant_id"]), qn
            ):
                # Intake is closed, but a finished map join still needs to
                # close the run so it cannot sit running forever.
                return self._maybe_complete_running_workflow_locked(
                    conn, run_id=run_id, now=now
                )
            job = conn.execute(
                f"""
                SELECT max_attempts, visibility_timeout_seconds
                FROM jobs
                WHERE id IN (
                    SELECT job_id FROM workflow_step_runs
                    WHERE run_id = %s AND job_id IS NOT NULL
                )
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            n = self._enqueue_ready_forward_steps(
                conn,
                run_id=run_id,
                tenant_id=str(run["tenant_id"]),
                steps_def=self._parse_steps_def(run, conn),
                queue_name=qn,
                max_attempts=int(job["max_attempts"]) if job else 3,
                visibility_timeout_seconds=(
                    int(job["visibility_timeout_seconds"]) if job else 30
                ),
                now=now,
            )
            finished = self._maybe_complete_running_workflow_locked(
                conn, run_id=run_id, now=now
            )
            after = conn.execute(
                "SELECT status FROM workflow_runs WHERE id = %s",
                (run_id,),
            ).fetchone()
            return n > 0 or finished or (
                after is not None
                and after["status"] == WorkflowRunStatus.COMPLETED.value
            )

    @staticmethod
    def _workflow_queue_name(conn: Any, run_id: UUID) -> str | None:
        row = conn.execute(
            """
            SELECT j.queue_name
            FROM workflow_step_runs s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.run_id = %s
            ORDER BY s.step_index ASC
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if row is None or not row.get("queue_name"):
            return None
        return str(row["queue_name"])

    def _repair_one_stuck_compensate_step(self, step_id: UUID) -> bool:
        """Redrive a single compensate step whose job is terminal/missing."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            step = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs WHERE id = %s FOR UPDATE
                """,
                (step_id,),
            ).fetchone()
            if step is None:
                return False
            if step["direction"] != WorkflowStepDirection.COMPENSATE.value:
                return False
            if step["status"] not in (
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.RUNNING.value,
            ):
                return False
            run = conn.execute(
                f"""
                SELECT {_RUN_COLUMNS}
                FROM workflow_runs WHERE id = %s FOR UPDATE
                """,
                (step["run_id"],),
            ).fetchone()
            if run is None or run["status"] != WorkflowRunStatus.COMPENSATING.value:
                return False
            job = None
            if step.get("job_id") is not None:
                job = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (step["job_id"],),
                ).fetchone()
            # Healthy in-flight work: do not redrive.
            if job is not None and job["status"] in (
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
            ):
                return False
            err = (
                (job.get("last_error") if job else None)
                or (f"job ended as {job['status']}" if job else "compensate job missing")
            )
            self._fail_or_redrive_compensate_step(
                conn,
                run=run,
                step=step,
                error=str(err),
                now=now,
            )
            return True

    def _repair_one_compensate_run(self, run_id: UUID) -> bool:
        """Advance one compensating run if it has no claimable compensate work."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            run = conn.execute(
                f"""
                SELECT {_RUN_COLUMNS}
                FROM workflow_runs WHERE id = %s FOR UPDATE
                """,
                (run_id,),
            ).fetchone()
            if run is None or run["status"] != WorkflowRunStatus.COMPENSATING.value:
                return False
            return self._ensure_compensate_progress(conn, run=run, now=now)

    def _maybe_complete_running_workflow_locked(
        self,
        conn: Any,
        *,
        run_id: UUID,
        now: Any,
    ) -> bool:
        """Mark a running workflow completed when every real step is done.

        Map join is not a worker job, so finishing the last child can leave
        every step_run ``completed`` while the run stays ``running`` with
        nothing left to lease. Re-read ``steps_def`` so a just-materialized
        fan-out is included; ignore ghost child slots that have no step_run.
        """
        run = conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
            (run_id,),
        ).fetchone()
        if run is None or run["status"] != WorkflowRunStatus.RUNNING.value:
            return False
        steps_def = self._parse_steps_def(run, conn)
        if not steps_def:
            return False
        rows = conn.execute(
            """
            SELECT step_index, status
            FROM workflow_step_runs
            WHERE run_id = %s AND direction = %s
            """,
            (run_id, WorkflowStepDirection.FORWARD.value),
        ).fetchall()
        by_idx = {int(r["step_index"]): str(r["status"]) for r in rows}
        terminal = {
            WorkflowStepStatus.COMPLETED.value,
            WorkflowStepStatus.SKIPPED.value,
            WorkflowStepStatus.FAILED.value,
        }
        completed_n = 0
        for i, sdef in enumerate(steps_def):
            st = by_idx.get(i)
            if st is None:
                if sdef.get("map_join_index") is not None:
                    # Extra child slot from a lost rematerialize — not work.
                    continue
                if str(sdef.get("kind") or "") == "map_join":
                    kids = [
                        int(c)
                        for c in (sdef.get("map_children") or [])
                    ] or [
                        j
                        for j, d in enumerate(steps_def)
                        if d.get("map_join_index") == i
                    ]
                    if kids and all(
                        by_idx.get(c) == WorkflowStepStatus.COMPLETED.value
                        for c in kids
                    ):
                        continue
                return False
            if st not in terminal:
                return False
            if st == WorkflowStepStatus.COMPLETED.value:
                completed_n += 1
        if completed_n < 1 and not by_idx:
            return False
        updated = conn.execute(
            """
            UPDATE workflow_runs
            SET status = %s,
                current_index = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s
              AND status = %s
            RETURNING id
            """,
            (
                WorkflowRunStatus.COMPLETED.value,
                len(steps_def),
                now,
                now,
                run_id,
                WorkflowRunStatus.RUNNING.value,
            ),
        ).fetchone()
        return updated is not None

    def _complete_forward_step(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: dict[str, Any],
        result: Any,
        now: Any,
    ) -> None:
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = %s,
                result = %s,
                completed_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                WorkflowStepStatus.COMPLETED.value,
                Jsonb(result) if result is not None else None,
                now,
                now,
                step["id"],
            ),
        )
        steps_def = self._parse_steps_def(run, conn)
        idx = int(step["step_index"])
        conn.execute(
            """
            UPDATE workflow_runs
            SET current_index = GREATEST(current_index, %s),
                updated_at = %s
            WHERE id = %s
            """,
            (idx + 1, now, run["id"]),
        )

        job = None
        if step.get("job_id") is not None:
            job = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (step["job_id"],),
            ).fetchone()
        qn = (
            job["queue_name"]
            if job
            else (self._workflow_queue_name(conn, run["id"]) or "default")
        )
        ma = int(job["max_attempts"]) if job else 3
        vt = int(job["visibility_timeout_seconds"]) if job else 30

        # Sibling completed while run is already compensating: do not fan-out;
        # wait for other in-flight forwards, then undo completed work.
        if run["status"] == WorkflowRunStatus.COMPENSATING.value:
            self._try_start_compensate_if_settled(
                conn,
                run=run,
                steps_def=steps_def,
                queue_name=qn,
                max_attempts=ma,
                visibility_timeout_seconds=vt,
                now=now,
            )
            return
        # Cancelled run: in-flight worker finished — record result, no fan-out.
        if run["status"] == WorkflowRunStatus.CANCELLED.value:
            return

        self._enqueue_ready_forward_steps(
            conn,
            run_id=run["id"],
            tenant_id=run["tenant_id"],
            steps_def=steps_def,
            queue_name=qn,
            max_attempts=ma,
            visibility_timeout_seconds=vt,
            now=now,
        )
        self._maybe_complete_running_workflow_locked(
            conn, run_id=run["id"], now=now
        )

    def _fail_forward_step(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: dict[str, Any],
        error: str | None,
        now: Any,
    ) -> None:
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = %s, error = %s, completed_at = %s, updated_at = %s
            WHERE id = %s
            """,
            (
                WorkflowStepStatus.FAILED.value,
                error,
                now,
                now,
                step["id"],
            ),
        )
        fail_idx = int(step["step_index"])
        steps_def = self._parse_steps_def(run, conn)

        job = None
        if step.get("job_id") is not None:
            job = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (step["job_id"],),
            ).fetchone()
        qn = (
            job["queue_name"]
            if job
            else (self._workflow_queue_name(conn, run["id"]) or "default")
        )
        ma = int(job["max_attempts"]) if job else 3
        vt = int(job["visibility_timeout_seconds"]) if job else 30

        # Already compensating (sibling failed earlier): settle then compensate.
        if run["status"] == WorkflowRunStatus.COMPENSATING.value:
            self._try_start_compensate_if_settled(
                conn,
                run=run,
                steps_def=steps_def,
                queue_name=qn,
                max_attempts=ma,
                visibility_timeout_seconds=vt,
                now=now,
            )
            return
        # Cancelled: in-flight nack/dead after cancel — no compensate fan-out.
        if run["status"] in (
            WorkflowRunStatus.FAILED.value,
            WorkflowRunStatus.COMPLETED.value,
            WorkflowRunStatus.CANCELLED.value,
        ):
            return

        if run.get("schedule_id") and hasattr(self, "_fail_schedule_generation"):
            self._fail_schedule_generation(
                conn,
                schedule_id=run["schedule_id"],
                tenant_id=str(run["tenant_id"]),
                iteration=(
                    None
                    if run.get("tick_iteration") is None
                    else int(run["tick_iteration"])
                ),
                force=True,
            )
        conn.execute(
            """
            UPDATE workflow_runs
            SET status = %s,
                fail_step_index = %s,
                fail_reason = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                WorkflowRunStatus.COMPENSATING.value,
                fail_idx,
                error or f"step {fail_idx} failed",
                now,
                run["id"],
            ),
        )
        # Refresh local status for settle helpers.
        run = dict(run)
        run["status"] = WorkflowRunStatus.COMPENSATING.value
        run["fail_step_index"] = fail_idx

        # Cancel unstarted siblings only — never interrupt a leased worker mid-step.
        self._cancel_unstarted_forward_siblings(
            conn, run=run, failed_step_id=step["id"], now=now
        )
        self._try_start_compensate_if_settled(
            conn,
            run=run,
            steps_def=steps_def,
            queue_name=qn,
            max_attempts=ma,
            visibility_timeout_seconds=vt,
            now=now,
        )

    def _enqueue_ready_forward_steps(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        now: Any,
    ) -> int:
        """Enqueue every forward step whose dependencies are all completed."""
        if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
            conn, tenant_id, queue_name
        ):
            # Drain closes intake. Do not raise — callers include the step
            # ack transaction, which must still complete the finished job.
            return 0
        st = conn.execute(
            "SELECT status FROM workflow_runs WHERE id = %s",
            (run_id,),
        ).fetchone()
        if st is not None and st["status"] in (
            WorkflowRunStatus.CANCELLED.value,
            WorkflowRunStatus.FAILED.value,
            WorkflowRunStatus.COMPLETED.value,
            WorkflowRunStatus.COMPENSATING.value,
        ):
            return 0
        ready = {
            int(r["step_index"])
            for r in conn.execute(
                """
                SELECT step_index
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND direction = 'forward'
                  AND status IN (%s, %s)
                """,
                (
                    run_id,
                    WorkflowStepStatus.COMPLETED.value,
                    WorkflowStepStatus.SKIPPED.value,
                ),
            ).fetchall()
        }
        completed = {
            int(r["step_index"])
            for r in conn.execute(
                """
                SELECT step_index
                FROM workflow_step_runs
                WHERE run_id = %s
                  AND direction = 'forward'
                  AND status = %s
                """,
                (run_id, WorkflowStepStatus.COMPLETED.value),
            ).fetchall()
        }
        existing = {
            int(r["step_index"])
            for r in conn.execute(
                """
                SELECT step_index
                FROM workflow_step_runs
                WHERE run_id = %s AND direction = 'forward'
                """,
                (run_id,),
            ).fetchall()
        }
        n = 0
        i = 0
        while i < len(steps_def):
            sdef = steps_def[i]
            if str(sdef.get("kind") or "") == "map_join":
                deps_ready = all(
                    int(d) in ready
                    for d in (sdef.get("map") or {}).get("from_indices")
                    or sdef.get("depends_on_indices")
                    or []
                )
                # After materialize, depends_on becomes the children; the
                # producer set lives on map.from_indices.
                producers = [
                    int(d)
                    for d in (sdef.get("map") or {}).get("from_indices") or []
                ]
                if producers:
                    deps_ready = all(d in ready for d in producers)
                if not deps_ready:
                    i += 1
                    continue
                children = sdef.get("map_children")
                if children is None:
                    recovered = [
                        j
                        for j, d in enumerate(steps_def)
                        if d.get("map_join_index") == i
                    ]
                    if recovered:
                        children = recovered
                        sdef["map_children"] = list(recovered)
                    else:
                        children = self._materialize_map_locked(
                            conn,
                            run_id=run_id,
                            tenant_id=tenant_id,
                            steps_def=steps_def,
                            map_index=i,
                            now=now,
                        )
                        if children is None:
                            return n
                child_idxs = [int(c) for c in children]
                for ci in child_idxs:
                    if ci in existing:
                        continue
                    self._create_and_enqueue_step(
                        conn,
                        run_id=run_id,
                        tenant_id=tenant_id,
                        steps_def=steps_def,
                        step_index=ci,
                        direction=WorkflowStepDirection.FORWARD.value,
                        queue_name=queue_name,
                        max_attempts=max_attempts,
                        visibility_timeout_seconds=visibility_timeout_seconds,
                        now=now,
                    )
                    existing.add(ci)
                    n += 1
                if child_idxs and any(c not in completed for c in child_idxs):
                    i += 1
                    continue
                if i not in existing:
                    self._complete_map_join_locked(
                        conn,
                        run_id=run_id,
                        tenant_id=tenant_id,
                        steps_def=steps_def,
                        map_index=i,
                        now=now,
                    )
                    existing.add(i)
                    completed.add(i)
                    ready.add(i)
                    n += 1
                i += 1
                continue
            if i in existing:
                i += 1
                continue
            deps = [int(d) for d in (sdef.get("depends_on_indices") or [])]
            if any(d not in ready for d in deps):
                i += 1
                continue
            branch = (
                sdef.get("branch") if isinstance(sdef.get("branch"), dict) else None
            )
            if branch:
                sources = [int(x) for x in (branch.get("source_indices") or [])]
                if any(s not in ready for s in sources):
                    i += 1
                    continue
                group = str(branch.get("group") or "")
                winner = self._winner_for_branch_group(
                    conn,
                    run_id=run_id,
                    steps_def=steps_def,
                    group=group,
                    source_indices=sources,
                )
                n += self._skip_losing_branch_steps(
                    conn,
                    run_id=run_id,
                    tenant_id=tenant_id,
                    steps_def=steps_def,
                    group=group,
                    winner=winner,
                    existing=existing,
                    ready=ready,
                    now=now,
                    source_indices=sources,
                )
                if i in existing:
                    i += 1
                    continue
            self._create_and_enqueue_step(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                steps_def=steps_def,
                step_index=i,
                direction=WorkflowStepDirection.FORWARD.value,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
            existing.add(i)
            n += 1
            i += 1
        self._maybe_complete_running_workflow_locked(conn, run_id=run_id, now=now)
        return n

    def _cancel_unstarted_forward_siblings(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        failed_step_id: Any,
        now: Any,
    ) -> int:
        """Skip pending (not leased) sibling forwards; leave in-flight alone."""
        rows = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'forward'
              AND status IN (%s, %s, {", ".join(["%s"] * len(_PARKED_FORWARD_STATUSES))})
              AND id <> %s
            FOR UPDATE
            """,
            (
                run["id"],
                WorkflowStepStatus.PENDING.value,
                WorkflowStepStatus.RUNNING.value,
                *_PARKED_FORWARD_STATUSES,
                failed_step_id,
            ),
        ).fetchall()
        cancelled = 0
        for srow in rows:
            job = None
            if srow.get("job_id") is not None:
                # Peek first — never wait on a sibling lease. Two nacks in
                # the same fan-out would otherwise deadlock locking each
                # other's in-flight jobs.
                peek = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (srow["job_id"],),
                ).fetchone()
                if peek is not None and peek["status"] == JobStatus.LEASED.value:
                    continue
                job = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE id = %s AND status <> %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (srow["job_id"], JobStatus.LEASED.value),
                ).fetchone()
                if job is None:
                    continue
            # In-flight lease: worker keeps running; settle later on its terminal.
            if job is not None and job["status"] == JobStatus.LEASED.value:
                continue
            if srow["status"] == WorkflowStepStatus.RUNNING.value and (
                job is None or job["status"] == JobStatus.LEASED.value
            ):
                continue
            if job is not None and job["status"] == JobStatus.PENDING.value:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        completed_at = %s,
                        updated_at = %s,
                        last_error = COALESCE(last_error, %s)
                    WHERE id = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        now,
                        now,
                        "workflow sibling failed; unstarted branch cancelled",
                        srow["job_id"],
                        JobStatus.PENDING.value,
                    ),
                )
            conn.execute(
                """
                UPDATE workflow_step_runs
                SET status = %s,
                    error = %s,
                    completed_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    WorkflowStepStatus.SKIPPED.value,
                    "cancelled: sibling branch failed",
                    now,
                    now,
                    srow["id"],
                ),
            )
            cancelled += 1
        return cancelled

    def _active_forward_work(self, conn: Any, *, run_id: UUID) -> bool:
        """True if any forward step still has claimable/in-flight work."""
        row = conn.execute(
            """
            SELECT 1
            FROM workflow_step_runs s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.run_id = %s
              AND s.direction = 'forward'
              AND s.status IN ('pending', 'running')
              AND j.status IN (
                    'pending', 'leased', 'awaiting_skills',
                    'awaiting_capacity', 'awaiting_approval',
                    'awaiting_children'
                  )
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()
        if row is not None:
            return True
        waiting = conn.execute(
            """
            SELECT 1
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'forward'
              AND status = ANY(%s)
            LIMIT 1
            """,
            (run_id, list(_PARKED_FORWARD_STATUSES)),
        ).fetchone()
        return waiting is not None

    def _try_start_compensate_if_settled(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        steps_def: list[dict[str, Any]],
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        now: Any,
    ) -> bool:
        """When no forward work remains, begin reverse compensate of completed.

        In-flight sibling branches are allowed to finish their current lease
        (no mid-step kill); only then does undo start.
        """
        if self._active_forward_work(conn, run_id=run["id"]):
            return False

        already = conn.execute(
            """
            SELECT 1 FROM workflow_step_runs
            WHERE run_id = %s AND direction = 'compensate'
            LIMIT 1
            """,
            (run["id"],),
        ).fetchone()
        if already is not None:
            # Chain already materialised — only redrive/advance (avoid re-entry
            # into this method via _ensure_compensate_progress).
            return False

        # All completed forwards (including siblings that finished after fail).
        completed_rows = conn.execute(
            """
            SELECT step_index, name
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'forward'
              AND status = %s
            ORDER BY step_index DESC
            """,
            (run["id"], WorkflowStepStatus.COMPLETED.value),
        ).fetchall()

        to_compensate: list[int] = []
        for crow in completed_rows:
            i = int(crow["step_index"])
            sdef = steps_def[i] if i < len(steps_def) else {}
            if not sdef.get("compensate"):
                conn.execute(
                    f"""
                    INSERT INTO workflow_step_runs (
                        id, run_id, tenant_id, step_index, name, direction,
                        status, job_id, result, error, idempotency_key, retry_count,
                        started_at, completed_at, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, NULL, NULL, NULL, %s, 0,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (run_id, step_index, direction) DO NOTHING
                    """,
                    (
                        uuid4(),
                        run["id"],
                        run["tenant_id"],
                        i,
                        str(sdef.get("name") or crow["name"] or f"step-{i}"),
                        WorkflowStepDirection.COMPENSATE.value,
                        WorkflowStepStatus.SKIPPED.value,
                        f"{run['id']}:{i}:compensate",
                        now,
                        now,
                        now,
                        now,
                    ),
                )
                continue
            to_compensate.append(i)

        if not to_compensate:
            done_status = self._compensate_done_status(run)
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s
                  AND status = %s
                """,
                (
                    done_status,
                    now,
                    now,
                    run["id"],
                    WorkflowRunStatus.COMPENSATING.value,
                ),
            )
            return True

        first = True
        for i in to_compensate:
            if first:
                self._create_and_enqueue_step(
                    conn,
                    run_id=run["id"],
                    tenant_id=run["tenant_id"],
                    steps_def=steps_def,
                    step_index=i,
                    direction=WorkflowStepDirection.COMPENSATE.value,
                    queue_name=queue_name,
                    max_attempts=max_attempts,
                    visibility_timeout_seconds=visibility_timeout_seconds,
                    now=now,
                )
                first = False
            else:
                sdef = steps_def[i]
                conn.execute(
                    f"""
                    INSERT INTO workflow_step_runs (
                        id, run_id, tenant_id, step_index, name, direction,
                        status, job_id, result, error, idempotency_key, retry_count,
                        started_at, completed_at, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, NULL, NULL, NULL, %s, 0,
                        NULL, NULL, %s, %s
                    )
                    ON CONFLICT (run_id, step_index, direction) DO NOTHING
                    """,
                    (
                        uuid4(),
                        run["id"],
                        run["tenant_id"],
                        i,
                        str(sdef.get("name") or f"step-{i}"),
                        WorkflowStepDirection.COMPENSATE.value,
                        WorkflowStepStatus.PENDING.value,
                        f"{run['id']}:{i}:compensate",
                        now,
                        now,
                    ),
                )
        return True

    def _fail_or_redrive_compensate_step(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: dict[str, Any],
        error: str,
        now: Any,
    ) -> None:
        """Redrive a killed/failed compensate job or continue reverse chain.

        Same ``idempotency_key`` is reused so external undo stays exactly-once
        even when a new job is enqueued after worker crash / lease expiry.
        """
        if run["status"] == WorkflowRunStatus.CANCELLED.value:
            return
        if step["status"] in (
            WorkflowStepStatus.COMPENSATED.value,
            WorkflowStepStatus.FAILED.value,
            WorkflowStepStatus.SKIPPED.value,
        ):
            return
        max_retries = int(
            run.get("max_compensate_retries")
            if run.get("max_compensate_retries") is not None
            else DEFAULT_MAX_COMPENSATE_RETRIES
        )
        retry = int(step.get("retry_count") or 0)
        # Inherit knobs from the dead job when present.
        job = None
        if step.get("job_id") is not None:
            job = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (step["job_id"],),
            ).fetchone()
        queue_name = job["queue_name"] if job else "default"
        max_attempts = int(job["max_attempts"]) if job else 3
        vt = int(job["visibility_timeout_seconds"]) if job else 30

        if retry < max_retries:
            # Only redrive when linked job is missing or terminal — never when
            # a healthy pending/leased job is still attached.
            if job is not None and job["status"] in (
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
            ):
                return
            new_retry = retry + 1
            updated = conn.execute(
                f"""
                UPDATE workflow_step_runs
                SET status = %s,
                    job_id = NULL,
                    error = %s,
                    retry_count = %s,
                    completed_at = NULL,
                    started_at = NULL,
                    updated_at = %s
                WHERE id = %s
                  AND status NOT IN ('compensated', 'failed', 'skipped')
                RETURNING {_STEP_COLUMNS}
                """,
                (
                    WorkflowStepStatus.PENDING.value,
                    error,
                    new_retry,
                    now,
                    step["id"],
                ),
            ).fetchone()
            if updated is None:
                return
            steps_def = self._parse_steps_def(run, conn)
            self._enqueue_job_for_step(
                conn,
                run_id=run["id"],
                tenant_id=run["tenant_id"],
                step_row=updated,
                steps_def=steps_def,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=vt,
                now=now,
                dedupe_suffix=f"r{new_retry}",
            )
            conn.execute(
                """
                UPDATE workflow_runs
                SET updated_at = %s
                WHERE id = %s AND status = %s
                """,
                (now, run["id"], WorkflowRunStatus.COMPENSATING.value),
            )
            return

        # Retry budget exhausted — mark this undo failed, continue reverse chain.
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = %s,
                error = %s,
                completed_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                WorkflowStepStatus.FAILED.value,
                f"{error} (after {retry} redrives)",
                now,
                now,
                step["id"],
            ),
        )
        reason = (
            f"compensate step {step['step_index']} ({step['name']}) failed "
            f"after {retry} redrives: {error}"
        )
        conn.execute(
            """
            UPDATE workflow_runs
            SET fail_reason = COALESCE(fail_reason || '; ', '') || %s,
                updated_at = %s
            WHERE id = %s
            """,
            (reason, now, run["id"]),
        )
        self._continue_compensate_chain(
            conn,
            run=run,
            after_step_index=int(step["step_index"]),
            queue_name=queue_name,
            max_attempts=max_attempts,
            visibility_timeout_seconds=vt,
            now=now,
        )

    def _continue_compensate_chain(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        after_step_index: int,
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        now: Any,
    ) -> None:
        """Enqueue next lower pending compensate job or finish the run."""
        if run["status"] == WorkflowRunStatus.CANCELLED.value:
            return
        # Re-read status in case cancel raced (caller held lock usually).
        cur = conn.execute(
            "SELECT status FROM workflow_runs WHERE id = %s",
            (run["id"],),
        ).fetchone()
        if cur is not None and cur["status"] == WorkflowRunStatus.CANCELLED.value:
            return
        nxt = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'compensate'
              AND status = 'pending'
              AND job_id IS NULL
              AND step_index < %s
            ORDER BY step_index DESC
            LIMIT 1
            FOR UPDATE
            """,
            (run["id"], after_step_index),
        ).fetchone()
        steps_def = self._parse_steps_def(run, conn)
        if nxt is not None:
            self._enqueue_job_for_step(
                conn,
                run_id=run["id"],
                tenant_id=run["tenant_id"],
                step_row=nxt,
                steps_def=steps_def,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
            return
        remaining = conn.execute(
            """
            SELECT 1 FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'compensate'
              AND status IN ('pending', 'running')
            LIMIT 1
            """,
            (run["id"],),
        ).fetchone()
        if remaining is None:
            # Prefer freshest fail_reason (cancel may have rewritten it).
            fresh = conn.execute(
                f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
                (run["id"],),
            ).fetchone()
            done_status = self._compensate_done_status(fresh or run)
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s
                  AND status = %s
                """,
                (
                    done_status,
                    now,
                    now,
                    run["id"],
                    WorkflowRunStatus.COMPENSATING.value,
                ),
            )

    def _ensure_compensate_progress(
        self, conn: Any, *, run: dict[str, Any], now: Any
    ) -> bool:
        """If compensating with no claimable job, enqueue next or redrive dead.

        A step with ``job_id`` pointing at a *pending* or *leased* job is
        healthy (not stuck), even if step status is still ``pending`` (job
        enqueued but not yet claimed).

        While sibling forward branches are still leased, do nothing — wait for
        natural terminal (never kill mid-step).
        """
        if self._active_forward_work(conn, run_id=run["id"]):
            return False

        # Compensate chain not started yet (waiting on settle) → begin now.
        any_comp = conn.execute(
            """
            SELECT 1 FROM workflow_step_runs
            WHERE run_id = %s AND direction = 'compensate'
            LIMIT 1
            """,
            (run["id"],),
        ).fetchone()
        if any_comp is None:
            steps_def = self._parse_steps_def(run, conn)
            return self._try_start_compensate_if_settled(
                conn,
                run=run,
                steps_def=steps_def,
                queue_name="default",
                max_attempts=3,
                visibility_timeout_seconds=30,
                now=now,
            )

        active = conn.execute(
            """
            SELECT 1
            FROM workflow_step_runs s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.run_id = %s
              AND s.direction = 'compensate'
              AND s.status IN ('pending', 'running')
              AND j.status IN (
                    'pending', 'leased', 'awaiting_skills',
                    'awaiting_capacity', 'awaiting_approval',
                    'awaiting_children'
                  )
            LIMIT 1
            """,
            (run["id"],),
        ).fetchone()
        if active is not None:
            return False
        # Highest-index non-terminal compensate that needs work.
        stuck = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s
              AND direction = 'compensate'
              AND status IN ('pending', 'running')
            ORDER BY step_index DESC
            LIMIT 1
            FOR UPDATE
            """,
            (run["id"],),
        ).fetchone()
        if stuck is None:
            done_status = self._compensate_done_status(run)
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s AND status = %s
                """,
                (
                    done_status,
                    now,
                    now,
                    run["id"],
                    WorkflowRunStatus.COMPENSATING.value,
                ),
            )
            return True
        # Linked job terminal or missing → redrive.
        job = None
        if stuck.get("job_id") is not None:
            job = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (stuck["job_id"],),
            ).fetchone()
        if job is not None and job["status"] in (
            JobStatus.PENDING.value,
            JobStatus.LEASED.value,
        ):
            return False
        if stuck.get("job_id") is not None:
            err = (
                (job.get("last_error") if job else None)
                or (f"job ended as {job['status']}" if job else "compensate job missing")
            )
            self._fail_or_redrive_compensate_step(
                conn,
                run=run,
                step=stuck,
                error=str(err),
                now=now,
            )
            return True
        # pending without job — enqueue first time only.
        steps_def = self._parse_steps_def(run, conn)
        self._enqueue_job_for_step(
            conn,
            run_id=run["id"],
            tenant_id=run["tenant_id"],
            step_row=stuck,
            steps_def=steps_def,
            queue_name="default",
            max_attempts=3,
            visibility_timeout_seconds=30,
            now=now,
        )
        return True

    def _complete_compensate_step(
        self,
        conn: Any,
        *,
        run: dict[str, Any],
        step: dict[str, Any],
        result: Any,
        now: Any,
    ) -> None:
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = %s,
                result = %s,
                completed_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                WorkflowStepStatus.COMPENSATED.value,
                Jsonb(result) if result is not None else None,
                now,
                now,
                step["id"],
            ),
        )
        # Cancelled mid-compensate: record this undo, do not chain further.
        if run["status"] == WorkflowRunStatus.CANCELLED.value:
            return
        job = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
            (step["job_id"],),
        ).fetchone()
        self._continue_compensate_chain(
            conn,
            run=run,
            after_step_index=int(step["step_index"]),
            queue_name=job["queue_name"] if job else "default",
            max_attempts=int(job["max_attempts"]) if job else 3,
            visibility_timeout_seconds=(
                int(job["visibility_timeout_seconds"]) if job else 30
            ),
            now=now,
        )

    def _create_and_enqueue_step(
        self,
        conn: Any,
        *,
        run_id: UUID,
        tenant_id: str,
        steps_def: list[dict[str, Any]],
        step_index: int,
        direction: str,
        queue_name: str,
        max_attempts: int,
        visibility_timeout_seconds: int,
        now: Any,
    ) -> dict[str, Any]:
        sdef = steps_def[step_index]
        if (
            direction == WorkflowStepDirection.FORWARD.value
            and str(sdef.get("kind") or "") == "map_join"
        ):
            # Map joins are materialized / completed by enqueue_ready, never
            # as a worker job.
            return {"id": None, "step_index": step_index, "job_id": None}
        step_id = uuid4()
        idem = f"{run_id}:{step_index}:{direction}"
        name = str(sdef.get("name") or f"step-{step_index}")
        conn.execute(
            f"""
            INSERT INTO workflow_step_runs (
                id, run_id, tenant_id, step_index, name, direction,
                status, job_id, result, error, idempotency_key, retry_count,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, NULL, NULL, NULL, %s, 0,
                NULL, NULL, %s, %s
            )
            ON CONFLICT (run_id, step_index, direction) DO NOTHING
            """,
            (
                step_id,
                run_id,
                tenant_id,
                step_index,
                name,
                direction,
                WorkflowStepStatus.PENDING.value,
                idem,
                now,
                now,
            ),
        )
        # Re-read in case of conflict (idempotent re-entry).
        step_row = conn.execute(
            f"""
            SELECT {_STEP_COLUMNS}
            FROM workflow_step_runs
            WHERE run_id = %s AND step_index = %s AND direction = %s
            FOR UPDATE
            """,
            (run_id, step_index, direction),
        ).fetchone()
        assert step_row is not None
        if step_row["job_id"] is not None:
            return step_row
        kind = str(sdef.get("kind") or "")
        if (
            direction == WorkflowStepDirection.FORWARD.value
            and kind == "wait_signal"
        ):
            return self._park_or_complete_wait_signal(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                step_row=step_row,
                steps_def=steps_def,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
        if direction == WorkflowStepDirection.FORWARD.value and kind in (
            "sleep",
            "delay",
            "timer",
            "timeout",
        ):
            return self._park_or_complete_sleep_or_timer(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                step_row=step_row,
                steps_def=steps_def,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
        if direction == WorkflowStepDirection.FORWARD.value and kind in (
            "approval",
            "wait_approval",
            "approve",
        ):
            return self._park_approval_step(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                step_row=step_row,
                steps_def=steps_def,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
        return self._enqueue_job_for_step(
            conn,
            run_id=run_id,
            tenant_id=tenant_id,
            step_row=step_row,
            steps_def=steps_def,
            queue_name=queue_name,
            max_attempts=max_attempts,
            visibility_timeout_seconds=visibility_timeout_seconds,
            now=now,
        )

    def _enqueue_job_for_step(
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
        dedupe_suffix: str | None = None,
    ) -> dict[str, Any]:
        if step_row.get("job_id") is not None:
            return step_row
        idx = int(step_row["step_index"])
        direction = step_row["direction"]
        # Forward intake is blocked on a drained queue. Compensate is cleanup
        # of work already accepted, so it may still enqueue.
        if (
            direction != WorkflowStepDirection.COMPENSATE.value
            and hasattr(self, "_assert_queue_accepts_enqueue")
        ):
            try:
                self._assert_queue_accepts_enqueue(conn, tenant_id, queue_name)
            except QueueDrainError:
                return step_row
        sdef = steps_def[idx]
        if direction == WorkflowStepDirection.COMPENSATE.value:
            action = dict(sdef.get("compensate") or {})
        else:
            action = {
                "kind": sdef.get("kind") or "effect",
                "payload": sdef.get("payload") or {},
            }
            if action["kind"] == "activity":
                for key in (
                    "activity",
                    "url",
                    "timeout_seconds",
                    "heartbeat_timeout_seconds",
                ):
                    if sdef.get(key) is not None:
                        action[key] = sdef[key]
        run_meta = conn.execute(
            """
            SELECT schedule_id, tick_iteration, tenant_id
            FROM workflow_runs
            WHERE id = %s
            """,
            (run_id,),
        ).fetchone()
        sched_id = (run_meta or {}).get("schedule_id")
        stamp_iter = (run_meta or {}).get("tick_iteration")
        job_id = uuid4()
        payload = {
            "kind": WORKFLOW_STEP_KIND,
            "workflow_run_id": str(run_id),
            "step_run_id": str(step_row["id"]),
            "direction": direction,
            "step_index": idx,
            "step_name": step_row["name"],
            "step": action,
            "idempotency_key": step_row["idempotency_key"],
        }
        if (
            sched_id
            and stamp_iter
            and direction == WorkflowStepDirection.FORWARD.value
            and hasattr(self, "_attach_generation_payload")
        ):
            sched_row = conn.execute(
                f"SELECT {_SCHEDULE_COLUMNS} FROM schedules WHERE id = %s",
                (sched_id,),
            ).fetchone()
            if sched_row is not None:
                try:
                    payload = self._attach_generation_payload(
                        conn,
                        sched_row,
                        payload,
                        iteration=int(stamp_iter),
                    )
                except Exception:
                    pass
        # Optional fail injection for tests/fleet.
        if direction == WorkflowStepDirection.FORWARD.value and sdef.get("fail"):
            payload["force_fail"] = True
            payload["error"] = str(sdef.get("fail_error") or "workflow step forced fail")
        labels = [f"workflow:{run_id}", f"wf-step:{idx}", f"wf-dir:{direction}"]
        # Include redrive suffix so a new pending job does not collide with a
        # still-active prior job row under the active dedupe unique index.
        dedupe = f"wf:{run_id}:{idx}:{direction}"
        if dedupe_suffix:
            dedupe = f"{dedupe}:{dedupe_suffix}"
        sealed, payload_key_id = self._seal_job_payload(
            conn, tenant_id=tenant_id, job_id=job_id, payload=payload
        )
        skills = _normalize_skills(sdef.get("required_skills"))
        status = (
            self._enqueue_status_for_skills(conn, skills)
            if hasattr(self, "_enqueue_status_for_skills")
            else JobStatus.PENDING.value
        )
        expires_at, expiry_action = self._workflow_step_expiry(
            conn, tenant_id=tenant_id, step_def=sdef, now=now
        )
        max_runtime_seconds = 3600
        vis = int(visibility_timeout_seconds)
        if str(action.get("kind") or "") == "activity":
            # Do not pin max_runtime to activity timeout here: that clock
            # starts at lease, but the activity clock starts later. Start
            # aligns the job deadline in start_workflow_activity.
            try:
                hb = float(action.get("heartbeat_timeout_seconds") or 0)
            except (TypeError, ValueError):
                hb = 0.0
            if hb > 0:
                vis = max(vis, int(hb * 3) + 1)
        # Compensation is cleanup, not new product work.
        if (
            direction != WorkflowStepDirection.COMPENSATE.value
            and hasattr(self, "_consume_spend_enqueues")
        ):
            self._consume_spend_enqueues(conn, tenant_id, extra=1, now=now)
        conn.execute(
            f"""
            INSERT INTO jobs (
                id, tenant_id, queue_name, payload, status, priority,
                attempts, max_attempts, visibility_timeout_seconds,
                max_runtime_seconds,
                retry_backoff_base_seconds, retry_backoff_max_seconds,
                available_at, dedupe_key, schedule_id, depends_on,
                labels, required_skills,
                deadline_at, estimated_duration_seconds, preemptible,
                created_at, updated_at, partition_id, payload_key_id,
                expires_at, expiry_action, tick_iteration
            ) VALUES (
                %s, %s, %s, %s, %s, 0,
                0, %s, %s,
                %s,
                0, 30,
                %s, %s, %s, %s,
                %s, %s,
                NULL, NULL, TRUE,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                job_id,
                tenant_id,
                queue_name,
                Jsonb(sealed),
                status,
                max_attempts,
                vis,
                max_runtime_seconds,
                now,
                dedupe,
                sched_id,
                [],
                labels,
                skills,
                now,
                now,
                (
                    self._compute_partition_id(
                        conn, tenant_id=tenant_id, job_id=job_id
                    )
                    if hasattr(self, "_compute_partition_id")
                    else 0
                ),
                payload_key_id,
                expires_at,
                expiry_action,
                int(stamp_iter) if stamp_iter else None,
            ),
        )
        if sched_id and stamp_iter:
            conn.execute(
                """
                UPDATE schedule_generations
                SET source_job_ids = (
                    SELECT ARRAY(
                        SELECT DISTINCT unnest(
                            COALESCE(source_job_ids, '{}') || %s::uuid[]
                        )
                    )
                ),
                    updated_at = %s
                WHERE schedule_id = %s
                  AND iteration = %s
                  AND tenant_id = %s
                  AND status = 'pending'
                """,
                (
                    [job_id],
                    now,
                    sched_id,
                    int(stamp_iter),
                    tenant_id,
                ),
            )
        # Keep step status pending until a worker actually leases the job.
        # Marking running here caused process_workflow_compensation to treat
        # healthy pending jobs as stuck and redrive (unique/dedupe conflicts).
        updated = conn.execute(
            f"""
            UPDATE workflow_step_runs
            SET job_id = %s,
                status = %s,
                updated_at = %s
            WHERE id = %s
              AND job_id IS NULL
            RETURNING {_STEP_COLUMNS}
            """,
            (
                job_id,
                WorkflowStepStatus.PENDING.value,
                now,
                step_row["id"],
            ),
        ).fetchone()
        if updated is None:
            # Concurrent enqueue won; re-read.
            updated = conn.execute(
                f"SELECT {_STEP_COLUMNS} FROM workflow_step_runs WHERE id = %s",
                (step_row["id"],),
            ).fetchone()
        assert updated is not None
        # Lightweight audit on the step job.
        job_row = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
            (job_id,),
        ).fetchone()
        if job_row is not None and hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=job_row,
                event_type="enqueued",
                from_status=None,
                actor=None,
                detail={"workflow_run_id": str(run_id), "direction": direction},
                now=now,
            )
        if job_row is not None and hasattr(self, "_record_usage"):
            self._record_usage(conn, tenant_id=tenant_id, now=now, enqueues=1)
        if job_row is not None and hasattr(self, "_stamp_job_quarantine"):
            self._stamp_job_quarantine(
                conn, job_id=job_id, tenant_id=tenant_id
            )
        if job_row is not None and hasattr(
            self, "_recheck_awaiting_skills_row_locked"
        ):
            self._recheck_awaiting_skills_row_locked(conn, job_row, now=now)
        return updated

    @staticmethod
    def _apply_workflow_ttl_defaults(
        steps: list[dict[str, Any]],
        *,
        ttl_seconds: float | int | None,
        expires_at: Any,
        expiry_action: str | None,
    ) -> None:
        if ttl_seconds is None and expires_at is None and not expiry_action:
            return
        exp_out = expires_at
        if hasattr(expires_at, "isoformat"):
            exp_out = expires_at.isoformat()
        for step in steps:
            if ttl_seconds is not None and step.get("ttl_seconds") is None:
                step["ttl_seconds"] = ttl_seconds
            if exp_out is not None and step.get("expires_at") is None:
                step["expires_at"] = exp_out
            if expiry_action and not step.get("expiry_action"):
                step["expiry_action"] = expiry_action
            nested = (step.get("map") or {}).get("step")
            if isinstance(nested, dict):
                if ttl_seconds is not None and nested.get("ttl_seconds") is None:
                    nested["ttl_seconds"] = ttl_seconds
                if exp_out is not None and nested.get("expires_at") is None:
                    nested["expires_at"] = exp_out
                if expiry_action and not nested.get("expiry_action"):
                    nested["expiry_action"] = expiry_action

    def _workflow_step_expiry(
        self,
        conn: Any,
        *,
        tenant_id: str,
        step_def: dict[str, Any],
        now: Any,
    ) -> tuple[Any, str]:
        ttl = step_def.get("ttl_seconds")
        exp_raw = step_def.get("expires_at")
        expires_at = None
        if exp_raw is not None:
            if hasattr(exp_raw, "isoformat"):
                expires_at = _as_utc(exp_raw)
            else:
                s = str(exp_raw).strip()
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                from datetime import datetime

                expires_at = _as_utc(datetime.fromisoformat(s))
        action = step_def.get("expiry_action")
        if hasattr(self, "_resolve_job_expiry"):
            return self._resolve_job_expiry(
                conn,
                tenant_id=tenant_id,
                now=now,
                ttl_seconds=ttl,
                expires_at=expires_at,
                expiry_action=action,
            )
        return None, "dead"

    def _mark_workflow_step_leased(
        self, conn: Any, job_row: dict[str, Any], *, now: Any = None
    ) -> None:
        """Flip step_run pending→running when its job is actually leased."""
        payload = self._open_job_payload(
            job_row.get("payload"),
            tenant_id=str(job_row["tenant_id"]),
            job_id=job_row["id"],
            conn=conn,
        )
        if payload.get("kind") != WORKFLOW_STEP_KIND:
            return
        ts = now or _utcnow()
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET status = %s,
                started_at = COALESCE(started_at, %s),
                updated_at = %s
            WHERE job_id = %s
              AND status = %s
            """,
            (
                WorkflowStepStatus.RUNNING.value,
                ts,
                ts,
                job_row["id"],
                WorkflowStepStatus.PENDING.value,
            ),
        )

    @classmethod
    def _expand_and_normalize_steps(
        cls, raw_steps: Sequence[Any]
    ) -> list[dict[str, Any]]:
        """Flatten linear + parallel + map blocks into a DAG of leaf steps.

        Parallel block form::

            {
              "type": "parallel",  # or kind: "parallel"
              "name": "fan",
              "branches": [
                [{"name": "a1", ...}, {"name": "a2", ...}],
                [{"name": "b1", ...}],
              ],
            }

        Map / foreach form (N children from the previous step's result)::

            {
              "type": "map",
              "name": "each",
              "items": "ids",
              "max_items": 256,
              "step": {"name": "work", "payload": {"op": "do"}},
            }

        If / switch form (exactly one path runs)::

            {
              "type": "if",
              "name": "route",
              "on": "choice",
              "equals": "vip",
              "then": [{"name": "vip-work", ...}],
              "else": [{"name": "std-work", ...}],
            }

        Unused branch steps are marked ``skipped`` so the join is not
        stuck. ``cases`` is a multi-way switch; first match wins.

        Each parallel branch runs concurrently after the previous wave; the
        next sequential step depends on every branch tail (join). A map
        placeholder joins after every spawned child completes.
        """
        if not raw_steps:
            raise ValueError("steps must be a non-empty list")
        flat: list[dict[str, Any]] = []
        # Indices of the previous wave that subsequent steps depend on.
        prev_wave: list[int] = []

        def _is_parallel(item: Any) -> bool:
            if not isinstance(item, dict):
                return False
            if _is_map_block(item):
                return False
            t = str(item.get("type") or item.get("kind") or "").strip().lower()
            return t == "parallel" or isinstance(item.get("branches"), list)

        def _append_leaf(
            raw: Any,
            *,
            depends: list[int],
            parallel_group: str | None = None,
            branch_id: int | None = None,
            path: str,
        ) -> int:
            if _is_parallel(raw) or _is_map_block(raw) or _is_if_block(raw):
                raise ValueError(
                    f"{path}: nested parallel/map/if blocks are not supported"
                )
            idx = len(flat)
            leaf = cls._normalize_leaf_step(raw, idx, path=path)
            leaf["depends_on_indices"] = list(depends)
            if parallel_group is not None:
                leaf["parallel_group"] = parallel_group
            if branch_id is not None:
                leaf["branch_id"] = int(branch_id)
            flat.append(leaf)
            return idx

        for bi, item in enumerate(raw_steps):
            path = f"steps[{bi}]"
            if not isinstance(item, dict):
                raise ValueError(f"{path} must be an object")
            if _is_parallel(item):
                branches = item.get("branches")
                if not isinstance(branches, list) or not branches:
                    raise ValueError(f"{path}.branches must be a non-empty list")
                pg = str(item.get("name") or f"parallel-{bi}").strip()
                tails: list[int] = []
                for b_i, branch in enumerate(branches):
                    if not isinstance(branch, list) or not branch:
                        raise ValueError(
                            f"{path}.branches[{b_i}] must be a non-empty step list"
                        )
                    dep = list(prev_wave)
                    last: int | None = None
                    for s_i, sraw in enumerate(branch):
                        last = _append_leaf(
                            sraw,
                            depends=dep if s_i == 0 else [int(last)],  # type: ignore[arg-type]
                            parallel_group=pg,
                            branch_id=b_i,
                            path=f"{path}.branches[{b_i}][{s_i}]",
                        )
                        dep = [last]
                    assert last is not None
                    tails.append(last)
                if not tails:
                    raise ValueError(f"{path}: parallel block produced no steps")
                prev_wave = tails
            elif _is_map_block(item):
                template = item.get("step") or item.get("do")
                if not isinstance(template, dict):
                    raise ValueError(f"{path}.step must be an object")
                pg = str(item.get("name") or f"map-{bi}").strip()
                if not pg:
                    raise ValueError(f"{path}.name is required")
                items_path = str(
                    item.get("items") or item.get("items_path") or "items"
                ).strip() or "items"
                raw_max = item.get("max_items")
                if raw_max is None:
                    max_items = int(DEFAULT_MAP_MAX_ITEMS)
                else:
                    try:
                        max_items = int(raw_max)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"{path}.max_items must be an integer"
                        ) from exc
                    if max_items < 0:
                        raise ValueError(f"{path}.max_items must be >= 0")
                leaf_idx = len(flat)
                tmpl = cls._normalize_leaf_step(
                    template, leaf_idx, path=f"{path}.step"
                )
                tmpl["depends_on_indices"] = []
                tmpl.pop("depends_on", None)
                if _is_map_block(template) or _is_parallel(template):
                    raise ValueError(f"{path}.step cannot be a map or parallel block")
                join = {
                    "name": pg,
                    "kind": "map_join",
                    "payload": {},
                    "depends_on_indices": list(prev_wave),
                    "map_group": pg,
                    "map": {
                        "items": items_path,
                        "max_items": max_items,
                        "from_indices": list(prev_wave),
                        "step": tmpl,
                    },
                }
                if item.get("required_skills") is not None:
                    join["required_skills"] = list(
                        _normalize_skills(item.get("required_skills"))
                    )
                    tmpl.setdefault(
                        "required_skills", join["required_skills"]
                    )
                flat.append(join)
                prev_wave = [leaf_idx]
            elif _is_if_block(item):
                cases = normalize_if_cases(item, path=path)
                # Display name is not unique; group by expansion index.
                pg = if_block_group(index=bi)
                tails: list[int] = []
                for c_i, case in enumerate(cases):
                    dep = list(prev_wave)
                    last: int | None = None
                    for s_i, sraw in enumerate(case["steps"]):
                        last = _append_leaf(
                            sraw,
                            depends=dep if s_i == 0 else [int(last)],  # type: ignore[arg-type]
                            path=f"{path}.branch[{c_i}][{s_i}]",
                        )
                        flat[last]["branch"] = {
                            "group": pg,
                            "id": c_i,
                            "pred": dict(case["pred"]),
                            "source_indices": list(prev_wave),
                        }
                        dep = [last]
                    assert last is not None
                    tails.append(last)
                if not tails:
                    raise ValueError(f"{path}: if/switch produced no steps")
                prev_wave = tails
            else:
                idx = _append_leaf(
                    item,
                    depends=list(prev_wave),
                    path=path,
                )
                prev_wave = [idx]

        if not flat:
            raise ValueError("steps expanded to an empty workflow")
        # Resolve optional depends_on by name (advanced); indices win if both set.
        by_name = {s["name"]: i for i, s in enumerate(flat)}
        if len(by_name) != len(flat):
            # Duplicate names only illegal when referenced by depends_on.
            pass
        for i, s in enumerate(flat):
            if s.get("depends_on_indices") is not None and "depends_on" not in s:
                # Already set by expansion; drop any raw name list residue.
                s.pop("depends_on", None)
                continue
            names = s.pop("depends_on", None)
            if names is None:
                s.setdefault("depends_on_indices", [])
                continue
            if not isinstance(names, list):
                raise ValueError(f"steps[{i}].depends_on must be a list of names")
            deps: list[int] = []
            for n in names:
                key = str(n).strip()
                if key not in by_name:
                    raise ValueError(
                        f"steps[{i}].depends_on unknown step name {key!r}"
                    )
                di = by_name[key]
                if di >= i:
                    raise ValueError(
                        f"steps[{i}].depends_on {key!r} must precede the step"
                    )
                deps.append(di)
            s["depends_on_indices"] = deps
        return flat

    @staticmethod
    def _normalize_leaf_step(raw: Any, index: int, *, path: str = "") -> dict[str, Any]:
        label = path or f"steps[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object")
        if str(raw.get("type") or "").strip().lower() == "parallel":
            raise ValueError(f"{label}: unexpected parallel marker on leaf")
        name = str(raw.get("name") or f"step-{index}").strip()
        if not name:
            raise ValueError(f"{label}.name is required")
        kind = str(raw.get("kind") or "effect").strip()
        if kind == "parallel":
            raise ValueError(f"{label}: kind=parallel requires branches")
        if kind in ("map", "foreach", "fanout", "map_join"):
            raise ValueError(
                f"{label}: map/foreach must be a block with a step template"
            )
        payload = raw.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError(f"{label}.payload must be an object")
        out: dict[str, Any] = {
            "name": name,
            "kind": kind,
            "payload": dict(payload),
            "depends_on_indices": [],
        }
        if kind == "wait_signal":
            from job_queue.queue.workflows_signals import normalize_signal_name

            out["signal"] = normalize_signal_name(
                raw.get("signal") or raw.get("signal_name"),
                label=f"{label}.signal",
            )
            if raw.get("timeout_seconds") is not None:
                try:
                    timeout = float(raw["timeout_seconds"])
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"{label}.timeout_seconds must be a number"
                    ) from exc
                if timeout < 0:
                    raise ValueError(f"{label}.timeout_seconds must be >= 0")
                out["timeout_seconds"] = timeout
            if raw.get("deadline_at") is not None:
                out["deadline_at"] = raw["deadline_at"]
        if kind in ("if", "switch", "branch", "choose"):
            raise ValueError(f"{label}: if/switch requires then or cases")
        if kind == "activity":
            from job_queue.queue.workflows_activities import apply_activity_fields

            apply_activity_fields(out, raw, label=label)
        if kind in ("sleep", "delay", "timer", "timeout"):
            from job_queue.queue.workflows_timers import wait_deadline

            for key in (
                "seconds",
                "sleep_seconds",
                "duration_seconds",
                "timeout_seconds",
            ):
                if raw.get(key) is not None:
                    out[key] = raw[key]
            for key in ("deadline_at", "until", "wake_at"):
                if raw.get(key) is not None:
                    out[key] = raw[key]
            # Validate now so a bad spec fails at start, not at park.
            wait_deadline(out, now=_utcnow(), kind=kind)
        if kind in ("approval", "wait_approval", "approve"):
            from job_queue.queue.workflows_approvals import apply_approval_fields

            apply_approval_fields(out, raw, label=label)
        if raw.get("fail"):
            out["fail"] = True
            if raw.get("fail_error") is not None:
                out["fail_error"] = str(raw["fail_error"])
        comp = raw.get("compensate")
        if comp is not None:
            if not isinstance(comp, dict):
                raise ValueError(f"{label}.compensate must be an object")
            ckind = str(comp.get("kind") or "effect").strip()
            if ckind == "wait_signal":
                raise ValueError(f"{label}.compensate cannot wait for a signal")
            if ckind in ("sleep", "delay", "timer", "timeout"):
                raise ValueError(f"{label}.compensate cannot sleep or time out")
            if ckind in ("approval", "wait_approval", "approve"):
                raise ValueError(f"{label}.compensate cannot wait for approval")
            out["compensate"] = {
                "kind": ckind,
                "payload": dict(comp.get("payload") or {}),
            }
            if ckind == "activity":
                from job_queue.queue.workflows_activities import apply_activity_fields

                apply_activity_fields(
                    out["compensate"], comp, label=f"{label}.compensate"
                )
        # Preserve explicit name-based depends_on for post-pass resolution.
        if raw.get("depends_on") is not None:
            out["depends_on"] = raw["depends_on"]
        if raw.get("required_skills") is not None:
            out["required_skills"] = list(
                _normalize_skills(raw.get("required_skills"))
            )
        if raw.get("ttl_seconds") is not None:
            out["ttl_seconds"] = raw["ttl_seconds"]
        if raw.get("expires_at") is not None:
            out["expires_at"] = raw["expires_at"]
        if raw.get("expiry_action") is not None:
            out["expiry_action"] = raw["expiry_action"]
        return out

    @staticmethod
    def _normalize_step_def(raw: Any, index: int) -> dict[str, Any]:
        """Backward-compatible single-leaf normalizer."""
        return WorkflowsMixin._normalize_leaf_step(raw, index)

    def _row_to_workflow_run(
        self, row: dict[str, Any], *, conn: Any | None = None
    ) -> WorkflowRun:
        steps = self._parse_steps_def(row, conn)
        return WorkflowRun(
            id=row["id"],
            tenant_id=row["tenant_id"],
            name=row.get("name"),
            status=row["status"],
            steps_def=list(steps),
            current_index=int(row["current_index"]),
            fail_step_index=row.get("fail_step_index"),
            fail_reason=row.get("fail_reason"),
            max_compensate_retries=int(
                row.get("max_compensate_retries")
                if row.get("max_compensate_retries") is not None
                else DEFAULT_MAX_COMPENSATE_RETRIES
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row.get("completed_at"),
            schedule_id=row.get("schedule_id"),
            tick_iteration=(
                None
                if row.get("tick_iteration") is None
                else int(row["tick_iteration"])
            ),
        )

    @staticmethod
    def _row_to_workflow_step(row: dict[str, Any]) -> WorkflowStepRun:
        result = row.get("result")
        if isinstance(result, str):
            import json

            result = json.loads(result)
        return WorkflowStepRun(
            id=row["id"],
            run_id=row["run_id"],
            tenant_id=row["tenant_id"],
            step_index=int(row["step_index"]),
            name=row["name"],
            direction=row["direction"],
            status=row["status"],
            job_id=row.get("job_id"),
            result=result,
            error=row.get("error"),
            idempotency_key=row["idempotency_key"],
            retry_count=int(row.get("retry_count") or 0),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            deadline_at=row.get("deadline_at"),
        )

    def _workflow_run_ids_for_schedule(
        self,
        conn: Any,
        *,
        tenant_id: str,
        schedule_id: UUID,
    ) -> list[UUID]:
        rows = conn.execute(
            """
            SELECT id
            FROM workflow_runs
            WHERE schedule_id = %s AND tenant_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (schedule_id, tenant_id),
        ).fetchall()
        return [r["id"] for r in rows]

    def _workflow_step_job_ids(
        self, conn: Any, *, run_ids: list[UUID]
    ) -> list[UUID]:
        if not run_ids:
            return []
        rows = conn.execute(
            """
            SELECT job_id
            FROM workflow_step_runs
            WHERE run_id = ANY(%s) AND job_id IS NOT NULL
            ORDER BY job_id
            """,
            (run_ids,),
        ).fetchall()
        return [r["job_id"] for r in rows]

    def _move_workflow_runs_for_schedule(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        from_tenant: str,
        to_tenant: str,
        now: Any,
    ) -> int:
        """Move this schedule's runs, steps, and signals onto *to_tenant*.

        ``steps_def`` and signal payloads are resealed under dest so dest
        can decrypt after the source DEK rows stay behind.
        """
        runs = conn.execute(
            f"""
            SELECT {_RUN_COLUMNS}
            FROM workflow_runs
            WHERE schedule_id = %s AND tenant_id = %s
            ORDER BY created_at ASC, id ASC
            FOR UPDATE
            """,
            (schedule_id, from_tenant),
        ).fetchall()
        if not runs:
            return 0
        run_ids = [r["id"] for r in runs]
        for run in runs:
            steps = self._parse_steps_def(run, conn)
            sealed, kid = self._seal_dict(
                conn,
                tenant_id=to_tenant,
                plaintext={"steps": steps},
                aad=workflow_steps_aad(run["id"]),
            )
            updated = conn.execute(
                f"""
                UPDATE workflow_runs
                SET tenant_id = %s,
                    steps_def = %s,
                    payload_key_id = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (to_tenant, Jsonb(sealed), kid, now, run["id"], from_tenant),
            ).fetchone()
            if updated is None:
                raise ScheduleError(
                    f"schedule {schedule_id} cannot move: workflow run "
                    f"{run['id']} raced"
                )
        conn.execute(
            """
            UPDATE workflow_step_runs
            SET tenant_id = %s,
                updated_at = %s
            WHERE run_id = ANY(%s) AND tenant_id = %s
            """,
            (to_tenant, now, run_ids, from_tenant),
        )
        if hasattr(self, "_seal_signal_payload"):
            sigs = conn.execute(
                """
                SELECT id, run_id, tenant_id, name, payload
                FROM workflow_signals
                WHERE run_id = ANY(%s) AND tenant_id = %s
                FOR UPDATE
                """,
                (run_ids, from_tenant),
            ).fetchall()
            for sig in sigs:
                opened = self._open_signal_payload(
                    sig.get("payload"),
                    tenant_id=from_tenant,
                    run_id=sig["run_id"],
                    name=str(sig["name"]),
                    conn=conn,
                )
                sealed, kid = self._seal_signal_payload(
                    conn,
                    tenant_id=to_tenant,
                    run_id=sig["run_id"],
                    name=str(sig["name"]),
                    payload=opened,
                )
                conn.execute(
                    """
                    UPDATE workflow_signals
                    SET tenant_id = %s,
                        payload = %s,
                        payload_key_id = %s
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (
                        to_tenant,
                        None if sealed is None else Jsonb(sealed),
                        kid,
                        sig["id"],
                        from_tenant,
                    ),
                )
        else:
            conn.execute(
                """
                UPDATE workflow_signals
                SET tenant_id = %s
                WHERE run_id = ANY(%s) AND tenant_id = %s
                """,
                (to_tenant, run_ids, from_tenant),
            )
        return len(runs)

