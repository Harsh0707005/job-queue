"""Multi-step saga workflows with reverse-order compensation.

Each step is a normal queue job (single-worker lease). On terminal forward
failure the orchestrator enqueues compensate jobs for completed steps in
reverse order. Durable step_run rows + unique (run, index, direction) prevent
double compensation under reclaim or dual coordinators.
"""

from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    WorkflowRun,
    WorkflowRunStatus,
    WorkflowStepDirection,
    WorkflowStepRun,
    WorkflowStepStatus,
)
from job_queue.queue._common import _JOB_COLUMNS, _row_to_job, _utcnow

WORKFLOW_STEP_KIND = "workflow.step"

_RUN_COLUMNS = """
    id, tenant_id, name, status, steps_def, current_index,
    fail_step_index, fail_reason, max_compensate_retries,
    created_at, updated_at, completed_at
"""

_STEP_COLUMNS = """
    id, run_id, tenant_id, step_index, name, direction, status, job_id,
    result, error, idempotency_key, retry_count, started_at, completed_at,
    created_at, updated_at
"""

# Default redrives when a compensate job dies (crash / timeout / nack).
DEFAULT_MAX_COMPENSATE_RETRIES = 5


class WorkflowsMixin:
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
    ) -> WorkflowRun:
        """Start a multi-step workflow and enqueue the first forward step job."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if not steps or not isinstance(steps, list):
            raise ValueError("steps must be a non-empty list")
        if int(max_compensate_retries) < 0:
            raise ValueError("max_compensate_retries must be >= 0")
        normalized = [self._normalize_step_def(s, i) for i, s in enumerate(steps)]
        now = _utcnow()
        rid = run_id or uuid4()
        with transaction(self._pool) as conn:
            conn.execute(
                f"""
                INSERT INTO workflow_runs (
                    id, tenant_id, name, status, steps_def, current_index,
                    fail_step_index, fail_reason, max_compensate_retries,
                    created_at, updated_at, completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 0,
                    NULL, NULL, %s,
                    %s, %s, NULL
                )
                """,
                (
                    rid,
                    tenant_id,
                    str(name).strip() if name else None,
                    WorkflowRunStatus.RUNNING.value,
                    Jsonb(normalized),
                    int(max_compensate_retries),
                    now,
                    now,
                ),
            )
            self._create_and_enqueue_step(
                conn,
                run_id=rid,
                tenant_id=tenant_id,
                steps_def=normalized,
                step_index=0,
                direction=WorkflowStepDirection.FORWARD.value,
                queue_name=queue_name,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )
            row = conn.execute(
                f"SELECT {_RUN_COLUMNS} FROM workflow_runs WHERE id = %s",
                (rid,),
            ).fetchone()
        assert row is not None
        return self._row_to_workflow_run(row)

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
        payload = job_row.get("payload") or {}
        if isinstance(payload, str):
            import json

            payload = json.loads(payload)
        if not isinstance(payload, dict):
            return
        if payload.get("kind") != WORKFLOW_STEP_KIND:
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
        return fixed

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
        steps_def = run["steps_def"]
        if isinstance(steps_def, str):
            import json

            steps_def = json.loads(steps_def)
        next_idx = int(step["step_index"]) + 1
        if next_idx >= len(steps_def):
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s,
                    current_index = %s,
                    completed_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    WorkflowRunStatus.COMPLETED.value,
                    next_idx,
                    now,
                    now,
                    run["id"],
                ),
            )
            return
        conn.execute(
            """
            UPDATE workflow_runs
            SET current_index = %s, updated_at = %s
            WHERE id = %s
            """,
            (next_idx, now, run["id"]),
        )
        # Inherit job knobs from the completed step job when present.
        job = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
            (step["job_id"],),
        ).fetchone()
        self._create_and_enqueue_step(
            conn,
            run_id=run["id"],
            tenant_id=run["tenant_id"],
            steps_def=steps_def,
            step_index=next_idx,
            direction=WorkflowStepDirection.FORWARD.value,
            queue_name=job["queue_name"] if job else "default",
            max_attempts=int(job["max_attempts"]) if job else 3,
            visibility_timeout_seconds=(
                int(job["visibility_timeout_seconds"]) if job else 30
            ),
            now=now,
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
        steps_def = run["steps_def"]
        if isinstance(steps_def, str):
            import json

            steps_def = json.loads(steps_def)

        # Build reverse compensate chain for completed forwards only.
        # Enqueue highest-index compensate first; others wait until prior acks.
        to_compensate: list[int] = []
        for i in range(fail_idx - 1, -1, -1):
            completed = conn.execute(
                """
                SELECT id, status FROM workflow_step_runs
                WHERE run_id = %s AND step_index = %s AND direction = 'forward'
                """,
                (run["id"], i),
            ).fetchone()
            if completed is None or completed["status"] != WorkflowStepStatus.COMPLETED.value:
                continue
            sdef = steps_def[i] if i < len(steps_def) else {}
            if not sdef.get("compensate"):
                # No undo defined — record skipped.
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
                        str(sdef.get("name") or f"step-{i}"),
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
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s
                """,
                (WorkflowRunStatus.FAILED.value, now, now, run["id"]),
            )
            return

        # Create pending compensate step_runs for all; enqueue only first.
        job = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
            (step["job_id"],),
        ).fetchone()
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
                    queue_name=job["queue_name"] if job else "default",
                    max_attempts=int(job["max_attempts"]) if job else 3,
                    visibility_timeout_seconds=(
                        int(job["visibility_timeout_seconds"]) if job else 30
                    ),
                    now=now,
                )
                first = False
            else:
                # Placeholder pending without job until previous compensate acks.
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
            steps_def = run["steps_def"]
            if isinstance(steps_def, str):
                import json

                steps_def = json.loads(steps_def)
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
        steps_def = run["steps_def"]
        if isinstance(steps_def, str):
            import json

            steps_def = json.loads(steps_def)
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
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s
                """,
                (WorkflowRunStatus.FAILED.value, now, now, run["id"]),
            )

    def _ensure_compensate_progress(
        self, conn: Any, *, run: dict[str, Any], now: Any
    ) -> bool:
        """If compensating with no claimable job, enqueue next or redrive dead.

        A step with ``job_id`` pointing at a *pending* or *leased* job is
        healthy (not stuck), even if step status is still ``pending`` (job
        enqueued but not yet claimed).
        """
        active = conn.execute(
            """
            SELECT 1
            FROM workflow_step_runs s
            JOIN jobs j ON j.id = s.job_id
            WHERE s.run_id = %s
              AND s.direction = 'compensate'
              AND s.status IN ('pending', 'running')
              AND j.status IN ('pending', 'leased')
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
            conn.execute(
                """
                UPDATE workflow_runs
                SET status = %s, completed_at = %s, updated_at = %s
                WHERE id = %s AND status = %s
                """,
                (
                    WorkflowRunStatus.FAILED.value,
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
        steps_def = run["steps_def"]
        if isinstance(steps_def, str):
            import json

            steps_def = json.loads(steps_def)
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
        sdef = steps_def[idx]
        if direction == WorkflowStepDirection.COMPENSATE.value:
            action = dict(sdef.get("compensate") or {})
        else:
            action = {
                "kind": sdef.get("kind") or "effect",
                "payload": sdef.get("payload") or {},
            }
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
        conn.execute(
            f"""
            INSERT INTO jobs (
                id, tenant_id, queue_name, payload, status, priority,
                attempts, max_attempts, visibility_timeout_seconds,
                max_runtime_seconds,
                retry_backoff_base_seconds, retry_backoff_max_seconds,
                available_at, dedupe_key, schedule_id, depends_on,
                labels,
                deadline_at, estimated_duration_seconds, preemptible,
                created_at, updated_at, partition_id
            ) VALUES (
                %s, %s, %s, %s, %s, 0,
                0, %s, %s,
                3600,
                0, 30,
                %s, %s, NULL, %s,
                %s,
                NULL, NULL, TRUE,
                %s, %s, %s
            )
            """,
            (
                job_id,
                tenant_id,
                queue_name,
                Jsonb(payload),
                JobStatus.PENDING.value,
                max_attempts,
                visibility_timeout_seconds,
                now,
                dedupe,
                [],
                labels,
                now,
                now,
                (
                    self._compute_partition_id(
                        conn, tenant_id=tenant_id, job_id=job_id
                    )
                    if hasattr(self, "_compute_partition_id")
                    else 0
                ),
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
        return updated

    def _mark_workflow_step_leased(
        self, conn: Any, job_row: dict[str, Any], *, now: Any = None
    ) -> None:
        """Flip step_run pending→running when its job is actually leased."""
        payload = job_row.get("payload") or {}
        if isinstance(payload, str):
            import json

            payload = json.loads(payload)
        if not isinstance(payload, dict) or payload.get("kind") != WORKFLOW_STEP_KIND:
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

    @staticmethod
    def _normalize_step_def(raw: Any, index: int) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError(f"steps[{index}] must be an object")
        name = str(raw.get("name") or f"step-{index}").strip()
        if not name:
            raise ValueError(f"steps[{index}].name is required")
        kind = str(raw.get("kind") or "effect").strip()
        payload = raw.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ValueError(f"steps[{index}].payload must be an object")
        out: dict[str, Any] = {
            "name": name,
            "kind": kind,
            "payload": dict(payload),
        }
        if raw.get("fail"):
            out["fail"] = True
            if raw.get("fail_error") is not None:
                out["fail_error"] = str(raw["fail_error"])
        comp = raw.get("compensate")
        if comp is not None:
            if not isinstance(comp, dict):
                raise ValueError(f"steps[{index}].compensate must be an object")
            out["compensate"] = {
                "kind": str(comp.get("kind") or "effect").strip(),
                "payload": dict(comp.get("payload") or {}),
            }
        return out

    @staticmethod
    def _row_to_workflow_run(row: dict[str, Any]) -> WorkflowRun:
        steps = row.get("steps_def") or []
        if isinstance(steps, str):
            import json

            steps = json.loads(steps)
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
        )
