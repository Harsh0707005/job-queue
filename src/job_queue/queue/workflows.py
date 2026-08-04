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
    fail_step_index, fail_reason, created_at, updated_at, completed_at
"""

_STEP_COLUMNS = """
    id, run_id, tenant_id, step_index, name, direction, status, job_id,
    result, error, idempotency_key, started_at, completed_at,
    created_at, updated_at
"""


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
        run_id: UUID | None = None,
    ) -> WorkflowRun:
        """Start a multi-step workflow and enqueue the first forward step job."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if not steps or not isinstance(steps, list):
            raise ValueError("steps must be a non-empty list")
        normalized = [self._normalize_step_def(s, i) for i, s in enumerate(steps)]
        now = _utcnow()
        rid = run_id or uuid4()
        with transaction(self._pool) as conn:
            conn.execute(
                f"""
                INSERT INTO workflow_runs (
                    id, tenant_id, name, status, steps_def, current_index,
                    fail_step_index, fail_reason, created_at, updated_at,
                    completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 0,
                    NULL, NULL, %s, %s, NULL
                )
                """,
                (
                    rid,
                    tenant_id,
                    str(name).strip() if name else None,
                    WorkflowRunStatus.RUNNING.value,
                    Jsonb(normalized),
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
                # Compensate job failed terminally — mark failed and end run.
                conn.execute(
                    """
                    UPDATE workflow_step_runs
                    SET status = %s, error = %s, completed_at = %s, updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        WorkflowStepStatus.FAILED.value,
                        error or "compensate failed",
                        ts,
                        ts,
                        step["id"],
                    ),
                )
                conn.execute(
                    """
                    UPDATE workflow_runs
                    SET status = %s,
                        fail_reason = %s,
                        completed_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        WorkflowRunStatus.FAILED.value,
                        f"compensate step {step['step_index']} failed: {error}",
                        ts,
                        ts,
                        run["id"],
                    ),
                )

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
                        status, job_id, result, error, idempotency_key,
                        started_at, completed_at, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, NULL, NULL, NULL, %s,
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
                        status, job_id, result, error, idempotency_key,
                        started_at, completed_at, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, NULL, NULL, NULL, %s,
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
        # Next lower pending compensate without a job yet.
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
            (run["id"], step["step_index"]),
        ).fetchone()
        steps_def = run["steps_def"]
        if isinstance(steps_def, str):
            import json

            steps_def = json.loads(steps_def)
        if nxt is not None:
            job = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (step["job_id"],),
            ).fetchone()
            # Attach a job to this pending compensate row.
            self._enqueue_job_for_step(
                conn,
                run_id=run["id"],
                tenant_id=run["tenant_id"],
                step_row=nxt,
                steps_def=steps_def,
                queue_name=job["queue_name"] if job else "default",
                max_attempts=int(job["max_attempts"]) if job else 3,
                visibility_timeout_seconds=(
                    int(job["visibility_timeout_seconds"]) if job else 30
                ),
                now=now,
            )
            return
        # No more compensates → run failed (saga rolled back).
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
                status, job_id, result, error, idempotency_key,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, NULL, NULL, NULL, %s,
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
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, 0,
                0, %s, %s,
                3600,
                0, 30,
                %s, %s, NULL, %s,
                %s,
                NULL, NULL, TRUE,
                %s, %s
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
                f"wf:{run_id}:{idx}:{direction}",
                [],
                labels,
                now,
                now,
            ),
        )
        updated = conn.execute(
            f"""
            UPDATE workflow_step_runs
            SET job_id = %s, status = %s, started_at = COALESCE(started_at, %s),
                updated_at = %s
            WHERE id = %s
            RETURNING {_STEP_COLUMNS}
            """,
            (
                job_id,
                WorkflowStepStatus.RUNNING.value,
                now,
                now,
                step_row["id"],
            ),
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
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
