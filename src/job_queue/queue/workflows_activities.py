"""External workflow activities: start, heartbeat, timeout → compensate.

Stage 1 — start: the worker records a durable running row (idempotent on
``step_run_id``) and POSTs to the activity URL.
Stage 2 — heartbeat: the worker (holding the lease) refreshes
``last_heartbeat_at`` / ``heartbeat_due_at``.
Stage 3 — timeout: a missed heartbeat or start-to-close deadline marks the
activity failed, dead-letters the step job, and starts reverse compensate.
The same start is never retried after a timeout.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    WebhookEvent,
    WorkflowActivity,
    WorkflowStepStatus,
)
from job_queue.queue._common import (
    LeaseError,
    _JOB_COLUMNS,
    _as_utc,
    _utcnow,
)
from job_queue.queue.workflows import WORKFLOW_STEP_KIND, _STEP_COLUMNS


_ACTIVITY_COLUMNS = """
    id, step_run_id, job_id, run_id, tenant_id, name, url, status,
    timeout_seconds, heartbeat_timeout_seconds, started_at,
    last_heartbeat_at, heartbeat_due_at, timeout_at, completed_at,
    result, error, created_at, updated_at
"""


def validate_activity_url(url: str, *, resolve_dns: bool = True) -> str:
    """Same SSRF rules as webhooks: http(s), no credentials, no metadata."""
    from job_queue.webhooks import validate_webhook_url

    try:
        return validate_webhook_url(url, resolve_dns=resolve_dns)
    except ValueError as exc:
        msg = str(exc).replace("webhook URL", "activity URL")
        if msg.startswith("url "):
            msg = "activity " + msg
        raise ValueError(msg) from exc


def parse_activity_seconds(raw: Any, *, label: str, default: float | None = None) -> float:
    if raw is None:
        if default is None:
            raise ValueError(f"{label} is required")
        return float(default)
    try:
        seconds = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    if seconds <= 0:
        raise ValueError(f"{label} must be > 0")
    return seconds


def apply_activity_fields(
    dest: dict[str, Any], raw: dict[str, Any], *, label: str
) -> None:
    name = str(raw.get("activity") or raw.get("name") or dest.get("name") or "").strip()
    if not name:
        raise ValueError(f"{label}.activity is required")
    dest["activity"] = name
    if raw.get("url") is not None and str(raw.get("url") or "").strip():
        dest["url"] = validate_activity_url(str(raw["url"]), resolve_dns=True)
    timeout = parse_activity_seconds(
        raw.get("timeout_seconds") or raw.get("start_to_close_seconds"),
        label=f"{label}.timeout_seconds",
    )
    dest["timeout_seconds"] = timeout
    hb = raw.get("heartbeat_timeout_seconds")
    if hb is None:
        dest["heartbeat_timeout_seconds"] = min(5.0, timeout)
    else:
        dest["heartbeat_timeout_seconds"] = parse_activity_seconds(
            hb, label=f"{label}.heartbeat_timeout_seconds"
        )


class WorkflowsActivitiesMixin:
    def start_workflow_activity(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
    ) -> WorkflowActivity:
        """Stage 1: record the activity start under the live lease."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        now = _utcnow()
        with transaction(self._pool) as conn:
            job = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(job, tenant_id)
            self._assert_active_lease(job, worker_id, lease_token, now)
            step = conn.execute(
                f"""
                SELECT {_STEP_COLUMNS}
                FROM workflow_step_runs
                WHERE job_id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (job_id, tenant_id),
            ).fetchone()
            if step is None:
                raise ValueError(f"job {job_id} is not a workflow step")
            existing = conn.execute(
                f"""
                SELECT {_ACTIVITY_COLUMNS}
                FROM workflow_activities
                WHERE step_run_id = %s
                FOR UPDATE
                """,
                (step["id"],),
            ).fetchone()
            if existing is not None:
                if existing["status"] == "running" and self._activity_is_due(
                    existing, now
                ):
                    self._terminate_activity_locked(
                        conn,
                        activity=existing,
                        job_row=job,
                        error=self._activity_due_error(existing, now),
                        status="timed_out",
                        now=now,
                    )
                    row = conn.execute(
                        f"SELECT {_ACTIVITY_COLUMNS} FROM workflow_activities WHERE id = %s",
                        (existing["id"],),
                    ).fetchone()
                    return self._row_to_workflow_activity(row or existing)
                return self._row_to_workflow_activity(existing)
            self._assert_tenant_mutable(
                conn, tenant_id, action="activity start", error=LeaseError
            )
            spec = self._activity_spec_from_job(job, conn=conn)
            timeout = float(spec["timeout_seconds"])
            hb = float(spec["heartbeat_timeout_seconds"])
            aid = uuid4()
            conn.execute(
                f"""
                INSERT INTO workflow_activities (
                    id, step_run_id, job_id, run_id, tenant_id, name, url,
                    status, timeout_seconds, heartbeat_timeout_seconds,
                    started_at, last_heartbeat_at, heartbeat_due_at,
                    timeout_at, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    'running', %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    aid,
                    step["id"],
                    job_id,
                    step["run_id"],
                    tenant_id,
                    spec["activity"],
                    spec.get("url"),
                    timeout,
                    hb,
                    now,
                    now,
                    now + timedelta(seconds=hb),
                    now + timedelta(seconds=timeout),
                    now,
                    now,
                ),
            )
            if step["status"] == WorkflowStepStatus.PENDING.value:
                conn.execute(
                    """
                    UPDATE workflow_step_runs
                    SET status = %s, started_at = COALESCE(started_at, %s),
                        updated_at = %s
                    WHERE id = %s AND status = %s
                    """,
                    (
                        WorkflowStepStatus.RUNNING.value,
                        now,
                        now,
                        step["id"],
                        WorkflowStepStatus.PENDING.value,
                    ),
                )
            self._align_job_deadline_to_activity(
                conn,
                job_id=job_id,
                timeout_seconds=timeout,
                now=now,
            )
            row = conn.execute(
                f"SELECT {_ACTIVITY_COLUMNS} FROM workflow_activities WHERE id = %s",
                (aid,),
            ).fetchone()
            assert row is not None
            return self._row_to_workflow_activity(row)

    def heartbeat_workflow_activity(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
    ) -> WorkflowActivity:
        """Stage 2: refresh the activity heartbeat under the live lease."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        now = _utcnow()
        with transaction(self._pool) as conn:
            job = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(job, tenant_id)
            self._assert_active_lease(job, worker_id, lease_token, now)
            self._assert_tenant_inflight_ok(
                conn, tenant_id, action="activity heartbeat", error=LeaseError
            )
            row = self._lock_activity_for_job(conn, job_id, tenant_id)
            if row["status"] != "running":
                return self._row_to_workflow_activity(row)
            if self._activity_is_due(row, now):
                self._terminate_activity_locked(
                    conn,
                    activity=row,
                    job_row=job,
                    error=self._activity_due_error(row, now),
                    status="timed_out",
                    now=now,
                )
                raise LeaseError(
                    f"job {job_id} {self._activity_due_error(row, now)}"
                )
            hb = float(row["heartbeat_timeout_seconds"])
            updated = conn.execute(
                f"""
                UPDATE workflow_activities
                SET last_heartbeat_at = %s,
                    heartbeat_due_at = %s,
                    updated_at = %s
                WHERE id = %s AND status = 'running'
                RETURNING {_ACTIVITY_COLUMNS}
                """,
                (now, now + timedelta(seconds=hb), now, row["id"]),
            ).fetchone()
            return self._row_to_workflow_activity(updated or row)

    def complete_workflow_activity(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        result: dict[str, Any] | None = None,
    ) -> WorkflowActivity:
        """Mark a running activity completed. Caller still acks the step job."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        if result is not None and not isinstance(result, dict):
            raise ValueError("result must be an object")
        now = _utcnow()
        with transaction(self._pool) as conn:
            job = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(job, tenant_id)
            self._assert_active_lease(job, worker_id, lease_token, now)
            self._assert_tenant_inflight_ok(
                conn, tenant_id, action="activity complete", error=LeaseError
            )
            row = self._lock_activity_for_job(conn, job_id, tenant_id)
            if row["status"] == "completed":
                return self._row_to_workflow_activity(row)
            if row["status"] != "running":
                raise ValueError(
                    f"activity {row['id']} is {row['status']}, cannot complete"
                )
            if self._activity_is_due(row, now):
                self._terminate_activity_locked(
                    conn,
                    activity=row,
                    job_row=job,
                    error=self._activity_due_error(row, now),
                    status="timed_out",
                    now=now,
                )
                raise LeaseError(
                    f"job {job_id} {self._activity_due_error(row, now)}"
                )
            updated = conn.execute(
                f"""
                UPDATE workflow_activities
                SET status = 'completed',
                    result = %s,
                    last_heartbeat_at = %s,
                    completed_at = %s,
                    updated_at = %s,
                    error = NULL
                WHERE id = %s AND status = 'running'
                RETURNING {_ACTIVITY_COLUMNS}
                """,
                (
                    Jsonb(result) if result is not None else None,
                    now,
                    now,
                    now,
                    row["id"],
                ),
            ).fetchone()
            return self._row_to_workflow_activity(updated or row)

    def fail_workflow_activity(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        error: str,
    ) -> WorkflowActivity:
        """Mark a running activity failed and start reverse compensate."""
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        msg = str(error or "activity failed").strip() or "activity failed"
        now = _utcnow()
        with transaction(self._pool) as conn:
            job = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(job, tenant_id)
            self._assert_active_lease(job, worker_id, lease_token, now)
            self._assert_tenant_inflight_ok(
                conn, tenant_id, action="activity fail", error=LeaseError
            )
            row = self._lock_activity_for_job(conn, job_id, tenant_id)
            if row["status"] != "running":
                return self._row_to_workflow_activity(row)
            self._terminate_activity_locked(
                conn,
                activity=row,
                job_row=job,
                error=msg,
                status="failed",
                now=now,
            )
            done = conn.execute(
                f"SELECT {_ACTIVITY_COLUMNS} FROM workflow_activities WHERE id = %s",
                (row["id"],),
            ).fetchone()
            return self._row_to_workflow_activity(done or row)

    @staticmethod
    def _count_running_activities(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM workflow_activities
            WHERE tenant_id = %s AND status = 'running'
            """,
            (tenant_id,),
        ).fetchone()
        return int(row["n"] if row is not None else 0)

    def get_workflow_activity(
        self, job_id: UUID, *, tenant_id: str
    ) -> WorkflowActivity | None:
        if not tenant_id or not str(tenant_id).strip():
            raise ValueError("tenant_id is required")
        tenant_id = str(tenant_id).strip()
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_ACTIVITY_COLUMNS}
                FROM workflow_activities
                WHERE job_id = %s AND tenant_id = %s
                """,
                (job_id, tenant_id),
            ).fetchone()
        return self._row_to_workflow_activity(row) if row else None

    def process_workflow_activities(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 50,
    ) -> int:
        """Stage 3: fail due activities and start compensate.

        Each row is its own transaction so one bad run cannot roll back
        the rest.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        params: list[Any] = []
        tenant_clause = ""
        if tenant_id is not None:
            tenant_clause = "AND a.tenant_id = %s"
            params.append(str(tenant_id).strip())
        params.append(limit)
        with self._pool.connection() as conn:
            ids = [
                r["id"]
                for r in conn.execute(
                    f"""
                    SELECT a.id
                    FROM workflow_activities a
                    WHERE a.status = 'running'
                      AND (a.heartbeat_due_at <= NOW() OR a.timeout_at <= NOW())
                    {tenant_clause}
                    ORDER BY LEAST(a.heartbeat_due_at, a.timeout_at) ASC
                    LIMIT %s
                    """,
                    tuple(params),
                ).fetchall()
            ]
        fixed = 0
        for aid in ids:
            if fixed >= limit:
                break
            try:
                if self._timeout_one_activity(aid):
                    fixed += 1
            except Exception:  # noqa: BLE001
                continue
        return fixed

    def _timeout_one_activity(self, activity_id: UUID) -> bool:
        now = _utcnow()
        with transaction(self._pool) as conn:
            peek = conn.execute(
                f"""
                SELECT {_ACTIVITY_COLUMNS}
                FROM workflow_activities
                WHERE id = %s
                """,
                (activity_id,),
            ).fetchone()
            if peek is None or peek["status"] != "running":
                return False
            if not self._activity_is_due(peek, now):
                return False
            try:
                self._assert_tenant_inflight_ok(
                    conn,
                    str(peek["tenant_id"]),
                    action="activity timeout",
                    error=ValueError,
                )
            except ValueError:
                return False
            # Lock the step job first so ack (job then activity) cannot deadlock.
            job = None
            if peek.get("job_id") is not None:
                job = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE id = %s
                    FOR UPDATE
                    """,
                    (peek["job_id"],),
                ).fetchone()
            row = conn.execute(
                f"""
                SELECT {_ACTIVITY_COLUMNS}
                FROM workflow_activities
                WHERE id = %s
                FOR UPDATE
                """,
                (activity_id,),
            ).fetchone()
            if row is None or row["status"] != "running":
                return False
            if not self._activity_is_due(row, now):
                return False
            self._terminate_activity_locked(
                conn,
                activity=row,
                job_row=job,
                error=self._activity_due_error(row, now),
                status="timed_out",
                now=now,
            )
            return True

    def _assert_activity_step_ackable(self, conn: Any, job_row: dict[str, Any]) -> None:
        """Refuse a successful ack until the activity row is completed.

        A worker that acks the step job while the activity is missing,
        running, failed, or timed out would advance the saga as if the
        external call succeeded.
        """
        try:
            self._activity_spec_from_job(job_row, conn=conn)
        except ValueError:
            return
        row = conn.execute(
            f"""
            SELECT {_ACTIVITY_COLUMNS}
            FROM workflow_activities
            WHERE job_id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (job_row["id"], job_row["tenant_id"]),
        ).fetchone()
        status = str(row["status"]) if row is not None else "not started"
        if row is None or status != "completed":
            raise LeaseError(
                f"cannot ack activity step {job_row['id']} "
                f"until the activity is completed (status={status})"
            )

    def _close_running_activity_for_step_locked(
        self,
        conn: Any,
        *,
        step_id: Any,
        error: str,
        now: Any,
        status: str = "failed",
    ) -> bool:
        """Fail a still-running activity when its step is skipped/cancelled.

        Does not touch the step job — the caller already cancelled or
        skipped that row. A leftover ``running`` activity would keep
        showing up on the due index after a run deadline.
        """
        if status not in ("failed", "timed_out"):
            status = "failed"
        updated = conn.execute(
            """
            UPDATE workflow_activities
            SET status = %s,
                error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE step_run_id = %s
              AND status = 'running'
            RETURNING id
            """,
            (status, error, now, now, step_id),
        ).fetchone()
        return updated is not None

    def _terminate_activity_locked(
        self,
        conn: Any,
        *,
        activity: dict[str, Any],
        job_row: dict[str, Any] | None,
        error: str,
        status: str,
        now: Any,
    ) -> None:
        conn.execute(
            """
            UPDATE workflow_activities
            SET status = %s,
                error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s AND status = 'running'
            """,
            (status, error, now, now, activity["id"]),
        )
        if job_row is None:
            return
        if job_row["status"] in (
            JobStatus.DEAD.value,
            JobStatus.COMPLETED.value,
            JobStatus.CANCELLED.value,
        ):
            if hasattr(self, "_on_workflow_step_terminal"):
                self._on_workflow_step_terminal(
                    conn,
                    job_row,
                    success=False,
                    error=error,
                    now=now,
                )
            return
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                last_error = %s,
                completed_at = COALESCE(completed_at, %s),
                updated_at = %s
            WHERE id = %s
              AND status IN (%s, %s)
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.DEAD.value,
                error,
                now,
                now,
                job_row["id"],
                JobStatus.LEASED.value,
                JobStatus.PENDING.value,
            ),
        ).fetchone()
        dead = updated
        if dead is None:
            dead = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (job_row["id"],),
            ).fetchone()
        if dead is None:
            return
        if updated is not None:
            self._append_job_audit(
                conn,
                job_row=dead,
                event_type="dead_lettered",
                from_status=job_row["status"],
                actor="system",
                detail={"reason": error},
                now=now,
            )
            self._record_metric(
                conn,
                tenant_id=str(dead["tenant_id"]),
                metric="dead",
                count=1,
                now=now,
            )
            if hasattr(self, "_note_tenant_failure"):
                self._note_tenant_failure(
                    conn, str(dead["tenant_id"]), now=now
                )
            if hasattr(self, "_record_lease_end_usage") and job_row[
                "status"
            ] == JobStatus.LEASED.value:
                self._record_lease_end_usage(conn, job_row, now=now)
            if hasattr(self, "_enqueue_job_webhooks"):
                self._enqueue_job_webhooks(
                    conn,
                    job=self._job(dead),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
        if hasattr(self, "_on_workflow_step_terminal"):
            self._on_workflow_step_terminal(
                conn,
                dead,
                success=False,
                error=error,
                now=now,
            )

    def _align_job_deadline_to_activity(
        self,
        conn: Any,
        *,
        job_id: UUID,
        timeout_seconds: float,
        now: Any,
    ) -> None:
        """Make the job hard deadline start at activity start, not lease.

        ``max_runtime`` is measured from ``lease_started_at``. A worker that
        is slow between lease and start would otherwise dead-letter before
        the activity timeout clock begins. After start, keep a small slack
        past ``timeout_at`` as a backstop.
        """
        import math

        row = conn.execute(
            """
            SELECT lease_started_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return
        slack = 2.0
        started = row.get("lease_started_at")
        elapsed = 0.0
        if started is not None:
            elapsed = max(0.0, (_as_utc(now) - _as_utc(started)).total_seconds())
        max_rt = max(1, int(math.ceil(elapsed + float(timeout_seconds) + slack)))
        conn.execute(
            """
            UPDATE jobs
            SET max_runtime_seconds = %s,
                updated_at = %s
            WHERE id = %s
              AND status = %s
            """,
            (max_rt, now, job_id, JobStatus.LEASED.value),
        )

    def _lock_activity_for_job(
        self, conn: Any, job_id: UUID, tenant_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            f"""
            SELECT {_ACTIVITY_COLUMNS}
            FROM workflow_activities
            WHERE job_id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (job_id, tenant_id),
        ).fetchone()
        if row is None:
            raise ValueError(f"no activity for job {job_id}")
        return row

    @staticmethod
    def _activity_is_due(row: dict[str, Any], now: Any) -> bool:
        due = row.get("heartbeat_due_at")
        timeout = row.get("timeout_at")
        if due is not None and _as_utc(due) <= _as_utc(now):
            return True
        if timeout is not None and _as_utc(timeout) <= _as_utc(now):
            return True
        return False

    @staticmethod
    def _activity_due_error(row: dict[str, Any], now: Any) -> str:
        timeout = row.get("timeout_at")
        if timeout is not None and _as_utc(timeout) <= _as_utc(now):
            return "activity timeout exceeded"
        return "activity heartbeat timeout"

    def _activity_spec_from_job(
        self, job_row: dict[str, Any], *, conn: Any
    ) -> dict[str, Any]:
        payload = self._open_job_payload(
            job_row.get("payload"),
            tenant_id=str(job_row["tenant_id"]),
            job_id=job_row["id"],
            conn=conn,
        )
        if payload.get("kind") != WORKFLOW_STEP_KIND:
            raise ValueError("job is not a workflow step")
        step = payload.get("step") if isinstance(payload.get("step"), dict) else {}
        kind = str(step.get("kind") or "")
        if kind != "activity":
            raise ValueError("workflow step is not an activity")
        out: dict[str, Any] = {}
        apply_activity_fields(out, step, label="activity")
        return out

    @staticmethod
    def _row_to_workflow_activity(row: dict[str, Any]) -> WorkflowActivity:
        result = row.get("result")
        if isinstance(result, str):
            import json

            result = json.loads(result)
        return WorkflowActivity(
            id=row["id"],
            step_run_id=row["step_run_id"],
            job_id=row.get("job_id"),
            run_id=row["run_id"],
            tenant_id=str(row["tenant_id"]),
            name=str(row["name"]),
            url=row.get("url"),
            status=str(row["status"]),
            timeout_seconds=float(row["timeout_seconds"]),
            heartbeat_timeout_seconds=float(row["heartbeat_timeout_seconds"]),
            started_at=row["started_at"],
            last_heartbeat_at=row["last_heartbeat_at"],
            heartbeat_due_at=row["heartbeat_due_at"],
            timeout_at=row["timeout_at"],
            completed_at=row.get("completed_at"),
            result=result if isinstance(result, dict) else None,
            error=row.get("error"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
