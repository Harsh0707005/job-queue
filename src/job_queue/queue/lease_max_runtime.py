"""JobQueue max-runtime release and exhausted-lease dead-letter."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from job_queue.models import (
    JobStatus,
    WebhookEvent,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _as_utc,
    _row_to_job,
    compute_retry_backoff_seconds,
)


class LeaseMaxRuntimeMixin:
    def _hard_deadline(self, row: dict[str, Any]) -> datetime | None:
        started = row.get("lease_started_at")
        if started is None:
            return None
        max_rt = row.get("max_runtime_seconds")
        if max_rt is None:
            max_rt = 3600
        return _as_utc(started) + timedelta(seconds=int(max_rt))

    def _release_max_runtime_exceeded(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        """Force-release a stuck lease that exceeded max_runtime_seconds."""
        attempts = int(row["attempts"])
        max_attempts = int(row["max_attempts"])
        error = "max runtime exceeded"
        tenant_id = row["tenant_id"]
        # Offboarding tenants must not regain pending work after a force-release.
        force_terminal = self._is_tenant_offboarding(conn, tenant_id)
        if attempts >= max_attempts or force_terminal:
            terminal = (
                JobStatus.CANCELLED.value
                if force_terminal and attempts < max_attempts
                else JobStatus.DEAD.value
            )
            if force_terminal and attempts < max_attempts:
                error = "max runtime exceeded during tenant offboard"
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
                    completed_at = CASE WHEN %s = %s THEN %s ELSE completed_at END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    terminal,
                    error,
                    terminal,
                    JobStatus.DEAD.value,
                    now,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type=(
                            "cancelled"
                            if terminal == JobStatus.CANCELLED.value
                            else "dead_lettered"
                        ),
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
                    )
                # System terminal paths must bump counters (not only worker nack).
                if terminal == JobStatus.DEAD.value:
                    self._record_metric(
                        conn,
                        tenant_id=str(tenant_id),
                        metric="dead",
                        count=1,
                        now=now,
                    )
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tenant_id,
                reason=f"parent job {row['id']} ended as {terminal} (max runtime)",
                now=now,
            )
            dead_row = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (row["id"],),
            ).fetchone()
            if dead_row is not None and terminal == JobStatus.DEAD.value:
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(dead_row),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
            # System terminal (max runtime) must drive saga compensation too.
            if dead_row is not None and terminal in (
                JobStatus.DEAD.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ):
                if hasattr(self, "_on_workflow_step_terminal"):
                    self._on_workflow_step_terminal(
                        conn,
                        dead_row,
                        success=False,
                        error=error,
                        now=now,
                    )
            self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
        else:
            base = row.get("retry_backoff_base_seconds")
            cap = row.get("retry_backoff_max_seconds")
            backoff = compute_retry_backoff_seconds(
                attempts,
                base_seconds=1 if base is None else int(base),
                max_seconds=300 if cap is None else int(cap),
            )
            next_available = now + timedelta(seconds=backoff)
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
                    available_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    JobStatus.PENDING.value,
                    error,
                    next_available,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type="requeued",
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
                    )
                # System force-release is operationally a nack/requeue.
                self._record_metric(
                    conn,
                    tenant_id=str(tenant_id),
                    metric="nack",
                    count=1,
                    now=now,
                )

    def _dead_letter_exhausted_leases(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None,
    ) -> None:
        """Mark expired leases that exhausted attempts as dead; cascade children."""
        if tenant_id is not None:
            dead_rows = conn.execute(
                """
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
                    completed_at = COALESCE(completed_at, %s),
                    updated_at = %s
                WHERE tenant_id = %s
                  AND status = %s
                  AND lease_expires_at < %s
                  AND attempts >= max_attempts
                RETURNING id, tenant_id
                """,
                (
                    JobStatus.DEAD.value,
                    now,
                    now,
                    tenant_id,
                    JobStatus.LEASED.value,
                    now,
                ),
            ).fetchall()
        else:
            dead_rows = conn.execute(
                """
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
                    completed_at = COALESCE(completed_at, %s),
                    updated_at = %s
                WHERE status = %s
                  AND lease_expires_at < %s
                  AND attempts >= max_attempts
                RETURNING id, tenant_id
                """,
                (
                    JobStatus.DEAD.value,
                    now,
                    now,
                    JobStatus.LEASED.value,
                    now,
                ),
            ).fetchall()

        tenants: set[str] = set()
        for dead in dead_rows:
            tid = dead["tenant_id"]
            tenants.add(tid)
            full = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (dead["id"],),
            ).fetchone()
            if full is not None:
                self._append_job_audit(
                    conn,
                    job_row=full,
                    event_type="dead_lettered",
                    from_status=JobStatus.LEASED.value,
                    actor="system",
                    detail={"reason": "exceeded max_attempts after lease expiry"},
                    now=now,
                )
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(full),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
                # Lease-expiry dead must reverse-compensate completed saga steps.
                if hasattr(self, "_on_workflow_step_terminal"):
                    self._on_workflow_step_terminal(
                        conn,
                        full,
                        success=False,
                        error=str(
                            full.get("last_error")
                            or "exceeded max_attempts after lease expiry"
                        ),
                        now=now,
                    )
            else:
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
            # Lease-expiry dead-letter (same txn as status flip).
            self._record_metric(
                conn,
                tenant_id=str(tid),
                metric="dead",
                count=1,
                now=now,
            )
        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)
