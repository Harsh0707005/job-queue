"""JobQueue single-job cancel."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _CANCELABLE_STATUSES,
    _JOB_COLUMNS,
    CancelError,
    _utcnow,
)


class JobsCancelOneMixin:
    def cancel(self, job_id: UUID, *, tenant_id: str) -> Job:
        """Cancel a *pending* or *leased* (running) job by id for *tenant_id*.

        **Pending:** marked ``cancelled`` and will never be leased.

        **Leased (running):** marked ``cancelled`` and the fencing
        ``lease_token`` / lease holder fields are cleared. The worker that held
        the job will fail heartbeat / ack / nack with ``LeaseError`` and cannot
        complete it. Cooperative handlers should watch ``lease_lost`` /
        ``ensure_held()`` and exit early.

        Terminal states (completed / failed / dead / already cancelled) raise
        ``CancelError``. Cross-tenant ids raise ``CancelError`` as not found.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")

        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (job_id, tenant_id),
            ).fetchone()

            if row is None:
                raise CancelError(f"job {job_id} not found for tenant {tenant_id!r}")

            job = self._job(row)
            if job.status not in (
                JobStatus.PENDING,
                JobStatus.LEASED,
                JobStatus.AWAITING_APPROVAL,
                JobStatus.AWAITING_CHILDREN,
                JobStatus.AWAITING_SKILLS,
            ):
                raise CancelError(
                    f"job {job_id} cannot be cancelled "
                    f"(status={job.status.value}; only pending, leased, "
                    f"awaiting_approval, awaiting_children, or "
                    f"awaiting_skills jobs cancel)",
                    job=job,
                )

            was_running = job.status == JobStatus.LEASED
            was_awaiting_children = job.status == JobStatus.AWAITING_CHILDREN
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    checkpoint = NULL,
                    checkpointed_at = NULL,
                    approval_deadline_at = NULL,
                    approval_outcome = COALESCE(approval_outcome, 'rejected'),
                    approval_resolved_at = COALESCE(approval_resolved_at, %s),
                    await_children_deadline_at = NULL,
                    await_children_outcome = CASE
                        WHEN status = %s THEN COALESCE(await_children_outcome, 'cancelled')
                        ELSE await_children_outcome
                    END,
                    await_children_resolved_at = CASE
                        WHEN status = %s THEN COALESCE(await_children_resolved_at, %s)
                        ELSE await_children_resolved_at
                    END,
                    last_error = CASE
                        WHEN %s THEN 'cancelled while running'
                        WHEN status = %s THEN COALESCE(last_error, 'cancelled while awaiting approval')
                        WHEN status = %s THEN COALESCE(last_error, 'cancelled while awaiting children')
                        ELSE last_error
                    END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status IN (%s, %s, %s, %s)
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    JobStatus.AWAITING_CHILDREN.value,
                    JobStatus.AWAITING_CHILDREN.value,
                    now,
                    was_running,
                    JobStatus.AWAITING_APPROVAL.value,
                    JobStatus.AWAITING_CHILDREN.value,
                    now,
                    job_id,
                    tenant_id,
                    JobStatus.PENDING.value,
                    JobStatus.LEASED.value,
                    JobStatus.AWAITING_APPROVAL.value,
                    JobStatus.AWAITING_CHILDREN.value,
                ),
            ).fetchone()
            if updated is None:
                # Race: terminalized between SELECT and UPDATE.
                current = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
                    (job_id, tenant_id),
                ).fetchone()
                current_job = self._job(current) if current else None
                raise CancelError(
                    f"job {job_id} cannot be cancelled (status changed during cancel)",
                    job=current_job,
                )
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="cancelled",
                from_status=job.status.value,
                actor=None,
                detail={
                    "was_running": was_running,
                    "was_awaiting_children": was_awaiting_children,
                },
                now=now,
            )
            self._cascade_cancel_dependents(
                conn,
                parent_id=job_id,
                tenant_id=tenant_id,
                reason=f"parent job {job_id} cancelled",
                now=now,
            )
            # If this cancelled job was itself a child, wake/fail its parent join.
            if updated.get("parent_job_id") is not None:
                self._maybe_wake_parent_on_join_locked(
                    conn,
                    parent_id=updated["parent_job_id"],
                    tenant_id=tenant_id,
                    now=now,
                )
            self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
            return self._job(updated)

    def _cancel_locked_row(
        self,
        conn: Any,
        *,
        row: dict[str, Any],
        tenant_id: str,
        now: datetime,
        reason: str,
    ) -> Job | None:
        """Cancel one already-locked cancelable job row. Returns updated job."""
        status = row["status"]
        if status not in {s.value for s in _CANCELABLE_STATUSES}:
            return None
        job_id = row["id"]
        was_running = status == JobStatus.LEASED.value
        was_awaiting_children = status == JobStatus.AWAITING_CHILDREN.value
        err = reason
        if was_running:
            err = f"{reason} (stopped while running)"
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                checkpoint = NULL,
                checkpointed_at = NULL,
                approval_deadline_at = NULL,
                approval_outcome = COALESCE(approval_outcome, 'rejected'),
                approval_resolved_at = COALESCE(approval_resolved_at, %s),
                await_children_deadline_at = NULL,
                await_children_outcome = CASE
                    WHEN status = %s THEN COALESCE(await_children_outcome, 'cancelled')
                    ELSE await_children_outcome
                END,
                await_children_resolved_at = CASE
                    WHEN status = %s THEN COALESCE(await_children_resolved_at, %s)
                    ELSE await_children_resolved_at
                END,
                last_error = %s,
                updated_at = %s
            WHERE id = %s
              AND tenant_id = %s
              AND status IN (%s, %s, %s, %s)
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.CANCELLED.value,
                now,
                JobStatus.AWAITING_CHILDREN.value,
                JobStatus.AWAITING_CHILDREN.value,
                now,
                err,
                now,
                job_id,
                tenant_id,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
                JobStatus.AWAITING_APPROVAL.value,
                JobStatus.AWAITING_CHILDREN.value,
            ),
        ).fetchone()
        if updated is None:
            return None
        self._append_job_audit(
            conn,
            job_row=updated,
            event_type="cancelled",
            from_status=status,
            actor=None,
            detail={
                "was_running": was_running,
                "was_awaiting_children": was_awaiting_children,
                "bulk": True,
                "reason": reason,
            },
            now=now,
        )
        self._cascade_cancel_dependents(
            conn,
            parent_id=job_id,
            tenant_id=tenant_id,
            reason=f"parent job {job_id} cancelled",
            now=now,
        )
        if updated.get("parent_job_id") is not None:
            self._maybe_wake_parent_on_join_locked(
                conn,
                parent_id=updated["parent_job_id"],
                tenant_id=tenant_id,
                now=now,
            )
        return self._job(updated)
