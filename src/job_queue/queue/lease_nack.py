"""JobQueue nack."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
    WebhookEvent,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    LeaseError,
    _row_to_job,
    _utcnow,
    compute_retry_backoff_seconds,
)


class LeaseNackMixin:
    def nack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        error: str | None = None,
        requeue: bool = True,
    ) -> Job:
        """Release a lease after a processing failure.

        If *requeue* is True and attempts < max_attempts, the job returns to
        ``pending`` with ``available_at`` pushed forward by exponential
        backoff (base * 2^(attempts-1), capped by the job's max backoff) so
        it is not immediately leaseable again. When attempts are exhausted
        (or requeue is False), the job is ``dead`` / ``failed`` and dependents
        are cascade-cancelled. *tenant_id* must match the current owner or
        ``lease_tenant_id`` (pre-merge claim tenant).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            attempts = row["attempts"]
            max_attempts = row["max_attempts"]
            owner_tenant = row["tenant_id"]

            if requeue and attempts < max_attempts:
                new_status = JobStatus.PENDING.value
                base = row.get("retry_backoff_base_seconds")
                cap = row.get("retry_backoff_max_seconds")
                backoff = compute_retry_backoff_seconds(
                    attempts,
                    base_seconds=1 if base is None else int(base),
                    max_seconds=300 if cap is None else int(cap),
                )
                next_available = now + timedelta(seconds=backoff)
            elif attempts >= max_attempts:
                new_status = JobStatus.DEAD.value
                next_available = row["available_at"]
            else:
                new_status = JobStatus.FAILED.value
                next_available = row["available_at"]

            # Stamp completed_at for terminal outcomes so retention can age them.
            terminal_at = (
                now
                if new_status
                in (
                    JobStatus.DEAD.value,
                    JobStatus.FAILED.value,
                )
                else None
            )
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    available_at = %s,
                    last_error = %s,
                    completed_at = COALESCE(%s, completed_at),
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    new_status,
                    next_available,
                    error,
                    terminal_at,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during nack")

            event_type = (
                "requeued"
                if new_status == JobStatus.PENDING.value
                else (
                    "dead_lettered" if new_status == JobStatus.DEAD.value else "failed"
                )
            )
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type=event_type,
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"error": error, "requeue": requeue},
                now=now,
            )

            if new_status in (JobStatus.DEAD.value, JobStatus.FAILED.value):
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=job_id,
                    tenant_id=owner_tenant,
                    reason=(
                        f"parent job {job_id} ended as {new_status}"
                        + (f": {error}" if error else "")
                    ),
                    now=now,
                )
            job = _row_to_job(updated)
            if new_status == JobStatus.DEAD.value:
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_DEAD, now=now
                )
            elif new_status == JobStatus.FAILED.value:
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_FAILED, now=now
                )
            # Child terminal fail must wake/fail a parked parent join.
            if updated.get("parent_job_id") is not None and new_status in (
                JobStatus.DEAD.value,
                JobStatus.FAILED.value,
            ):
                self._maybe_wake_parent_on_join_locked(
                    conn,
                    parent_id=updated["parent_job_id"],
                    tenant_id=owner_tenant,
                    now=now,
                )
            # Offboarding: requeued pending is purged when no leases remain;
            # terminal nacks also trigger purge when this was the last holder.
            self._maybe_finalize_offboard(conn, tenant_id=job.tenant_id, now=now)
            return job
