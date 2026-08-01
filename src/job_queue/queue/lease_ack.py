"""JobQueue ack/release."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

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
)


class LeaseAckMixin:
    def ack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        result: Any = None,
    ) -> Job:
        """Mark a leased job completed. Only the current lease holder may ack.

        Optional *result* is stored as JSON on the job and can be read later
        via ``get`` / ``get_result``. *tenant_id* must match the current owner
        or ``lease_tenant_id`` (pre-merge claim tenant).
        """
        now = _utcnow()
        result_json = Jsonb(result) if result is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_lease_holder(row, worker_id, lease_token)
            hard = self._hard_deadline(row)
            if hard is not None and now >= hard:
                self._release_max_runtime_exceeded(conn, row, now=now)
                runtime_exceeded = True
            else:
                runtime_exceeded = False
                self._assert_lease_not_expired(row, now)

                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        completed_at = %s,
                        result = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.COMPLETED.value,
                        now,
                        result_json,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise LeaseError(f"job {job_id} lease lost during ack")
                job = _row_to_job(updated)
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="completed",
                    from_status=JobStatus.LEASED.value,
                    actor=worker_id,
                    detail={"has_result": result is not None},
                    now=now,
                )
                self._record_job_duration(conn, job=job, now=now)
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_COMPLETED, now=now
                )
                # Wake parked parent if this was a spawned child.
                if updated.get("parent_job_id") is not None:
                    self._maybe_wake_parent_on_join_locked(
                        conn,
                        parent_id=updated["parent_job_id"],
                        tenant_id=updated["tenant_id"],
                        now=now,
                    )
                self._maybe_finalize_offboard(conn, tenant_id=job.tenant_id, now=now)
                return job

        if runtime_exceeded:
            raise LeaseError(f"job {job_id} max runtime exceeded; refusing ack")
        raise LeaseError(f"job {job_id} lease lost during ack")

    def release(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        reason: str | None = None,
    ) -> Job:
        """Return a leased job to ``pending`` immediately (no failure count).

        Clears the lease so another worker can claim without waiting for
        visibility timeout. Does **not** change ``attempts`` (the claim already
        counted). ``available_at`` is set to now. Intended for graceful drain
        on worker shutdown/deploy.
        """
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)
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
                    last_error = COALESCE(%s, last_error),
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.PENDING.value,
                    now,
                    reason,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during release")
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="released",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"reason": reason or "drain"},
                now=now,
            )
            self._maybe_finalize_offboard(conn, tenant_id=updated["tenant_id"], now=now)
            return _row_to_job(updated)
