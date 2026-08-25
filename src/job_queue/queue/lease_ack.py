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

                result_json = None
                if result is not None:
                    from job_queue.crypto import result_aad

                    old_result_blob = (
                        self._blob_id_of(row.get("result"))
                        if hasattr(self, "_blob_id_of")
                        else None
                    )
                    sealed_result, _kid = self._seal_json_value(
                        conn,
                        tenant_id=str(row["tenant_id"]),
                        value=result,
                        aad=result_aad(job_id),
                        job_id=job_id,
                        blob_kind="result",
                    )
                    if hasattr(self, "_orphan_blob"):
                        self._orphan_blob(conn, old_result_blob)
                    result_json = Jsonb(sealed_result)

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
                job = self._job(updated)
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="completed",
                    from_status=JobStatus.LEASED.value,
                    actor=worker_id,
                    detail={"has_result": result is not None},
                    now=now,
                )
                self._record_metric(
                    conn,
                    tenant_id=str(updated["tenant_id"]),
                    metric="ack",
                    count=1,
                    now=now,
                )
                if hasattr(self, "_record_lease_end_usage"):
                    self._record_lease_end_usage(conn, row, now=now)
                self._record_job_duration(conn, job=job, now=now)
                if hasattr(self, "_record_last_success_worker"):
                    enabled = bool(updated.get("prefer_last_worker"))
                    if not enabled:
                        qs = conn.execute(
                            """
                            SELECT prefer_last_worker_queues
                            FROM tenant_settings
                            WHERE tenant_id = %s
                            """,
                            (updated["tenant_id"],),
                        ).fetchone()
                        opted = list(
                            (qs or {}).get("prefer_last_worker_queues") or []
                        )
                        enabled = str(updated["queue_name"]) in opted
                    if enabled:
                        self._record_last_success_worker(
                            conn,
                            tenant_id=str(updated["tenant_id"]),
                            queue_name=str(updated["queue_name"]),
                            kind=str(updated.get("payload_kind") or "")
                            or self._payload_kind(job.payload),
                            worker_id=worker_id,
                            now=now,
                        )
                # Advance the saga before sealing the schedule generation so
                # the next step exists (or the run is terminal) first.
                if hasattr(self, "_on_workflow_step_terminal"):
                    self._on_workflow_step_terminal(
                        conn,
                        updated,
                        success=True,
                        result=result,
                        now=now,
                    )
                if hasattr(self, "_maybe_advance_schedule_completed"):
                    self._maybe_advance_schedule_completed(
                        conn,
                        schedule_id=updated.get("schedule_id"),
                        tenant_id=str(updated["tenant_id"]),
                    )
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
            if hasattr(self, "_record_lease_end_usage"):
                self._record_lease_end_usage(conn, row, now=now, releases=1)
            self._maybe_finalize_offboard(conn, tenant_id=updated["tenant_id"], now=now)
            return self._job(updated)
