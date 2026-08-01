"""JobQueue lease ops domain (LeaseOpsMixin): heartbeat, ack, nack, release, checkpoint."""
from __future__ import annotations

import json

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import init_schema, transaction
from job_queue.models import (
    AuditConsistencyReport,
    BulkCancelResult,
    Job,
    JobAuditEvent,
    JobProgressEvent,
    JobRetentionSettings,
    JobSideEffect,
    JobStatus,
    PreemptResult,
    RetentionPurgeResult,
    Schedule,
    ScheduleFire,
    SpawnChildrenResult,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)
from job_queue.queue._common import (
    JOB_EVENTS_CHANNEL,
    _ACTIVE_JOB_STATUSES,
    _CANCELABLE_STATUSES,
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _DEDUPE_LOCK_CLASS,
    _DEFAULT_ESTIMATE_SECONDS,
    _DEPS_SATISFIED_SQL,
    _EWMA_ALPHA,
    _JOB_COLUMNS,
    _MAX_LABELS,
    _MAX_LABEL_LEN,
    _MAX_PREEMPTS_PER_JOB,
    _NOTIFY_MAX_BYTES,
    _SCHEDULE_COLUMNS,
    _SIDE_EFFECT_SELECT,
    _TENANT_RETENTION_FOREVER,
    _UNSET,
    _WEBHOOK_DELIVERY_COLUMNS,
    _WEBHOOK_ENDPOINT_COLUMNS,
    ApprovalError,
    ApprovalRequested,
    CancelError,
    ChildrenAwaited,
    DependencyError,
    LeaseError,
    MergeError,
    RateLimitError,
    RebuildError,
    ReplayError,
    ScheduleError,
    SpawnError,
    _SkipLeaseCandidate,
    _as_utc,
    _normalize_labels,
    _normalize_status_filter,
    _row_to_job,
    _row_to_schedule,
    _utcnow,
    compute_retry_backoff_seconds,
)


class LeaseOpsMixin:
    def heartbeat(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        extend_by_seconds: int | None = None,
    ) -> Job:
        """Extend the lease held by *worker_id* under *lease_token*.

        Requires matching worker, fencing token, and tenant. Tenant matches
        when *tenant_id* equals the job's current owner **or**
        ``lease_tenant_id`` (frozen at claim; allows mid-flight merge). An
        unrelated tenant is rejected even with a valid token. Raises
        ``LeaseError`` if the lease was reclaimed, expired, completed, or the
        hard ``max_runtime_seconds`` deadline has been exceeded (the job is
        force-released so a stuck worker cannot heartbeat forever).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_lease_holder(row, worker_id, lease_token)
            hard = self._hard_deadline(row)
            if hard is not None and now >= hard:
                # Commit force-release, then signal the worker (do not raise
                # inside the transaction or the release would roll back).
                self._release_max_runtime_exceeded(conn, row, now=now)
                runtime_exceeded = True
            else:
                runtime_exceeded = False
                self._assert_lease_not_expired(row, now)

                extend = extend_by_seconds or row["visibility_timeout_seconds"]
                new_expiry = now + timedelta(seconds=extend)
                if hard is not None and new_expiry > hard:
                    new_expiry = hard

                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET lease_expires_at = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s
                      AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        new_expiry,
                        now,
                        job_id,
                        lease_token,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
                if updated is None:
                    raise LeaseError(f"job {job_id} lease lost during heartbeat")
                return _row_to_job(updated)

        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded "
                f"(limit={row.get('max_runtime_seconds')}s)"
            )
        raise LeaseError(f"job {job_id} lease lost during heartbeat")

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
                self._maybe_finalize_offboard(
                    conn, tenant_id=job.tenant_id, now=now
                )
                return job

        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded; refusing ack"
            )
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
            self._maybe_finalize_offboard(
                conn, tenant_id=updated["tenant_id"], now=now
            )
            return _row_to_job(updated)

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
                    "dead_lettered"
                    if new_status == JobStatus.DEAD.value
                    else "failed"
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
            if (
                updated.get("parent_job_id") is not None
                and new_status
                in (
                    JobStatus.DEAD.value,
                    JobStatus.FAILED.value,
                )
            ):
                self._maybe_wake_parent_on_join_locked(
                    conn,
                    parent_id=updated["parent_job_id"],
                    tenant_id=owner_tenant,
                    now=now,
                )
            # Offboarding: requeued pending is purged when no leases remain;
            # terminal nacks also trigger purge when this was the last holder.
            self._maybe_finalize_offboard(
                conn, tenant_id=job.tenant_id, now=now
            )
            return job

    def ensure_lease(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
    ) -> Job:
        """Return the job if this worker still holds a live lease; else raise.

        After ``merge_tenant``, *tenant_id* may be the pre-merge claim tenant
        (``lease_tenant_id``); an unrelated tenant is still rejected.

        If ``max_runtime_seconds`` has elapsed, force-releases the lease (same
        as :meth:`heartbeat`) so cooperative handlers cannot keep a stuck
        lease alive by only calling ``ensure_held`` without heartbeating.
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
                return _row_to_job(row)
        if runtime_exceeded:
            raise LeaseError(
                f"job {job_id} max runtime exceeded "
                f"(limit={row.get('max_runtime_seconds')}s)"
            )
        raise LeaseError(f"job {job_id} lease lost during ensure_lease")

    def checkpoint(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        progress: Any,
    ) -> Job:
        """Persist progress JSON for a leased job (survives reclaim).

        Only the current lease holder may write. *progress* is stored as JSON
        on the job (``checkpoint`` / ``checkpointed_at``) and returned on
        subsequent ``lease`` / ``get`` so a replacement worker can resume.
        Does not clear on nack requeue or lease expiry; cancel clears it.
        """
        now = _utcnow()
        progress_json = Jsonb(progress) if progress is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
            self._assert_lease_tenant(row, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET checkpoint = %s,
                    checkpointed_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    progress_json,
                    now,
                    now,
                    job_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during checkpoint")
            job = _row_to_job(updated)
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="checkpointed",
                from_status=JobStatus.LEASED.value,
                actor=worker_id,
                detail={"checkpoint": progress},
                now=now,
            )
            return job

    @staticmethod
    def _assert_lease_holder(
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
    ) -> None:
        if row["status"] != JobStatus.LEASED.value:
            raise LeaseError(
                f"job {row['id']} is not leased (status={row['status']})"
            )
        if row["leased_by"] != worker_id:
            raise LeaseError(
                f"job {row['id']} is leased by {row['leased_by']!r}, "
                f"not {worker_id!r}"
            )
        if row["lease_token"] != lease_token:
            raise LeaseError(
                f"job {row['id']} lease_token mismatch "
                f"(fence invalidated; lease was reclaimed)"
            )

    @staticmethod
    def _assert_lease_not_expired(row: dict[str, Any], now: datetime) -> None:
        expires = row["lease_expires_at"]
        if expires is not None and expires < now:
            raise LeaseError(f"job {row['id']} lease has expired")

    @classmethod
    def _assert_active_lease(
        cls,
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
        now: datetime,
    ) -> None:
        cls._assert_lease_holder(row, worker_id, lease_token)
        cls._assert_lease_not_expired(row, now)
