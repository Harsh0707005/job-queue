"""JobQueue jobs domain (JobsMixin)."""
from __future__ import annotations

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

class JobsMixin:
    def enqueue(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        queue_name: str = "default",
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        max_runtime_seconds: int = 3600,
        retry_backoff_base_seconds: int = 1,
        retry_backoff_max_seconds: int = 300,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        dedupe_key: str | None = None,
        depends_on: UUID | Sequence[UUID] | None = None,
        job_id: UUID | None = None,
        deadline_at: datetime | None = None,
        estimated_duration_seconds: int | None = None,
        preemptible: bool = True,
        labels: Sequence[str] | str | None = None,
    ) -> Job:
        """Add a job to the queue (or return an existing active dedupe match).

        Parameters
        ----------
        available_at:
            Earliest time the job may be claimed. Defaults to now (immediate).
        delay_seconds:
            Convenience alternative to ``available_at``: run after this many
            seconds from now. Mutually exclusive with ``available_at``.
        dedupe_key:
            If set, at most one *active* (pending or leased) job with this key
            may exist for the tenant/queue. A second enqueue returns the
            existing job instead of creating a duplicate. Completed / dead /
            failed jobs do not block a later enqueue with the same key.
        depends_on:
            Parent job id or list of parent ids (same tenant). The job is not
            claimable until every parent is ``completed``. If a parent is
            already failed/dead/cancelled at enqueue time, raises
            ``DependencyError``. If a parent later fails, pending children are
            cascade-cancelled.
        max_runtime_seconds:
            Hard wall-clock limit for one lease attempt (from
            ``lease_started_at``). Heartbeats cannot extend past this; on
            exceed the job is force-released (requeued or dead-lettered).
        retry_backoff_base_seconds / retry_backoff_max_seconds:
            On nack requeue, delay is ``base * 2^(attempt-1)`` seconds, capped
            by max. After ``max_attempts`` failures the job is dead-lettered.
        deadline_at:
            Absolute deadline. When set, the job may preempt less-urgent
            work if concurrency is full and waiting would miss the deadline.
        estimated_duration_seconds:
            Hint for remaining-work / slack calculations. When omitted the
            queue uses learned EWMAs (by payload kind) or a default.
        preemptible:
            If False this job will not be chosen as a preemption victim.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if max_runtime_seconds < 1:
            raise ValueError("max_runtime_seconds must be >= 1")
        if retry_backoff_base_seconds < 0:
            raise ValueError("retry_backoff_base_seconds must be >= 0")
        if retry_backoff_max_seconds < 0:
            raise ValueError("retry_backoff_max_seconds must be >= 0")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if dedupe_key is not None and dedupe_key == "":
            raise ValueError("dedupe_key must be non-empty when provided")
        if estimated_duration_seconds is not None and estimated_duration_seconds < 1:
            raise ValueError("estimated_duration_seconds must be >= 1 when set")

        dep_ids = self._normalize_depends_on(depends_on)
        label_list = _normalize_labels(labels)

        now = _utcnow()
        if available_at is not None:
            run_at = _as_utc(available_at)
        elif delay_seconds is not None:
            run_at = now + timedelta(seconds=float(delay_seconds))
        else:
            run_at = now
        deadline = _as_utc(deadline_at) if deadline_at is not None else None

        jid = job_id or uuid4()
        if jid in dep_ids:
            raise DependencyError("a job cannot depend on itself")

        with transaction(self._pool) as conn:
            # Serialize with merge_tenant / pending-cap for this tenant.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; enqueue is rejected"
                )
            if dep_ids:
                self._validate_dependencies(conn, tenant_id=tenant_id, depends_on=dep_ids)

            if dedupe_key is not None:
                # Serialize with replay_dead_letter on the same key so a
                # check-then-activate race cannot create two active rows or
                # surface a raw unique_violation.
                self._lock_dedupe_key(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    dedupe_key=dedupe_key,
                )
                existing = self._find_active_dedupe(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=queue_name,
                    dedupe_key=dedupe_key,
                )
                if existing is not None:
                    return _row_to_job(existing)

                # New pending row: enforce per-tenant pending cap + rate.
                self._assert_pending_capacity(conn, tenant_id)
                self._consume_enqueue_rate(conn, tenant_id, now=now)

                # ON CONFLICT remains a safety net for any missed paths.
                row = conn.execute(
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
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, NULL, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (tenant_id, queue_name, dedupe_key)
                        WHERE dedupe_key IS NOT NULL
                          AND status IN (
                              'pending', 'leased',
                              'awaiting_approval', 'awaiting_children'
                          )
                    DO NOTHING
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        jid,
                        tenant_id,
                        queue_name,
                        Jsonb(payload),
                        JobStatus.PENDING.value,
                        priority,
                        max_attempts,
                        visibility_timeout_seconds,
                        max_runtime_seconds,
                        retry_backoff_base_seconds,
                        retry_backoff_max_seconds,
                        run_at,
                        dedupe_key,
                        dep_ids,
                        label_list,
                        deadline,
                        estimated_duration_seconds,
                        bool(preemptible),
                        now,
                        now,
                    ),
                ).fetchone()

                if row is None:
                    existing = self._find_active_dedupe(
                        conn,
                        tenant_id=tenant_id,
                        queue_name=queue_name,
                        dedupe_key=dedupe_key,
                    )
                    if existing is None:
                        raise RuntimeError(
                            f"dedupe race for key {dedupe_key!r}: "
                            "conflict without an active row"
                        )
                    return _row_to_job(existing)
                job = _row_to_job(row)
                self._append_job_audit(
                    conn,
                    job_row=row,
                    event_type="enqueued",
                    from_status=None,
                    actor=None,
                    detail={
                        "queue_name": queue_name,
                        "priority": priority,
                        "dedupe_key": dedupe_key,
                    },
                    now=now,
                )
                return job

            self._assert_pending_capacity(conn, tenant_id)
            self._consume_enqueue_rate(conn, tenant_id, now=now)

            row = conn.execute(
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
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s,
                    %s, %s,
                    %s, NULL, NULL, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s
                )
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    jid,
                    tenant_id,
                    queue_name,
                    Jsonb(payload),
                    JobStatus.PENDING.value,
                    priority,
                    max_attempts,
                    visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds,
                    retry_backoff_max_seconds,
                    run_at,
                    dep_ids,
                    label_list,
                    deadline,
                    estimated_duration_seconds,
                    bool(preemptible),
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            job = _row_to_job(row)
            self._append_job_audit(
                conn,
                job_row=row,
                event_type="enqueued",
                from_status=None,
                actor=None,
                detail={
                    "queue_name": queue_name,
                    "priority": priority,
                    "labels": label_list,
                },
                now=now,
            )
            return job


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
                raise CancelError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )

            job = _row_to_job(row)
            if job.status not in (
                JobStatus.PENDING,
                JobStatus.LEASED,
                JobStatus.AWAITING_APPROVAL,
                JobStatus.AWAITING_CHILDREN,
            ):
                raise CancelError(
                    f"job {job_id} cannot be cancelled "
                    f"(status={job.status.value}; only pending, leased, "
                    f"awaiting_approval, or awaiting_children jobs cancel)",
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
                current_job = _row_to_job(current) if current else None
                raise CancelError(
                    f"job {job_id} cannot be cancelled "
                    f"(status changed during cancel)",
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
            return _row_to_job(updated)


    def list_jobs(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs for *tenant_id*, optionally filtered by labels and status.

        *labels*: job must include **all** listed labels (AND). Order is
        newest ``created_at`` first.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        label_list = _normalize_labels(labels)
        status_list = _normalize_status_filter(status)

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status_list:
            clauses.append("status = ANY(%s)")
            params.append(status_list)
        if label_list:
            clauses.append("labels @> %s::text[]")
            params.append(label_list)
        params.extend([limit, offset])
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE {where}
                ORDER BY created_at DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            ).fetchall()
            return [_row_to_job(r) for r in rows]


    def cancel_matching(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        batch_size: int = _CANCEL_MATCHING_BATCH_DEFAULT,
        max_jobs: int | None = None,
        limit: int | None = None,
        lock_wait_seconds: float = 5.0,
        lock_retry_interval: float = 0.05,
    ) -> BulkCancelResult:
        """Cancel **all** cancelable jobs matching tenant + optional filters.

        Loops in batches of *batch_size* until no matching cancelable rows
        remain (or *max_jobs* total cancellations if set). Does **not**
        silently stop after one batch.

        Rows are locked with ``FOR UPDATE SKIP LOCKED``. An empty SKIP LOCKED
        page is **not** treated as success by itself: remaining matches are
        counted without locking. If matches still exist (held by other
        transactions), the sweep waits and retries until *lock_wait_seconds*
        elapses, then returns ``complete=False`` with ``remaining_estimate``.

        *limit* is a deprecated alias for *max_jobs*. Prefer *max_jobs*.

        Filters:
          * *labels*: job must include all listed labels
          * *status*: restrict to these statuses (must still be cancelable)
          * *queue_name*: optional queue scope

        When *status* is omitted, all cancelable statuses are targeted
        (pending, leased, awaiting_approval, awaiting_children). Terminal
        jobs are never cancelled. Cascades dependents / join children like
        :meth:`cancel`.
        """
        import time as _time

        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if lock_wait_seconds < 0:
            raise ValueError("lock_wait_seconds must be >= 0")
        if lock_retry_interval <= 0:
            raise ValueError("lock_retry_interval must be > 0")
        # Backward compat: old callers used limit= as a hard page size that
        # silently truncated. Treat limit as max_jobs (absolute cap).
        if max_jobs is None and limit is not None:
            max_jobs = int(limit)
        if max_jobs is not None and max_jobs < 1:
            raise ValueError("max_jobs must be >= 1 when set")

        label_list = _normalize_labels(labels)
        status_list = _normalize_status_filter(status)
        cancelable_values = {s.value for s in _CANCELABLE_STATUSES}
        if status_list is None:
            target_statuses = list(cancelable_values)
        else:
            target_statuses = [s for s in status_list if s in cancelable_values]
            if not target_statuses:
                raise ValueError(
                    "status filter has no cancelable statuses "
                    f"(cancelable={sorted(cancelable_values)})"
                )

        sample: list[Job] = []
        total = 0
        batches = 0
        complete = False
        remaining = 0
        lock_wait_deadline = _time.monotonic() + float(lock_wait_seconds)

        while True:
            if max_jobs is not None and total >= max_jobs:
                remaining = self._count_matching_cancelable(
                    tenant_id=tenant_id,
                    labels=label_list,
                    statuses=target_statuses,
                    queue_name=queue_name,
                )
                complete = remaining == 0
                break

            take = batch_size
            if max_jobs is not None:
                take = min(take, max_jobs - total)
            now = _utcnow()
            batch_n = 0
            with transaction(self._pool) as conn:
                clauses = [
                    "tenant_id = %s",
                    "status = ANY(%s)",
                ]
                params: list[Any] = [tenant_id, target_statuses]
                if queue_name is not None:
                    clauses.append("queue_name = %s")
                    params.append(queue_name)
                if label_list:
                    clauses.append("labels @> %s::text[]")
                    params.append(label_list)
                params.append(take)
                where = " AND ".join(clauses)
                rows = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE {where}
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    tuple(params),
                ).fetchall()

                for row in rows:
                    job = self._cancel_locked_row(
                        conn,
                        row=row,
                        tenant_id=tenant_id,
                        now=now,
                        reason="bulk cancel matching filter",
                    )
                    if job is not None:
                        batch_n += 1
                        total += 1
                        if len(sample) < _CANCEL_MATCHING_RETURN_CAP:
                            sample.append(job)

                self._maybe_finalize_offboard(
                    conn, tenant_id=tenant_id, now=now
                )

            batches += 1

            if batch_n > 0:
                # Progress made; keep sweeping (short page still may have
                # locked leftovers — next iteration will COUNT if empty).
                continue

            # Empty SKIP LOCKED page: either truly drained, or every remaining
            # match is locked by another transaction. Never assume success.
            remaining = self._count_matching_cancelable(
                tenant_id=tenant_id,
                labels=label_list,
                statuses=target_statuses,
                queue_name=queue_name,
            )
            if remaining == 0:
                complete = True
                break

            # Matches exist but are locked — wait and retry until deadline.
            if _time.monotonic() >= lock_wait_deadline:
                complete = False
                break
            _time.sleep(float(lock_retry_interval))

        return BulkCancelResult(
            tenant_id=tenant_id,
            cancelled=total,
            jobs=tuple(sample),
            complete=complete,
            remaining_estimate=remaining if not complete else 0,
            batches=batches,
        )


    def _count_matching_cancelable(
        self,
        *,
        tenant_id: str,
        labels: list[str],
        statuses: list[str],
        queue_name: str | None,
    ) -> int:
        clauses = ["tenant_id = %s", "status = ANY(%s)"]
        params: list[Any] = [tenant_id, statuses]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if labels:
            clauses.append("labels @> %s::text[]")
            params.append(labels)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE {where}",
                tuple(params),
            ).fetchone()
            return int(row["n"] if row else 0)


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
        return _row_to_job(updated)


    def get(self, job_id: UUID, *, tenant_id: str) -> Job | None:
        """Fetch a job by id for *tenant_id* only (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (job_id, tenant_id),
            ).fetchone()
            return _row_to_job(row) if row else None


    def get_result(self, job_id: UUID, *, tenant_id: str) -> Any | None:
        """Return the stashed ack *result* for a job, or None if missing/unset.

        Cross-tenant and unknown ids return None. Incomplete jobs typically
        have ``result is None`` until a successful ``ack(..., result=...)``.
        """
        job = self.get(job_id, tenant_id=tenant_id)
        if job is None:
            return None
        return job.result


    def count(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        status: JobStatus | None = None,
    ) -> int:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status is not None:
            clauses.append("status = %s")
            params.append(status.value)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE {where}",
                params,
            ).fetchone()
            assert row is not None
            return int(row["n"])


    @staticmethod
    def _lock_dedupe_key(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> None:
        """Transaction-scoped lock shared by enqueue and replay for one key."""
        # Two-arg form avoids colliding with tenant concurrency locks.
        # Use unit separators (not NUL — psycopg/Postgres reject 0x00 in text).
        lock_key = f"{tenant_id}\x1f{queue_name}\x1f{dedupe_key}"
        conn.execute(
            "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
            (_DEDUPE_LOCK_CLASS, lock_key),
        )


    @staticmethod
    def _normalize_depends_on(
        depends_on: UUID | Sequence[UUID] | None,
    ) -> list[UUID]:
        if depends_on is None:
            return []
        if isinstance(depends_on, UUID):
            return [depends_on]
        ids = list(depends_on)
        for item in ids:
            if not isinstance(item, UUID):
                raise TypeError("depends_on entries must be UUID instances")
        # Preserve order, drop duplicates.
        seen: set[UUID] = set()
        unique: list[UUID] = []
        for item in ids:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return unique


    def _validate_dependencies(
        self,
        conn: Any,
        *,
        tenant_id: str,
        depends_on: list[UUID],
    ) -> None:
        """Ensure parents exist for this tenant and are not already terminal-failed."""
        terminal_fail = {
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
        }
        for parent_id in depends_on:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (parent_id, tenant_id),
            ).fetchone()
            if row is None:
                raise DependencyError(
                    f"dependency {parent_id} not found for tenant {tenant_id!r}"
                )
            if row["status"] in terminal_fail:
                raise DependencyError(
                    f"dependency {parent_id} is already {row['status']}; "
                    "refusing to enqueue a child that can never run"
                )


    def _cascade_cancel_dependents(
        self,
        conn: Any,
        *,
        parent_id: UUID,
        tenant_id: str,
        reason: str,
        now: datetime,
    ) -> None:
        """Cancel dependents of *parent_id* (recursive).

        Covers:
          * jobs with ``depends_on`` containing *parent_id*
          * spawned subtasks with ``parent_job_id = parent_id``

        Active statuses cancelled: pending, leased, awaiting_approval,
        awaiting_children. Running (leased) holders have fencing tokens cleared.
        """
        active_pending = JobStatus.PENDING.value
        active_leased = JobStatus.LEASED.value
        active_approval = JobStatus.AWAITING_APPROVAL.value
        active_children = JobStatus.AWAITING_CHILDREN.value
        frontier: list[UUID] = [parent_id]
        visited: set[UUID] = set()
        while frontier:
            current = frontier.pop()
            if current in visited:
                continue
            visited.add(current)
            children = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status IN (%s, %s, %s, %s)
                  AND (
                      depends_on @> ARRAY[%s]::uuid[]
                      OR parent_job_id = %s
                  )
                FOR UPDATE
                """,
                (
                    tenant_id,
                    active_pending,
                    active_leased,
                    active_approval,
                    active_children,
                    current,
                    current,
                ),
            ).fetchall()
            for child in children:
                was_running = child["status"] == JobStatus.LEASED.value
                from_status = child["status"]
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
                        await_children_deadline_at = NULL,
                        await_children_outcome = CASE
                            WHEN status = %s
                            THEN COALESCE(await_children_outcome, 'cancelled')
                            ELSE await_children_outcome
                        END,
                        await_children_resolved_at = CASE
                            WHEN status = %s
                            THEN COALESCE(await_children_resolved_at, %s)
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
                        JobStatus.AWAITING_CHILDREN.value,
                        JobStatus.AWAITING_CHILDREN.value,
                        now,
                        err,
                        now,
                        child["id"],
                        tenant_id,
                        active_pending,
                        active_leased,
                        active_approval,
                        active_children,
                    ),
                ).fetchone()
                if updated is not None:
                    self._append_job_audit(
                        conn,
                        job_row=updated,
                        event_type="cascade_cancelled",
                        from_status=from_status,
                        actor="system",
                        detail={"reason": err},
                        now=now,
                    )
                frontier.append(child["id"])


    @staticmethod
    def _find_active_dedupe(
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        dedupe_key: str,
    ) -> dict[str, Any] | None:
        return conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND dedupe_key = %s
              AND status IN (%s, %s, %s, %s)
            LIMIT 1
            """,
            (
                tenant_id,
                queue_name,
                dedupe_key,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
                JobStatus.AWAITING_APPROVAL.value,
                JobStatus.AWAITING_CHILDREN.value,
            ),
        ).fetchone()


