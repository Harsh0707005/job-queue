"""JobQueue enqueue domain: enqueue, dedupe, dependencies."""
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


class JobsEnqueueMixin:
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
