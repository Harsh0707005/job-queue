"""JobQueue enqueue."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    DependencyError,
    _as_utc,
    _normalize_labels,
    _utcnow,
)


class JobsEnqueueCoreMixin:
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
                self._validate_dependencies(
                    conn, tenant_id=tenant_id, depends_on=dep_ids
                )

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
                    return self._job(existing, conn=conn)

                # New pending row: enforce per-tenant pending cap + rate.
                self._assert_pending_capacity(conn, tenant_id)
                self._consume_enqueue_rate(conn, tenant_id, now=now)

                part_id = self._compute_partition_id(
                    conn, tenant_id=tenant_id, job_id=jid
                )
                sealed, payload_key_id = self._seal_job_payload(
                    conn, tenant_id=tenant_id, job_id=jid, payload=payload
                )
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
                        created_at, updated_at, partition_id, payload_key_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, NULL, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s, %s, %s
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
                        Jsonb(sealed),
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
                        part_id,
                        payload_key_id,
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
                    return self._job(existing, conn=conn)
                job = self._job(row, conn=conn)
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
            part_id = self._compute_partition_id(
                conn, tenant_id=tenant_id, job_id=jid
            )
            sealed, payload_key_id = self._seal_job_payload(
                conn, tenant_id=tenant_id, job_id=jid, payload=payload
            )

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
                    created_at, updated_at, partition_id, payload_key_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s,
                    %s, %s,
                    %s, NULL, NULL, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                )
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    jid,
                    tenant_id,
                    queue_name,
                    Jsonb(sealed),
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
                    part_id,
                    payload_key_id,
                ),
            ).fetchone()
            assert row is not None
            job = self._job(row, conn=conn)
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
