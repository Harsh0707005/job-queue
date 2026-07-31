"""JobQueue dead_letters domain (DeadLettersMixin)."""
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

class DeadLettersMixin:
    def list_dead_letters(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """Return dead-lettered jobs for *tenant_id*, newest first.

        Optionally filter by *queue_name*. Use *limit* / *offset* for paging.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        clauses = ["tenant_id = %s", "status = %s"]
        params: list[Any] = [tenant_id, JobStatus.DEAD.value]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        where = " AND ".join(clauses)
        params.extend([limit, offset])

        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE {where}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [_row_to_job(r) for r in rows]


    def replay_dead_letter(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> Job:
        """Move a dead-lettered job back to ``pending`` so it can be leased.

        Parameters
        ----------
        reset_attempts:
            When True (default), set ``attempts`` back to 0.
        max_attempts:
            Optional new max attempts (must be >= 1). Leaves existing value
            when omitted.
        available_at / delay_seconds:
            Optional delay before the replayed job becomes claimable.
            Defaults to now (immediately claimable).
        clear_error:
            When True, clear ``last_error``; otherwise keep it for forensics.

        Raises ``ReplayError`` if the job is missing, wrong tenant, not dead,
        or its ``dedupe_key`` collides with another active job.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        now = _utcnow()
        if available_at is not None:
            run_at = _as_utc(available_at)
        elif delay_seconds is not None:
            run_at = now + timedelta(seconds=float(delay_seconds))
        else:
            run_at = now

        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ReplayError(
                    f"tenant {tenant_id!r} is offboarding; replay is rejected"
                )
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
                raise ReplayError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )

            job = _row_to_job(row)
            if job.status != JobStatus.DEAD:
                raise ReplayError(
                    f"job {job_id} is not dead-lettered "
                    f"(status={job.status.value})",
                    job=job,
                )

            try:
                self._assert_pending_capacity(conn, tenant_id)
                self._consume_enqueue_rate(conn, tenant_id, now=now)
            except ValueError as exc:
                raise ReplayError(str(exc), job=job) from exc
            except RateLimitError as exc:
                raise ReplayError(str(exc), job=job) from exc

            # Hold the same advisory lock as enqueue(dedupe_key=...) so the
            # check + status flip is atomic w.r.t. concurrent inserts.
            if job.dedupe_key is not None:
                self._lock_dedupe_key(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=job.queue_name,
                    dedupe_key=job.dedupe_key,
                )
                conflict = self._find_active_dedupe(
                    conn,
                    tenant_id=tenant_id,
                    queue_name=job.queue_name,
                    dedupe_key=job.dedupe_key,
                )
                if conflict is not None and conflict["id"] != job.id:
                    raise ReplayError(
                        f"job {job_id} cannot be replayed: dedupe_key "
                        f"{job.dedupe_key!r} is already active on job "
                        f"{conflict['id']}",
                        job=job,
                    )

            new_attempts = 0 if reset_attempts else job.attempts
            new_max = max_attempts if max_attempts is not None else job.max_attempts
            new_error = None if clear_error else job.last_error

            # Savepoint: if the unique index still fires (defensive), convert
            # to ReplayError instead of leaking a raw DB error.
            try:
                with conn.transaction():
                    updated = conn.execute(
                        f"""
                        UPDATE jobs
                        SET status = %s,
                            attempts = %s,
                            max_attempts = %s,
                            available_at = %s,
                            leased_by = NULL,
                            lease_token = NULL,
                            lease_tenant_id = NULL,
                            lease_expires_at = NULL,
                            completed_at = NULL,
                            last_error = %s,
                            updated_at = %s
                        WHERE id = %s
                          AND tenant_id = %s
                          AND status = %s
                        RETURNING {_JOB_COLUMNS}
                        """,
                        (
                            JobStatus.PENDING.value,
                            new_attempts,
                            new_max,
                            run_at,
                            new_error,
                            now,
                            job_id,
                            tenant_id,
                            JobStatus.DEAD.value,
                        ),
                    ).fetchone()
            except UniqueViolation as exc:
                raise ReplayError(
                    f"job {job_id} cannot be replayed: dedupe_key "
                    f"{job.dedupe_key!r} became active concurrently",
                    job=job,
                ) from exc

            if updated is None:
                raise ReplayError(
                    f"job {job_id} could not be replayed (status changed)",
                    job=job,
                )
            out = _row_to_job(updated)
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="replayed",
                from_status=JobStatus.DEAD.value,
                actor=None,
                detail={"reset_attempts": reset_attempts},
                now=now,
            )
            return out


    def replay_dead_letters(
        self,
        *,
        tenant_id: str,
        job_ids: list[UUID] | None = None,
        queue_name: str | None = None,
        limit: int | None = None,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> list[Job]:
        """Replay multiple dead-lettered jobs back to ``pending``.

        If *job_ids* is provided, only those ids are considered (still scoped
        to *tenant_id*). Otherwise all dead letters for the tenant (optionally
        filtered by *queue_name*) are replayed, up to *limit* when set.

        Jobs that cannot be replayed (wrong state, dedupe conflict) are
        skipped; only successfully replayed jobs are returned. For strict
        single-job semantics with errors, use ``replay_dead_letter``.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")

        if job_ids is not None:
            targets = list(job_ids)
        else:
            page_limit = limit if limit is not None else 10_000
            dead = self.list_dead_letters(
                tenant_id=tenant_id,
                queue_name=queue_name,
                limit=page_limit,
                offset=0,
            )
            targets = [j.id for j in dead]
            if limit is not None:
                targets = targets[:limit]

        replayed: list[Job] = []
        for jid in targets:
            try:
                replayed.append(
                    self.replay_dead_letter(
                        jid,
                        tenant_id=tenant_id,
                        reset_attempts=reset_attempts,
                        max_attempts=max_attempts,
                        available_at=available_at,
                        delay_seconds=delay_seconds,
                        clear_error=clear_error,
                    )
                )
            except ReplayError:
                continue
        return replayed


