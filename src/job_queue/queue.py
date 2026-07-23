"""Multi-tenant job queue with lease / heartbeat / ack semantics.

Design notes
------------
* **Lease**: ``lease()`` atomically claims the next available job using
  ``SELECT ... FOR UPDATE SKIP LOCKED``.  Only one worker can hold a lease.
* **Fencing token**: every lease mints a new ``lease_token``. Heartbeat /
  ack / nack require that token. After reclaim the old token is dead, so a
  zombie worker cannot complete or extend a job another worker now owns.
* **Visibility timeout**: while leased, the job is invisible until
  ``lease_expires_at``. If the worker dies (no heartbeat), the lease expires
  and another worker may reclaim it. Live workers must heartbeat (the
  ``Worker`` helper does this) and abort when the lease is lost.
* **Delayed jobs**: pending jobs with ``available_at`` in the future cannot
  be claimed until that time.
* **Deduplication**: ``dedupe_key`` ensures at most one active (pending or
  leased) job per key within a tenant queue; a second enqueue returns the
  existing row instead of inserting a duplicate.
* **Tenant concurrency limit**: at most N jobs may be ``leased`` for a tenant
  at once (``set_concurrency_limit``). Enforced under a per-tenant advisory
  lock so concurrent workers cannot exceed the cap.
* **Cancel**: pending jobs can be cancelled by id; leased jobs are left alone.
* **Dead letter**: exhausted jobs land in ``dead``; inspect via
  ``list_dead_letters`` and re-queue with ``replay_dead_letter`` /
  ``replay_dead_letters``.
* **Tenant isolation**: every mutating and read-by-id API requires
  ``tenant_id``; cross-tenant access is rejected.
* **Ack**: marks the job completed under a still-valid lease only.
* **Nack**: releases the lease (or dead-letters after max attempts).
* **Recurring schedules**: ``create_schedule`` registers interval-based work;
  ``process_due_schedules`` enqueues jobs when due; pause/resume/delete stop them.
  A schedule never enqueues a new job while a previous one is still
  ``pending`` or ``leased`` (no overlapping runs).
* **Dependencies**: ``depends_on`` parent job id(s). A child is not claimable
  until every parent is ``completed``. If a parent ends as failed/dead/cancelled,
  pending dependents are cascade-cancelled (they do not sit forever).
* **Fair multi-tenant lease**: ``lease(worker_id=...)`` without ``tenant_id``
  claims the next ready job across tenants, preferring tenants with fewer
  current leases so a noisy tenant cannot starve others. Per-job rules
  (delay, deps, concurrency caps, queue name) still apply.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from job_queue.db import init_schema, transaction
from job_queue.models import Job, JobStatus, Schedule, ScheduleFire

# Namespace for pg_advisory_xact_lock(classid, hash) so dedupe locks never
# collide with tenant concurrency locks (which use single-arg hashtext).
_DEDUPE_LOCK_CLASS = 884_422_01

_JOB_COLUMNS = """
    id, tenant_id, queue_name, payload, status, priority, attempts,
    max_attempts, visibility_timeout_seconds, available_at, dedupe_key,
    schedule_id, depends_on, leased_by, lease_token, lease_expires_at,
    created_at, updated_at, completed_at, result, last_error
"""

# Pending job is claimable only when every depends_on parent is completed.
_DEPS_SATISFIED_SQL = """
(
    COALESCE(depends_on, '{}'::uuid[]) = '{}'::uuid[]
    OR NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(depends_on, '{}'::uuid[])) AS parent_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM jobs AS parent
            WHERE parent.id = parent_id
              AND parent.tenant_id = jobs.tenant_id
              AND parent.status = 'completed'
        )
    )
)
"""


def _row_to_job(row: dict[str, Any]) -> Job:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    deps = row.get("depends_on") or []
    if isinstance(deps, str):
        # unlikely, but be defensive
        deps = []
    return Job(
        id=row["id"],
        tenant_id=row["tenant_id"],
        queue_name=row["queue_name"],
        payload=payload,
        status=JobStatus(row["status"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        visibility_timeout_seconds=row["visibility_timeout_seconds"],
        available_at=row["available_at"],
        dedupe_key=row["dedupe_key"],
        schedule_id=row["schedule_id"],
        depends_on=tuple(deps),
        leased_by=row["leased_by"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        result=row.get("result"),
        last_error=row["last_error"],
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class LeaseError(Exception):
    """Raised when a lease operation is invalid (wrong owner/tenant/token, expired, etc.)."""


class CancelError(Exception):
    """Raised when a job cannot be cancelled (wrong state / tenant / missing)."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class ReplayError(Exception):
    """Raised when a dead-lettered job cannot be replayed."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class DependencyError(Exception):
    """Raised when job dependency setup is invalid."""


class _SkipLeaseCandidate(Exception):
    """Internal: abandon a locked fair-lease candidate and try another."""


class ScheduleError(Exception):
    """Raised when a schedule operation is invalid."""

    def __init__(self, message: str, *, schedule: Schedule | None = None) -> None:
        super().__init__(message)
        self.schedule = schedule


_SCHEDULE_COLUMNS = """
    id, tenant_id, name, queue_name, payload, interval_seconds, priority,
    max_attempts, visibility_timeout_seconds, enabled, next_run_at,
    last_enqueued_at, created_at, updated_at
"""


def _row_to_schedule(row: dict[str, Any]) -> Schedule:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return Schedule(
        id=row["id"],
        tenant_id=row["tenant_id"],
        name=row["name"],
        queue_name=row["queue_name"],
        payload=payload,
        interval_seconds=row["interval_seconds"],
        priority=row["priority"],
        max_attempts=row["max_attempts"],
        visibility_timeout_seconds=row["visibility_timeout_seconds"],
        enabled=bool(row["enabled"]),
        next_run_at=row["next_run_at"],
        last_enqueued_at=row["last_enqueued_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class JobQueue:
    """PostgreSQL-backed multi-tenant job queue."""

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    @classmethod
    def connect(cls, dsn: str, *, min_size: int = 1, max_size: int = 10) -> JobQueue:
        """Open a pool, ensure schema exists, return a ready queue."""
        from job_queue.db import create_pool

        pool = create_pool(dsn, min_size=min_size, max_size=max_size)
        with pool.connection() as conn:
            init_schema(conn)
        return cls(pool)

    def close(self) -> None:
        self._pool.close()

    # ------------------------------------------------------------------
    # Tenant concurrency limits
    # ------------------------------------------------------------------

    def set_concurrency_limit(
        self,
        tenant_id: str,
        max_concurrent: int | None,
    ) -> None:
        """Set how many jobs may be leased at once for *tenant_id*.

        Pass ``None`` (or remove the limit) to allow unlimited concurrent leases.
        ``max_concurrent`` must be >= 1 when set.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1 or None")

        now = _utcnow()
        with transaction(self._pool) as conn:
            if max_concurrent is None:
                conn.execute(
                    "DELETE FROM tenant_settings WHERE tenant_id = %s",
                    (tenant_id,),
                )
                return
            conn.execute(
                """
                INSERT INTO tenant_settings (tenant_id, max_concurrent_leases, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET max_concurrent_leases = EXCLUDED.max_concurrent_leases,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, max_concurrent, now),
            )

    def get_concurrency_limit(self, tenant_id: str) -> int | None:
        """Return the tenant's max concurrent leases, or None if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT max_concurrent_leases
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                return None
            return row["max_concurrent_leases"]

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def enqueue(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        queue_name: str = "default",
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        dedupe_key: str | None = None,
        depends_on: UUID | Sequence[UUID] | None = None,
        job_id: UUID | None = None,
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
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if dedupe_key is not None and dedupe_key == "":
            raise ValueError("dedupe_key must be non-empty when provided")

        dep_ids = self._normalize_depends_on(depends_on)

        now = _utcnow()
        if available_at is not None:
            run_at = _as_utc(available_at)
        elif delay_seconds is not None:
            run_at = now + timedelta(seconds=float(delay_seconds))
        else:
            run_at = now

        jid = job_id or uuid4()
        if jid in dep_ids:
            raise DependencyError("a job cannot depend on itself")

        with transaction(self._pool) as conn:
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

                # ON CONFLICT remains a safety net for any missed paths.
                row = conn.execute(
                    f"""
                    INSERT INTO jobs (
                        id, tenant_id, queue_name, payload, status, priority,
                        attempts, max_attempts, visibility_timeout_seconds,
                        available_at, dedupe_key, schedule_id, depends_on,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s, %s, NULL, %s,
                        %s, %s
                    )
                    ON CONFLICT (tenant_id, queue_name, dedupe_key)
                        WHERE dedupe_key IS NOT NULL
                          AND status IN ('pending', 'leased')
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
                        run_at,
                        dedupe_key,
                        dep_ids,
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
                return _row_to_job(row)

            row = conn.execute(
                f"""
                INSERT INTO jobs (
                    id, tenant_id, queue_name, payload, status, priority,
                    attempts, max_attempts, visibility_timeout_seconds,
                    available_at, dedupe_key, schedule_id, depends_on,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s, NULL, NULL, %s,
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
                    run_at,
                    dep_ids,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return _row_to_job(row)

    # ------------------------------------------------------------------
    # Lease
    # ------------------------------------------------------------------

    def lease(
        self,
        *,
        worker_id: str,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> Job | None:
        """Claim the next available job.

        Pass *tenant_id* to lease only that tenant's work (legacy / dedicated
        workers). Omit *tenant_id* for **fair multi-tenant** leasing: the
        queue picks a ready job across tenants, preferring tenants that
        currently hold fewer leases so one noisy tenant cannot starve others.

        Available means:
          * status = pending AND available_at <= now() AND all ``depends_on``
            parents are ``completed``, OR
          * status = leased AND lease_expires_at < now()  (orphaned lease)
            AND attempts < max_attempts

        Delayed pending jobs, unmet dependencies, and per-tenant concurrency
        caps are always enforced. Expired leases that already used all
        attempts are dead-lettered first (dependents cascade-cancelled).

        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never claim the
        same row. A fresh ``lease_token`` is minted for fencing.
        """
        if not worker_id:
            raise ValueError("worker_id is required")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")

        if tenant_id is None:
            return self._lease_fair(
                worker_id=worker_id,
                queue_name=queue_name,
                visibility_timeout_seconds=visibility_timeout_seconds,
            )
        return self._lease_for_tenant(
            tenant_id=tenant_id,
            worker_id=worker_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )

    def _lease_for_tenant(
        self,
        *,
        tenant_id: str,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
    ) -> Job | None:
        now = _utcnow()
        token = uuid4()

        with transaction(self._pool) as conn:
            # Serialize lease decisions for this tenant so concurrency caps
            # cannot be exceeded by racing workers.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )

            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=tenant_id)

            limit = self._read_concurrency_limit(conn, tenant_id)
            leased_count = self._count_leased(conn, tenant_id)
            at_limit = limit is not None and leased_count >= limit

            if at_limit:
                candidate = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      AND status = %s
                      AND lease_expires_at < %s
                      AND attempts < max_attempts
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.LEASED.value,
                        now,
                    ),
                ).fetchone()
            else:
                candidate = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      AND (
                            (
                                status = %s
                                AND available_at <= %s
                                AND {_DEPS_SATISFIED_SQL}
                            )
                         OR (
                                status = %s
                            AND lease_expires_at < %s
                            AND attempts < max_attempts
                         )
                      )
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.PENDING.value,
                        now,
                        JobStatus.LEASED.value,
                        now,
                    ),
                ).fetchone()

            if candidate is None:
                return None
            return self._claim_locked_job(
                conn,
                candidate,
                worker_id=worker_id,
                token=token,
                visibility_timeout_seconds=visibility_timeout_seconds,
                now=now,
            )

    def _lease_fair(
        self,
        *,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        _max_attempts: int = 64,
    ) -> Job | None:
        """Lease fairly across tenants for *queue_name*."""
        now = _utcnow()

        with transaction(self._pool) as conn:
            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=None)

            for _ in range(_max_attempts):
                try:
                    # Savepoint so a rejected candidate releases its row lock.
                    with conn.transaction():
                        claimed = self._try_claim_fair_candidate(
                            conn,
                            worker_id=worker_id,
                            queue_name=queue_name,
                            visibility_timeout_seconds=visibility_timeout_seconds,
                            now=now,
                        )
                        if claimed is None:
                            return None
                        return claimed
                except _SkipLeaseCandidate:
                    continue
            return None

    def _try_claim_fair_candidate(
        self,
        conn: Any,
        *,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        now: datetime,
    ) -> Job | None:
        token = uuid4()
        # Prefer tenants with fewer current leases, then job priority / age.
        # Concurrency caps and reclaim rules are applied in WHERE / re-check.
        candidate = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE queue_name = %s
              AND (
                    (
                        status = %s
                        AND available_at <= %s
                        AND {_DEPS_SATISFIED_SQL}
                        AND (
                            NOT EXISTS (
                                SELECT 1
                                FROM tenant_settings ts
                                WHERE ts.tenant_id = jobs.tenant_id
                                  AND ts.max_concurrent_leases IS NOT NULL
                            )
                            OR (
                                SELECT COUNT(*)::int
                                FROM jobs j2
                                WHERE j2.tenant_id = jobs.tenant_id
                                  AND j2.status = %s
                            ) < (
                                SELECT ts.max_concurrent_leases
                                FROM tenant_settings ts
                                WHERE ts.tenant_id = jobs.tenant_id
                            )
                        )
                    )
                 OR (
                        status = %s
                    AND lease_expires_at < %s
                    AND attempts < max_attempts
                 )
              )
            ORDER BY
              (
                SELECT COUNT(*)::int
                FROM jobs j3
                WHERE j3.tenant_id = jobs.tenant_id
                  AND j3.status = %s
              ) ASC,
              priority DESC,
              created_at ASC
            FOR UPDATE OF jobs SKIP LOCKED
            LIMIT 1
            """,
            (
                queue_name,
                JobStatus.PENDING.value,
                now,
                JobStatus.LEASED.value,
                JobStatus.LEASED.value,
                now,
                JobStatus.LEASED.value,
            ),
        ).fetchone()

        if candidate is None:
            return None

        # Re-check concurrency under tenant lock (closes races with other workers).
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (candidate["tenant_id"],),
        )
        if candidate["status"] == JobStatus.PENDING.value:
            limit = self._read_concurrency_limit(conn, candidate["tenant_id"])
            leased_count = self._count_leased(conn, candidate["tenant_id"])
            if limit is not None and leased_count >= limit:
                raise _SkipLeaseCandidate("tenant at concurrency limit")

        return self._claim_locked_job(
            conn,
            candidate,
            worker_id=worker_id,
            token=token,
            visibility_timeout_seconds=visibility_timeout_seconds,
            now=now,
        )

    def _claim_locked_job(
        self,
        conn: Any,
        candidate: dict[str, Any],
        *,
        worker_id: str,
        token: UUID,
        visibility_timeout_seconds: int | None,
        now: datetime,
    ) -> Job:
        new_attempts = candidate["attempts"] + 1
        vt = visibility_timeout_seconds or candidate["visibility_timeout_seconds"]
        lease_expires = now + timedelta(seconds=vt)

        row = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = %s,
                lease_token = %s,
                lease_expires_at = %s,
                attempts = %s,
                visibility_timeout_seconds = %s,
                updated_at = %s,
                last_error = NULL
            WHERE id = %s
              AND tenant_id = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.LEASED.value,
                worker_id,
                token,
                lease_expires,
                new_attempts,
                vt,
                now,
                candidate["id"],
                candidate["tenant_id"],
            ),
        ).fetchone()
        assert row is not None
        return _row_to_job(row)

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
                    lease_expires_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
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
                    lease_expires_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'exceeded max_attempts after lease expiry'
                    ),
                    updated_at = %s
                WHERE status = %s
                  AND lease_expires_at < %s
                  AND attempts >= max_attempts
                RETURNING id, tenant_id
                """,
                (
                    JobStatus.DEAD.value,
                    now,
                    JobStatus.LEASED.value,
                    now,
                ),
            ).fetchall()

        for dead in dead_rows:
            self._cascade_cancel_dependents(
                conn,
                parent_id=dead["id"],
                tenant_id=dead["tenant_id"],
                reason=f"parent job {dead['id']} dead-lettered",
                now=now,
            )

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    def cancel(self, job_id: UUID, *, tenant_id: str) -> Job:
        """Cancel a *pending* job by id for *tenant_id*.

        Only ``pending`` jobs are cancelled. If the job is leased (or in any
        other non-pending state), raises ``CancelError`` and leaves it
        unchanged. Cross-tenant ids raise ``CancelError`` as not found.
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
            if job.status != JobStatus.PENDING:
                raise CancelError(
                    f"job {job_id} cannot be cancelled "
                    f"(status={job.status.value}; only pending jobs cancel)",
                    job=job,
                )

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    job_id,
                    tenant_id,
                    JobStatus.PENDING.value,
                ),
            ).fetchone()
            if updated is None:
                # Race: became leased between SELECT and UPDATE.
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
            self._cascade_cancel_dependents(
                conn,
                parent_id=job_id,
                tenant_id=tenant_id,
                reason=f"parent job {job_id} cancelled",
                now=now,
            )
            return _row_to_job(updated)

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

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

        Requires matching tenant, worker, and fencing token. Raises
        ``LeaseError`` if the lease was reclaimed, expired, or completed.
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_tenant(conn, job_id, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            extend = extend_by_seconds or row["visibility_timeout_seconds"]
            new_expiry = now + timedelta(seconds=extend)

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET lease_expires_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    new_expiry,
                    now,
                    job_id,
                    tenant_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during heartbeat")
            return _row_to_job(updated)

    # ------------------------------------------------------------------
    # Ack / Nack
    # ------------------------------------------------------------------

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
        via ``get`` / ``get_result``.
        """
        now = _utcnow()
        result_json = Jsonb(result) if result is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_tenant(conn, job_id, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    completed_at = %s,
                    result = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
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
                    tenant_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during ack")
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
        ``pending`` so another worker can pick it up. Otherwise it is marked
        ``failed`` (or ``dead`` when attempts are exhausted).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_tenant(conn, job_id, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)

            attempts = row["attempts"]
            max_attempts = row["max_attempts"]

            if requeue and attempts < max_attempts:
                new_status = JobStatus.PENDING.value
            elif attempts >= max_attempts:
                new_status = JobStatus.DEAD.value
            else:
                new_status = JobStatus.FAILED.value

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    last_error = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    new_status,
                    error,
                    now,
                    job_id,
                    tenant_id,
                    lease_token,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                raise LeaseError(f"job {job_id} lease lost during nack")

            if new_status in (JobStatus.DEAD.value, JobStatus.FAILED.value):
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=job_id,
                    tenant_id=tenant_id,
                    reason=(
                        f"parent job {job_id} ended as {new_status}"
                        + (f": {error}" if error else "")
                    ),
                    now=now,
                )
            return _row_to_job(updated)

    def ensure_lease(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
    ) -> Job:
        """Return the job if this worker still holds a live lease; else raise."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = self._lock_job_for_tenant(conn, job_id, tenant_id)
            self._assert_active_lease(row, worker_id, lease_token, now)
            return _row_to_job(row)

    # ------------------------------------------------------------------
    # Inspection helpers
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Dead letter inspect + replay
    # ------------------------------------------------------------------

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
            return _row_to_job(updated)

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


    # ------------------------------------------------------------------
    # Recurring schedules
    # ------------------------------------------------------------------

    def create_schedule(
        self,
        *,
        tenant_id: str,
        interval_seconds: int,
        payload: dict[str, Any],
        queue_name: str = "default",
        name: str | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        start_at: datetime | None = None,
        enabled: bool = True,
        schedule_id: UUID | None = None,
    ) -> Schedule:
        """Register a recurring schedule that enqueues jobs every N seconds.

        The schedule is stored in Postgres. Call ``process_due_schedules()``
        (or run ``ScheduleRunner``) to materialize due ticks into jobs.

        Parameters
        ----------
        interval_seconds:
            Fire period in seconds (>= 1).
        start_at:
            First eligible fire time. Defaults to now (due on next tick).
        name:
            Optional unique name per tenant for lookup / idempotent registration.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if interval_seconds < 1:
            raise ValueError("interval_seconds must be >= 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if name is not None and name == "":
            raise ValueError("name must be non-empty when provided")

        now = _utcnow()
        first_run = _as_utc(start_at) if start_at is not None else now
        sid = schedule_id or uuid4()

        with transaction(self._pool) as conn:
            try:
                row = conn.execute(
                    f"""
                    INSERT INTO schedules (
                        id, tenant_id, name, queue_name, payload,
                        interval_seconds, priority, max_attempts,
                        visibility_timeout_seconds, enabled, next_run_at,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        sid,
                        tenant_id,
                        name,
                        queue_name,
                        Jsonb(payload),
                        interval_seconds,
                        priority,
                        max_attempts,
                        visibility_timeout_seconds,
                        enabled,
                        first_run,
                        now,
                        now,
                    ),
                ).fetchone()
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule name {name!r} already exists for tenant {tenant_id!r}"
                ) from exc
            assert row is not None
            return _row_to_schedule(row)

    def get_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule | None:
        """Fetch a schedule by id for *tenant_id* (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return _row_to_schedule(row) if row else None

    def list_schedules(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Schedule]:
        """List schedules for a tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if enabled_only:
            clauses.append("enabled = TRUE")
        where = " AND ".join(clauses)
        params.extend([limit, offset])
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [_row_to_schedule(r) for r in rows]

    def pause_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule:
        """Stop a schedule from firing (keeps the row; use resume to re-enable)."""
        return self._set_schedule_enabled(schedule_id, tenant_id=tenant_id, enabled=False)

    def resume_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        next_run_at: datetime | None = None,
    ) -> Schedule:
        """Re-enable a paused schedule.

        If *next_run_at* is omitted, the next fire is set to now so it is due
        on the following ``process_due_schedules`` call.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        run_at = _as_utc(next_run_at) if next_run_at is not None else now
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = TRUE,
                    next_run_at = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (run_at, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return _row_to_schedule(row)

    def delete_schedule(self, schedule_id: UUID, *, tenant_id: str) -> bool:
        """Permanently remove a schedule. Returns True if a row was deleted."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM schedules
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return row is not None

    def process_due_schedules(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
    ) -> list[ScheduleFire]:
        """Enqueue jobs for schedules that are due and advance their next run.

        Safe for concurrent callers: due rows are claimed with
        ``FOR UPDATE SKIP LOCKED``. Each claimed schedule enqueues at most one
        job in the same transaction, then sets
        ``next_run_at = now + interval_seconds`` (fixed delay from fire time,
        no catch-up burst).

        **No overlapping runs:** if a previous job for the same schedule is
        still ``pending`` or ``leased``, this tick is skipped and
        ``next_run_at`` is left unchanged so the schedule remains due and can
        fire as soon as that in-flight job completes.

        Returns a list of ``ScheduleFire`` (updated schedule + enqueued job).
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        now = _utcnow()
        fires: list[ScheduleFire] = []

        with transaction(self._pool) as conn:
            if tenant_id is not None:
                due = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE enabled = TRUE
                      AND next_run_at <= %s
                      AND tenant_id = %s
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, tenant_id, limit),
                ).fetchall()
            else:
                due = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE enabled = TRUE
                      AND next_run_at <= %s
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, limit),
                ).fetchall()

            for sched_row in due:
                # Skip if a prior run is still in flight (pending or leased).
                active = conn.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE schedule_id = %s
                      AND status IN (%s, %s)
                    LIMIT 1
                    """,
                    (
                        sched_row["id"],
                        JobStatus.PENDING.value,
                        JobStatus.LEASED.value,
                    ),
                ).fetchone()
                if active is not None:
                    continue

                next_run = now + timedelta(seconds=int(sched_row["interval_seconds"]))
                job_id = uuid4()
                payload = sched_row["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                try:
                    with conn.transaction():
                        job_row = conn.execute(
                            f"""
                            INSERT INTO jobs (
                                id, tenant_id, queue_name, payload, status, priority,
                                attempts, max_attempts, visibility_timeout_seconds,
                                available_at, dedupe_key, schedule_id, depends_on,
                                created_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                0, %s, %s,
                                %s, NULL, %s, %s,
                                %s, %s
                            )
                            RETURNING {_JOB_COLUMNS}
                            """,
                            (
                                job_id,
                                sched_row["tenant_id"],
                                sched_row["queue_name"],
                                Jsonb(payload),
                                JobStatus.PENDING.value,
                                sched_row["priority"],
                                sched_row["max_attempts"],
                                sched_row["visibility_timeout_seconds"],
                                now,
                                sched_row["id"],
                                [],
                                now,
                                now,
                            ),
                        ).fetchone()
                except UniqueViolation:
                    # Race: another ticker inserted an active job for this schedule.
                    continue

                assert job_row is not None

                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET next_run_at = %s,
                        last_enqueued_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (next_run, now, now, sched_row["id"]),
                ).fetchone()
                assert updated is not None
                fires.append(
                    ScheduleFire(
                        schedule=_row_to_schedule(updated),
                        job=_row_to_job(job_row),
                    )
                )

        return fires

    def _set_schedule_enabled(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> Schedule:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (enabled, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return _row_to_schedule(row)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _lock_job_for_tenant(
        self,
        conn: Any,
        job_id: UUID,
        tenant_id: str,
    ) -> dict[str, Any]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
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
            raise LeaseError(f"job {job_id} not found for tenant {tenant_id!r}")
        return row

    @staticmethod
    def _read_concurrency_limit(conn: Any, tenant_id: str) -> int | None:
        row = conn.execute(
            """
            SELECT max_concurrent_leases
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return None
        return row["max_concurrent_leases"]

    @staticmethod
    def _count_leased(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM jobs
            WHERE tenant_id = %s AND status = %s
            """,
            (tenant_id, JobStatus.LEASED.value),
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
        """Cancel pending jobs that depend on *parent_id* (recursively)."""
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
                  AND status = %s
                  AND depends_on @> ARRAY[%s]::uuid[]
                FOR UPDATE
                """,
                (tenant_id, JobStatus.PENDING.value, current),
            ).fetchall()
            for child in children:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status = %s
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        reason,
                        now,
                        child["id"],
                        tenant_id,
                        JobStatus.PENDING.value,
                    ),
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
              AND status IN (%s, %s)
            LIMIT 1
            """,
            (
                tenant_id,
                queue_name,
                dedupe_key,
                JobStatus.PENDING.value,
                JobStatus.LEASED.value,
            ),
        ).fetchone()

    @staticmethod
    def _assert_active_lease(
        row: dict[str, Any],
        worker_id: str,
        lease_token: UUID,
        now: datetime,
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
        expires = row["lease_expires_at"]
        if expires is not None and expires < now:
            raise LeaseError(f"job {row['id']} lease has expired")
