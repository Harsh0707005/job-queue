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
* **Max runtime**: ``max_runtime_seconds`` is a hard wall-clock limit from
  ``lease_started_at``. Heartbeats cannot extend the lease past that deadline;
  once exceeded the lease is force-released (requeue or dead-letter) so a
  stuck worker cannot hold a job forever by heartbeating.
* **Delayed jobs**: pending jobs with ``available_at`` in the future cannot
  be claimed until that time.
* **Deduplication**: ``dedupe_key`` ensures at most one active (pending or
  leased) job per key within a tenant queue; a second enqueue returns the
  existing row instead of inserting a duplicate.
* **Tenant concurrency limit**: at most N jobs may be ``leased`` for a tenant
  at once (``set_concurrency_limit``). Enforced under a per-tenant advisory
  lock so concurrent workers cannot exceed the cap.
* **Tenant pending cap**: at most N jobs may be ``pending`` for a tenant
  (``set_pending_limit``). Further ``enqueue`` / schedule ticks / replays
  raise ``ValueError`` until pending drops (e.g. after lease or cancel).
* **Tenant merge**: ``merge_tenant(source, target)`` moves all of *source*'s
  jobs (pending + running + history), schedules, and webhooks to *target*
  without stopping workers. Lease fencing tokens stay valid; heartbeat /
  ack / nack still succeed when the worker still sends the pre-merge
  ``tenant_id`` (ops lock by job id + lease token). After merge *source*
  is empty (no jobs / schedules / webhooks / settings).
* **Cancel**: ``cancel`` stops *pending* or *leased* (running) jobs. Cancelling
  a leased job clears the fencing ``lease_token`` so the holding worker's
  heartbeat / ack / nack fail with ``LeaseError`` and cannot complete it.
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
  dependents are cascade-cancelled **recursively** — both *pending* and
  *leased* (running) children are stopped: fencing tokens cleared so the
  holding worker cannot ack/heartbeat and finish them.
* **Priority lanes + fair multi-tenant lease**: ``lease`` / ``lease_batch``
  without ``tenant_id`` claim ready jobs across tenants in **priority lanes**:
  higher ``priority`` always before lower. Within the same priority, tenants
  rotate by **least-recently-served** (``tenant_fairness.last_lease_at``) so a
  noisy high-priority backlog cannot monopolize the lane — and a noisy
  *low*-priority backlog cannot jump ahead of others' high-priority work.
  ``lease_batch(limit=N)`` pulls up to N jobs in one go (each claim still
  updates fairness so a single batch rotates tenants). Scoped
  ``lease(tenant_id=...)`` is priority-ordered within that tenant only.
  Per-job rules (delay, deps, concurrency caps, queue name) still apply.
* **Retry backoff**: nack-requeue uses exponential ``available_at`` delay
  (base * 2^(attempt-1), capped by max). After ``max_attempts`` the job is
  dead-lettered instead of requeued forever.
* **Tenant pause**: ``pause_tenant`` stops new leases and schedule ticks for
  that tenant, and blocks reclaim of expired leases until resume. In-flight
  holders may still heartbeat / ack / nack. ``resume_tenant`` restores
  leasing and aligns overdue schedules for a single clean next fire (no
  missed-tick flood). ``get_tenant_stats`` reports counts.
* **Tenant offboard**: ``offboard_tenant`` fully retires a tenant — cancels
  pending work, deletes schedules and non-claimed webhooks, purges non-running
  jobs, and blocks new enqueue. Already-leased jobs and already-claimed
  (``delivering``) webhooks may still finish / finalize; when the last
  in-flight work ends the tenant is auto-purged empty (settings/fairness
  cleared).
* **Webhooks**: terminal events (completed / failed / dead) insert durable
  ``webhook_deliveries`` first; ``process_webhook_deliveries`` POSTs with
  exponential backoff and dead-letters failed deliveries. Paused tenants do
  not enqueue new deliveries and their pending rows are not claimed until
  resume.
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
from job_queue.models import (
    Job,
    JobStatus,
    Schedule,
    ScheduleFire,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookEvent,
)

# Namespace for pg_advisory_xact_lock(classid, hash) so dedupe locks never
# collide with tenant concurrency locks (which use single-arg hashtext).
_DEDUPE_LOCK_CLASS = 884_422_01

_JOB_COLUMNS = """
    id, tenant_id, queue_name, payload, status, priority, attempts,
    max_attempts, visibility_timeout_seconds, max_runtime_seconds,
    retry_backoff_base_seconds, retry_backoff_max_seconds, available_at,
    dedupe_key, schedule_id, depends_on, leased_by, lease_token,
    lease_expires_at, lease_started_at, created_at, updated_at, completed_at,
    result, last_error
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
        max_runtime_seconds=(
            3600
            if row.get("max_runtime_seconds") is None
            else int(row["max_runtime_seconds"])
        ),
        retry_backoff_base_seconds=(
            1
            if row.get("retry_backoff_base_seconds") is None
            else int(row["retry_backoff_base_seconds"])
        ),
        retry_backoff_max_seconds=(
            300
            if row.get("retry_backoff_max_seconds") is None
            else int(row["retry_backoff_max_seconds"])
        ),
        available_at=row["available_at"],
        dedupe_key=row["dedupe_key"],
        schedule_id=row["schedule_id"],
        depends_on=tuple(deps),
        leased_by=row["leased_by"],
        lease_token=row["lease_token"],
        lease_expires_at=row["lease_expires_at"],
        lease_started_at=row.get("lease_started_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        result=row.get("result"),
        last_error=row["last_error"],
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_retry_backoff_seconds(
    attempts: int,
    *,
    base_seconds: int = 1,
    max_seconds: int = 300,
) -> int:
    """Exponential delay after a failed attempt: base * 2^(attempts-1), capped.

    ``attempts`` is the lease attempt count after the failure (typically >= 1).
    """
    if base_seconds < 0 or max_seconds < 0:
        raise ValueError("backoff seconds must be >= 0")
    if attempts < 1:
        attempts = 1
    # Cap exponent to avoid huge ints before min().
    exp = min(attempts - 1, 30)
    delay = base_seconds * (2**exp)
    return int(min(max_seconds, delay))


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


class MergeError(Exception):
    """Raised when a tenant ownership transfer cannot complete."""


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
    # Tenant concurrency limits + pause / metrics
    # ------------------------------------------------------------------

    def set_concurrency_limit(
        self,
        tenant_id: str,
        max_concurrent: int | None,
    ) -> None:
        """Set how many jobs may be leased at once for *tenant_id*.

        Pass ``None`` to clear the limit (unlimited). Does not clear *paused*.
        ``max_concurrent`` must be >= 1 when set.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1 or None")

        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, %s, NULL, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET max_concurrent_leases = EXCLUDED.max_concurrent_leases,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, max_concurrent, now),
            )
            # Drop empty settings rows (no limits, not paused, not offboarding).
            if max_concurrent is None:
                self._delete_empty_tenant_settings(conn, tenant_id)

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

    def set_pending_limit(
        self,
        tenant_id: str,
        max_pending: int | None,
    ) -> None:
        """Set how many *pending* jobs a tenant may hold in the queue.

        When the cap is reached, further ``enqueue`` (and schedule ticks /
        dead-letter replay that would create a new pending row) fail with
        ``ValueError``. Pass ``None`` to clear (unlimited). Does not clear
        *paused* / concurrency / offboarding. ``max_pending`` must be >= 1
        when set.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_pending is not None and max_pending < 1:
            raise ValueError("max_pending must be >= 1 or None")

        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, %s, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET max_pending_jobs = EXCLUDED.max_pending_jobs,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, max_pending, now),
            )
            if max_pending is None:
                self._delete_empty_tenant_settings(conn, tenant_id)

    def get_pending_limit(self, tenant_id: str) -> int | None:
        """Return the tenant's max pending jobs, or None if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._read_pending_limit(conn, tenant_id)

    def pause_tenant(self, tenant_id: str) -> None:
        """Pause a tenant: no new leases, no schedule fires, no reclaim.

        * Workers (scoped or fair) will not lease pending jobs for this tenant.
        * Expired leases for this tenant will not be reclaimed by anyone until
          resume (holders may still heartbeat / ack / nack while the lease lives).
        * Recurring schedules for this tenant are skipped (no tick backlog).
        * Manual enqueue still works; jobs wait as pending until resume.
        * No new webhook deliveries are enqueued; pending webhook rows are not
          claimed/sent until resume.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, TRUE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )

    def resume_tenant(self, tenant_id: str) -> None:
        """Resume leasing and schedule ticks for a paused tenant.

        Overdue schedules are aligned so the next ``process_due_schedules``
        produces at most one job per schedule (no catch-up flood of missed
        ticks from the pause window). Cannot resume a tenant that is
        offboarding.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding and cannot be resumed"
                )
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = FALSE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )
            # Clean schedule pickup: one next fire, not N missed intervals.
            # Schedules that were due during pause keep a single due timestamp
            # of *now* so the next tick enqueues once, then advances normally.
            conn.execute(
                """
                UPDATE schedules
                SET next_run_at = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                  AND enabled = TRUE
                  AND next_run_at < %s
                """,
                (now, now, tenant_id, now),
            )
            self._delete_empty_tenant_settings(conn, tenant_id)
    def is_tenant_paused(self, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._is_tenant_paused(conn, tenant_id)

    def is_tenant_offboarding(self, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._is_tenant_offboarding(conn, tenant_id)

    def offboard_tenant(self, tenant_id: str) -> TenantOffboardResult:
        """Fully offboard a tenant without interrupting in-flight work.

        1. Marks tenant paused + offboarding (no new leases / schedule ticks /
           webhook claims; enqueue is rejected).
        2. Cancels all *pending* jobs (cascade kills pending dependents).
        3. Deletes all schedules.
        4. Deletes *non-claimed* webhook deliveries and any endpoints that no
           longer have a ``delivering`` claim. Already-claimed (``delivering``)
           deliveries — and the endpoints they reference — are kept so agents
           can still POST and ``finalize_webhook_delivery``.
        5. Purges all non-``leased`` job rows (history / cancelled / dead).
        6. Leaves *leased* jobs running and *delivering* webhooks in flight;
           when the last of both ends, remaining rows and settings are
           auto-purged so the tenant is empty.

        Returns a :class:`~job_queue.models.TenantOffboardResult` with counts
        and how much in-flight work still needs to finish.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, TRUE, TRUE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    offboarding = TRUE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )

            # Cancel pending only — leave leased running.
            pending_rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                FOR UPDATE
                """,
                (tenant_id, JobStatus.PENDING.value),
            ).fetchall()
            pending_cancelled = 0
            for prow in pending_rows:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        last_error = COALESCE(last_error, 'tenant offboarded'),
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status = %s
                    RETURNING id
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        now,
                        prow["id"],
                        tenant_id,
                        JobStatus.PENDING.value,
                    ),
                ).fetchone()
                if updated is None:
                    continue
                pending_cancelled += 1
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=prow["id"],
                    tenant_id=tenant_id,
                    reason=f"parent job {prow['id']} cancelled (tenant offboard)",
                    now=now,
                )

            sched_del = conn.execute(
                """
                DELETE FROM schedules
                WHERE tenant_id = %s
                RETURNING id
                """,
                (tenant_id,),
            ).fetchall()

            # Keep claimed (delivering) webhook rows so agents can finalize.
            # Endpoints are kept only while they still have a delivering claim
            # (FK ON DELETE CASCADE would otherwise wipe those claims).
            conn.execute(
                """
                DELETE FROM webhook_deliveries
                WHERE tenant_id = %s
                  AND status <> 'delivering'
                """,
                (tenant_id,),
            )
            wh_del = conn.execute(
                """
                DELETE FROM webhook_endpoints e
                WHERE e.tenant_id = %s
                  AND NOT EXISTS (
                        SELECT 1
                        FROM webhook_deliveries d
                        WHERE d.endpoint_id = e.id
                          AND d.status = 'delivering'
                  )
                RETURNING e.id
                """,
                (tenant_id,),
            ).fetchall()
            # Disable any remaining endpoints (still referenced by claims).
            conn.execute(
                """
                UPDATE webhook_endpoints
                SET enabled = FALSE,
                    updated_at = %s
                WHERE tenant_id = %s
                  AND enabled = TRUE
                """,
                (now, tenant_id),
            )

            purged = conn.execute(
                """
                DELETE FROM jobs
                WHERE tenant_id = %s
                  AND status <> %s
                RETURNING id
                """,
                (tenant_id, JobStatus.LEASED.value),
            ).fetchall()

            leased_n = self._count_leased(conn, tenant_id)
            delivering_n = self._count_webhook_delivering(conn, tenant_id)

            empty = False
            if leased_n == 0 and delivering_n == 0:
                self._finalize_offboard_locked(conn, tenant_id=tenant_id, now=now)
                empty = True

            return TenantOffboardResult(
                tenant_id=tenant_id,
                pending_cancelled=pending_cancelled,
                schedules_deleted=len(sched_del),
                webhooks_deleted=len(wh_del),
                jobs_purged=len(purged),
                leased_remaining=leased_n,
                webhooks_delivering_remaining=delivering_n,
                offboarding=not empty,
                empty=empty,
            )

    def merge_tenant(
        self,
        source_tenant_id: str,
        target_tenant_id: str,
    ) -> TenantMergeResult:
        """Transfer ownership of *source* into *target* without stopping workers.

        Moves **all** of the source tenant's rows to the target in one
        transaction:

        * jobs (pending, leased/running, and terminal history)
        * schedules
        * webhook endpoints and deliveries (including ``delivering``)

        Lease fencing tokens and ``leased_by`` are preserved so in-flight
        workers keep heartbeating / acking. Heartbeat / ack / nack accept the
        pre-merge ``tenant_id`` when the lease token still matches (ops lock
        by job id + token, not the caller's tenant string).

        Conflict handling (so unique indexes cannot fail the transfer):

        * Active ``dedupe_key`` collisions with the target → clear the
          *source* job's dedupe key (job itself still moves).
        * Schedule ``name`` collisions → rename source schedule to
          ``{name}__from_{source_tenant_id}``.

        Source is blocked for new work (paused + offboarding) for the
        duration of the transfer, then its settings / fairness rows are
        deleted. Target settings (pause, caps, offboarding) are unchanged.
        Refuses to merge into an offboarding target, or if the target's
        pending-job cap would be exceeded by the combined pending set.

        After a successful call, ``tenant_is_empty(source)`` is True.
        """
        if not source_tenant_id or not target_tenant_id:
            raise ValueError("source_tenant_id and target_tenant_id are required")
        if source_tenant_id == target_tenant_id:
            raise ValueError("source_tenant_id and target_tenant_id must differ")

        now = _utcnow()
        source = source_tenant_id
        target = target_tenant_id

        with transaction(self._pool) as conn:
            # Serialize with enqueue / pending-cap / other merges for both
            # tenants (ordered lock acquisition avoids deadlocks).
            for tid in sorted((source, target)):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (tid,),
                )

            if self._is_tenant_offboarding(conn, target):
                raise MergeError(
                    f"cannot merge into tenant {target!r}: target is offboarding"
                )

            # Block concurrent source enqueue / schedule / webhook registration.
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, TRUE, TRUE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    offboarding = TRUE,
                    updated_at = EXCLUDED.updated_at
                """,
                (source, now),
            )

            # Pending-cap: refuse if target would exceed after absorbing source.
            target_limit = self._read_pending_limit(conn, target)
            if target_limit is not None:
                source_pending = self._count_pending(conn, source)
                target_pending = self._count_pending(conn, target)
                if source_pending + target_pending > target_limit:
                    raise MergeError(
                        f"cannot merge tenant {source!r} into {target!r}: "
                        f"pending job cap would be exceeded "
                        f"({source_pending}+{target_pending} > {target_limit}); "
                        f"raise or clear the target pending limit first"
                    )

            # Resolve active dedupe collisions (prefer target's active key).
            dedupe_cleared = conn.execute(
                """
                UPDATE jobs j
                SET dedupe_key = NULL,
                    updated_at = %s
                WHERE j.tenant_id = %s
                  AND j.dedupe_key IS NOT NULL
                  AND j.status IN ('pending', 'leased')
                  AND EXISTS (
                        SELECT 1
                        FROM jobs t
                        WHERE t.tenant_id = %s
                          AND t.queue_name = j.queue_name
                          AND t.dedupe_key = j.dedupe_key
                          AND t.status IN ('pending', 'leased')
                  )
                RETURNING j.id
                """,
                (now, source, target),
            ).fetchall()

            # Resolve schedule name collisions (unique per tenant when set).
            names_renamed = conn.execute(
                """
                UPDATE schedules s
                SET name = s.name || %s,
                    updated_at = %s
                WHERE s.tenant_id = %s
                  AND s.name IS NOT NULL
                  AND EXISTS (
                        SELECT 1
                        FROM schedules t
                        WHERE t.tenant_id = %s
                          AND t.name = s.name
                  )
                RETURNING s.id
                """,
                (f"__from_{source}", now, source, target),
            ).fetchall()

            jobs_moved = conn.execute(
                """
                UPDATE jobs
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id, status
                """,
                (target, now, source),
            ).fetchall()
            leased_moved = sum(
                1 for r in jobs_moved if r["status"] == JobStatus.LEASED.value
            )

            sched_moved = conn.execute(
                """
                UPDATE schedules
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()

            # Endpoints before deliveries is fine (no tenant FK); both move.
            ep_moved = conn.execute(
                """
                UPDATE webhook_endpoints
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()
            del_moved = conn.execute(
                """
                UPDATE webhook_deliveries
                SET tenant_id = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                RETURNING id
                """,
                (target, now, source),
            ).fetchall()

            # Drop source control-plane state (settings + fairness).
            conn.execute(
                "DELETE FROM tenant_settings WHERE tenant_id = %s",
                (source,),
            )
            conn.execute(
                "DELETE FROM tenant_fairness WHERE tenant_id = %s",
                (source,),
            )

            if not self._tenant_is_empty(conn, source):
                # Should be unreachable; surface loudly if a new table is added.
                raise MergeError(
                    f"merge left residual state on source tenant {source!r}"
                )

            return TenantMergeResult(
                source_tenant_id=source,
                target_tenant_id=target,
                jobs_moved=len(jobs_moved),
                jobs_leased_moved=leased_moved,
                schedules_moved=len(sched_moved),
                webhook_endpoints_moved=len(ep_moved),
                webhook_deliveries_moved=len(del_moved),
                dedupe_keys_cleared=len(dedupe_cleared),
                schedule_names_renamed=len(names_renamed),
                source_empty=True,
            )

    def tenant_is_empty(self, tenant_id: str) -> bool:
        """True when tenant has no jobs, schedules, webhooks, or settings."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._tenant_is_empty(conn, tenant_id)

    def get_tenant_stats(self, tenant_id: str) -> TenantStats:
        """Return job counts and control flags for one tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with self._pool.connection() as conn:
            return self._load_tenant_stats(conn, tenant_id, now=now)

    def list_tenant_stats(self) -> list[TenantStats]:
        """Stats for every tenant that has jobs and/or settings."""
        now = _utcnow()
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT tenant_id FROM jobs
                UNION
                SELECT tenant_id FROM tenant_settings
                ORDER BY tenant_id
                """
            ).fetchall()
            return [
                self._load_tenant_stats(conn, r["tenant_id"], now=now) for r in rows
            ]

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
        max_runtime_seconds: int = 3600,
        retry_backoff_base_seconds: int = 1,
        retry_backoff_max_seconds: int = 300,
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
        max_runtime_seconds:
            Hard wall-clock limit for one lease attempt (from
            ``lease_started_at``). Heartbeats cannot extend past this; on
            exceed the job is force-released (requeued or dead-lettered).
        retry_backoff_base_seconds / retry_backoff_max_seconds:
            On nack requeue, delay is ``base * 2^(attempt-1)`` seconds, capped
            by max. After ``max_attempts`` failures the job is dead-lettered.
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

                # New pending row: enforce per-tenant pending cap.
                self._assert_pending_capacity(conn, tenant_id)

                # ON CONFLICT remains a safety net for any missed paths.
                row = conn.execute(
                    f"""
                    INSERT INTO jobs (
                        id, tenant_id, queue_name, payload, status, priority,
                        attempts, max_attempts, visibility_timeout_seconds,
                        max_runtime_seconds,
                        retry_backoff_base_seconds, retry_backoff_max_seconds,
                        available_at, dedupe_key, schedule_id, depends_on,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        0, %s, %s,
                        %s,
                        %s, %s,
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
                        max_runtime_seconds,
                        retry_backoff_base_seconds,
                        retry_backoff_max_seconds,
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

            self._assert_pending_capacity(conn, tenant_id)

            row = conn.execute(
                f"""
                INSERT INTO jobs (
                    id, tenant_id, queue_name, payload, status, priority,
                    attempts, max_attempts, visibility_timeout_seconds,
                    max_runtime_seconds,
                    retry_backoff_base_seconds, retry_backoff_max_seconds,
                    available_at, dedupe_key, schedule_id, depends_on,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    0, %s, %s,
                    %s,
                    %s, %s,
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
                    max_runtime_seconds,
                    retry_backoff_base_seconds,
                    retry_backoff_max_seconds,
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
        """Claim the next available job (batch size 1).

        See :meth:`lease_batch` for multi-job pulls and full semantics.
        """
        jobs = self.lease_batch(
            worker_id=worker_id,
            limit=1,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        return jobs[0] if jobs else None

    def lease_batch(
        self,
        *,
        worker_id: str,
        limit: int = 1,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> list[Job]:
        """Claim up to *limit* available jobs in one pull.

        Pass *tenant_id* to lease only that tenant's work. Omit *tenant_id* for
        **fair multi-tenant** leasing with **priority lanes**: higher priority
        first; within a lane tenants rotate by least-recently-served so one
        noisy tenant cannot monopolize the batch.

        Available means:
          * status = pending AND available_at <= now() AND all ``depends_on``
            parents are ``completed``, OR
          * status = leased AND lease_expires_at < now()  (orphaned lease)
            AND attempts < max_attempts

        Delayed pending jobs, unmet dependencies, and per-tenant concurrency
        caps are always enforced. Paused tenants yield no new leases (in-flight
        work may still complete). Expired leases that already used all
        attempts are dead-lettered first (dependents cascade-cancelled).

        Uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never claim the
        same row. Each job gets a fresh ``lease_token`` for fencing. Returns
        fewer than *limit* jobs when the queue is short (possibly empty list).
        """
        if not worker_id:
            raise ValueError("worker_id is required")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if limit > 500:
            raise ValueError("limit must be <= 500")

        if tenant_id is None:
            return self._lease_fair_batch(
                worker_id=worker_id,
                queue_name=queue_name,
                visibility_timeout_seconds=visibility_timeout_seconds,
                limit=limit,
            )
        return self._lease_for_tenant_batch(
            tenant_id=tenant_id,
            worker_id=worker_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
            limit=limit,
        )

    def _lease_for_tenant_batch(
        self,
        *,
        tenant_id: str,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        limit: int,
    ) -> list[Job]:
        now = _utcnow()
        claimed: list[Job] = []

        with transaction(self._pool) as conn:
            # Serialize lease decisions for this tenant so concurrency caps
            # cannot be exceeded by racing workers.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )

            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=tenant_id)
            # Offboarding: cancel abandoned expired leases so the tenant can empty.
            self._reap_offboarding_expired_leases(
                conn, now=now, tenant_id=tenant_id
            )

            # Paused: no new claims (pending or reclaim). In-flight holders
            # still use heartbeat/ack/nack outside this path.
            if self._is_tenant_paused(conn, tenant_id):
                return []

            conc_limit = self._read_concurrency_limit(conn, tenant_id)
            leased_count = self._count_leased(conn, tenant_id)

            if conc_limit is not None and leased_count >= conc_limit:
                # Only reclaim expired leases (does not increase concurrent
                # holders beyond the cap in steady state).
                candidates = conn.execute(
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
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.LEASED.value,
                        now,
                        limit,
                    ),
                ).fetchall()
            else:
                take = limit
                if conc_limit is not None:
                    take = min(limit, conc_limit - leased_count)
                    take = max(take, 0)
                if take < 1:
                    return []
                candidates = conn.execute(
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
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        queue_name,
                        JobStatus.PENDING.value,
                        now,
                        JobStatus.LEASED.value,
                        now,
                        take,
                    ),
                ).fetchall()

            for candidate in candidates:
                claimed.append(
                    self._claim_locked_job(
                        conn,
                        candidate,
                        worker_id=worker_id,
                        token=uuid4(),
                        visibility_timeout_seconds=visibility_timeout_seconds,
                        now=now,
                    )
                )
        return claimed

    def _lease_fair_batch(
        self,
        *,
        worker_id: str,
        queue_name: str,
        visibility_timeout_seconds: int | None,
        limit: int,
        _max_skips: int | None = None,
    ) -> list[Job]:
        """Lease up to *limit* jobs fairly across tenants for *queue_name*.

        Each claim updates ``tenant_fairness.last_lease_at`` so subsequent
        jobs in the same batch rotate tenants within a priority lane.
        """
        now = _utcnow()
        claimed: list[Job] = []
        # Allow several skip attempts per desired job (paused / at-cap tenants).
        max_skips = _max_skips if _max_skips is not None else max(64, limit * 16)
        skips = 0

        with transaction(self._pool) as conn:
            self._dead_letter_exhausted_leases(conn, now=now, tenant_id=None)
            self._reap_offboarding_expired_leases(conn, now=now, tenant_id=None)

            while len(claimed) < limit and skips < max_skips:
                try:
                    # Savepoint so a rejected candidate releases its row lock.
                    with conn.transaction():
                        job = self._try_claim_fair_candidate(
                            conn,
                            worker_id=worker_id,
                            queue_name=queue_name,
                            visibility_timeout_seconds=visibility_timeout_seconds,
                            now=now,
                        )
                        if job is None:
                            break
                        claimed.append(job)
                except _SkipLeaseCandidate:
                    skips += 1
                    continue
        return claimed

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
        # Priority lanes first (higher priority always before lower), then
        # least-recently-served tenant within that lane (never-served =
        # -infinity). A noisy tenant's high-priority flood still rotates with
        # other tenants at the same priority; it cannot leapfrog higher-priority
        # work of quieter tenants, and low-priority noise cannot starve high.
        candidate = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE queue_name = %s
              AND NOT EXISTS (
                    SELECT 1
                    FROM tenant_settings ts
                    WHERE ts.tenant_id = jobs.tenant_id
                      AND ts.paused = TRUE
              )
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
              priority DESC,
              COALESCE(
                (
                    SELECT tf.last_lease_at
                    FROM tenant_fairness tf
                    WHERE tf.tenant_id = jobs.tenant_id
                ),
                TIMESTAMPTZ '-infinity'
              ) ASC,
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
            ),
        ).fetchone()

        if candidate is None:
            return None

        # Re-check pause + concurrency under tenant lock (closes races).
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (candidate["tenant_id"],),
        )
        if self._is_tenant_paused(conn, candidate["tenant_id"]):
            raise _SkipLeaseCandidate("tenant paused")
        if candidate["status"] == JobStatus.PENDING.value:
            limit = self._read_concurrency_limit(conn, candidate["tenant_id"])
            leased_count = self._count_leased(conn, candidate["tenant_id"])
            if limit is not None and leased_count >= limit:
                raise _SkipLeaseCandidate("tenant at concurrency limit")

        claimed = self._claim_locked_job(
            conn,
            candidate,
            worker_id=worker_id,
            token=token,
            visibility_timeout_seconds=visibility_timeout_seconds,
            now=now,
        )
        # Record that this tenant was just served (drives next fair pick).
        # Use clock_timestamp() so successive claims in the same batch get
        # distinct last_lease_at values and rotate tenants correctly.
        conn.execute(
            """
            INSERT INTO tenant_fairness (tenant_id, last_lease_at)
            VALUES (%s, clock_timestamp())
            ON CONFLICT (tenant_id) DO UPDATE
            SET last_lease_at = clock_timestamp()
            """,
            (claimed.tenant_id,),
        )
        return claimed

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
        max_rt = int(candidate.get("max_runtime_seconds") or 3600)
        lease_expires = now + timedelta(seconds=vt)
        hard = now + timedelta(seconds=max_rt)
        if lease_expires > hard:
            lease_expires = hard

        row = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = %s,
                lease_token = %s,
                lease_expires_at = %s,
                lease_started_at = %s,
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
                now,
                new_attempts,
                vt,
                now,
                candidate["id"],
                candidate["tenant_id"],
            ),
        ).fetchone()
        assert row is not None
        return _row_to_job(row)

    def _hard_deadline(self, row: dict[str, Any]) -> datetime | None:
        started = row.get("lease_started_at")
        if started is None:
            return None
        max_rt = row.get("max_runtime_seconds")
        if max_rt is None:
            max_rt = 3600
        return _as_utc(started) + timedelta(seconds=int(max_rt))

    def _release_max_runtime_exceeded(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        """Force-release a stuck lease that exceeded max_runtime_seconds."""
        attempts = int(row["attempts"])
        max_attempts = int(row["max_attempts"])
        error = "max runtime exceeded"
        tenant_id = row["tenant_id"]
        # Offboarding tenants must not regain pending work after a force-release.
        force_terminal = self._is_tenant_offboarding(conn, tenant_id)
        if attempts >= max_attempts or force_terminal:
            terminal = (
                JobStatus.CANCELLED.value
                if force_terminal and attempts < max_attempts
                else JobStatus.DEAD.value
            )
            if force_terminal and attempts < max_attempts:
                error = "max runtime exceeded during tenant offboard"
            conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    completed_at = CASE WHEN %s = %s THEN %s ELSE completed_at END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                """,
                (
                    terminal,
                    error,
                    terminal,
                    JobStatus.DEAD.value,
                    now,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            )
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tenant_id,
                reason=f"parent job {row['id']} ended as {terminal} (max runtime)",
                now=now,
            )
            dead_row = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (row["id"],),
            ).fetchone()
            if dead_row is not None and terminal == JobStatus.DEAD.value:
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(dead_row),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
            self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
        else:
            base = row.get("retry_backoff_base_seconds")
            cap = row.get("retry_backoff_max_seconds")
            backoff = compute_retry_backoff_seconds(
                attempts,
                base_seconds=1 if base is None else int(base),
                max_seconds=300 if cap is None else int(cap),
            )
            next_available = now + timedelta(seconds=backoff)
            conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    available_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                """,
                (
                    JobStatus.PENDING.value,
                    error,
                    next_available,
                    now,
                    row["id"],
                    tenant_id,
                    JobStatus.LEASED.value,
                ),
            )

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

        tenants: set[str] = set()
        for dead in dead_rows:
            tid = dead["tenant_id"]
            tenants.add(tid)
            self._cascade_cancel_dependents(
                conn,
                parent_id=dead["id"],
                tenant_id=tid,
                reason=f"parent job {dead['id']} dead-lettered",
                now=now,
            )
            full = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (dead["id"],),
            ).fetchone()
            if full is not None:
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(full),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

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
            if job.status not in (JobStatus.PENDING, JobStatus.LEASED):
                raise CancelError(
                    f"job {job_id} cannot be cancelled "
                    f"(status={job.status.value}; only pending or leased jobs cancel)",
                    job=job,
                )

            was_running = job.status == JobStatus.LEASED
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = CASE
                        WHEN %s THEN 'cancelled while running'
                        ELSE last_error
                    END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status IN (%s, %s)
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.CANCELLED.value,
                    was_running,
                    now,
                    job_id,
                    tenant_id,
                    JobStatus.PENDING.value,
                    JobStatus.LEASED.value,
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
            self._cascade_cancel_dependents(
                conn,
                parent_id=job_id,
                tenant_id=tenant_id,
                reason=f"parent job {job_id} cancelled",
                now=now,
            )
            self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
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

        Requires matching worker and fencing token. *tenant_id* is accepted for
        API compatibility; after a mid-flight ``merge_tenant`` the caller's
        tenant may differ from the row (lock is by job id + token). Raises
        ``LeaseError`` if the lease was reclaimed, expired, completed, or the
        hard ``max_runtime_seconds`` deadline has been exceeded (the job is
        force-released so a stuck worker cannot heartbeat forever).
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
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
        via ``get`` / ``get_result``. *tenant_id* may be the pre-merge tenant
        after ownership transfer; fencing is the lease token.
        """
        now = _utcnow()
        result_json = Jsonb(result) if result is not None else None

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
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
                self._enqueue_job_webhooks(
                    conn, job=job, event=WebhookEvent.JOB_COMPLETED, now=now
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
        are cascade-cancelled. *tenant_id* may be the pre-merge tenant after
        ownership transfer; fencing is the lease token.
        """
        now = _utcnow()

        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
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

            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    available_at = %s,
                    last_error = %s,
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
                    now,
                    job_id,
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

        After ``merge_tenant``, *tenant_id* may be the pre-merge value; the
        lease token is the source of truth.
        """
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = self._lock_job_for_lease(conn, job_id, tenant_id=tenant_id)
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
            except ValueError as exc:
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
    # Webhooks (durable outbox)
    # ------------------------------------------------------------------

    _WEBHOOK_ENDPOINT_COLUMNS = """
        id, tenant_id, url, secret, events, enabled, max_attempts,
        backoff_base_seconds, backoff_max_seconds, created_at, updated_at
    """

    _WEBHOOK_DELIVERY_COLUMNS = """
        id, endpoint_id, tenant_id, job_id, event, payload, status, attempts,
        max_attempts, backoff_base_seconds, backoff_max_seconds, next_attempt_at,
        delivering_until, last_error, last_status_code, created_at, updated_at,
        completed_at
    """

    def register_webhook(
        self,
        *,
        tenant_id: str,
        url: str,
        events: Sequence[str] | None = None,
        secret: str | None = None,
        max_attempts: int = 5,
        backoff_base_seconds: int = 1,
        backoff_max_seconds: int = 300,
        enabled: bool = True,
        endpoint_id: UUID | None = None,
    ) -> WebhookEndpoint:
        """Register an HTTP webhook endpoint for terminal job events."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not url or not url.startswith(("http://", "https://")):
            raise ValueError("url must be an http(s) URL")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if backoff_base_seconds < 0 or backoff_max_seconds < 0:
            raise ValueError("backoff seconds must be >= 0")

        event_list = list(events) if events is not None else [
            WebhookEvent.JOB_COMPLETED.value,
            WebhookEvent.JOB_FAILED.value,
            WebhookEvent.JOB_DEAD.value,
        ]
        allowed = {e.value for e in WebhookEvent}
        for ev in event_list:
            if ev not in allowed:
                raise ValueError(f"unsupported webhook event {ev!r}")
        if not event_list:
            raise ValueError("events must be non-empty")

        now = _utcnow()
        eid = endpoint_id or uuid4()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; "
                    "webhook registration is rejected"
                )
            row = conn.execute(
                f"""
                INSERT INTO webhook_endpoints (
                    id, tenant_id, url, secret, events, enabled, max_attempts,
                    backoff_base_seconds, backoff_max_seconds, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                RETURNING {self._WEBHOOK_ENDPOINT_COLUMNS}
                """,
                (
                    eid,
                    tenant_id,
                    url,
                    secret,
                    event_list,
                    enabled,
                    max_attempts,
                    backoff_base_seconds,
                    backoff_max_seconds,
                    now,
                    now,
                ),
            ).fetchone()
            assert row is not None
            return self._row_to_webhook_endpoint(row)

    def list_webhooks(self, *, tenant_id: str) -> list[WebhookEndpoint]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {self._WEBHOOK_ENDPOINT_COLUMNS}
                FROM webhook_endpoints
                WHERE tenant_id = %s
                ORDER BY created_at ASC
                """,
                (tenant_id,),
            ).fetchall()
            return [self._row_to_webhook_endpoint(r) for r in rows]

    def delete_webhook(self, endpoint_id: UUID, *, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM webhook_endpoints
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (endpoint_id, tenant_id),
            ).fetchone()
            return row is not None

    def set_webhook_enabled(
        self, endpoint_id: UUID, *, tenant_id: str, enabled: bool
    ) -> WebhookEndpoint:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE webhook_endpoints
                SET enabled = %s, updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {self._WEBHOOK_ENDPOINT_COLUMNS}
                """,
                (enabled, now, endpoint_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ValueError(
                    f"webhook {endpoint_id} not found for tenant {tenant_id!r}"
                )
            return self._row_to_webhook_endpoint(row)

    def list_webhook_deliveries(
        self,
        *,
        tenant_id: str,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[WebhookDelivery]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if status is not None:
            clauses.append("status = %s")
            params.append(status)
        params.extend([limit, offset])
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [self._row_to_webhook_delivery(r) for r in rows]

    def get_webhook_delivery(
        self, delivery_id: UUID, *, tenant_id: str
    ) -> WebhookDelivery | None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE id = %s AND tenant_id = %s
                """,
                (delivery_id, tenant_id),
            ).fetchone()
            return self._row_to_webhook_delivery(row) if row else None

    def claim_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        claim_seconds: float | None = None,
        http_timeout_seconds: float = 10.0,
    ) -> tuple[list[Any], list[Any]]:
        """Claim due webhook deliveries without performing HTTP POSTs.

        Returns ``(claimed, auto_finalized)`` where *claimed* is a list of
        :class:`~job_queue.models.ClaimedWebhook` ready for the agent to POST,
        and *auto_finalized* are :class:`~job_queue.webhooks.WebhookDeliveryResult`
        for cases that need no HTTP (e.g. reclaim with proven 2xx last_status_code).

        Claim commits ``status=delivering`` before return so a crash mid-POST
        cannot leave the row as pending.

        Hot path uses a **set-based** ``UPDATE ... FROM (SELECT ... FOR UPDATE
        SKIP LOCKED LIMIT n)`` plus a single join to endpoints so large pending
        backlogs do not N-round-trip per row.
        """
        from job_queue.models import ClaimedWebhook
        from job_queue.webhooks import WebhookDeliveryResult

        if limit < 1:
            raise ValueError("limit must be >= 1")

        lease_s = claim_seconds if claim_seconds is not None else max(
            30.0, float(http_timeout_seconds) * 3.0
        )
        now = _utcnow()
        claim_until = now + timedelta(seconds=lease_s)

        claimed: list[ClaimedWebhook] = []
        auto_finalized: list[Any] = []

        with transaction(self._pool) as conn:
            # Cheap gate: only scan offboarding reaper when needed.
            has_offboard = conn.execute(
                """
                SELECT 1 FROM tenant_settings
                WHERE offboarding = TRUE
                LIMIT 1
                """
            ).fetchone()
            if has_offboard is not None:
                self._reap_offboarding_expired_webhooks(
                    conn, now=now, tenant_id=None
                )

            # 1) Auto-succeed expired delivering claims that already have 2xx
            #    evidence (crash after POST, before status flip).
            auto_rows = conn.execute(
                f"""
                UPDATE webhook_deliveries d
                SET status = 'succeeded',
                    delivering_until = NULL,
                    last_error = NULL,
                    updated_at = %s,
                    completed_at = COALESCE(d.completed_at, %s)
                WHERE d.id IN (
                    SELECT d2.id
                    FROM webhook_deliveries d2
                    WHERE d2.status = 'delivering'
                      AND d2.delivering_until IS NOT NULL
                      AND d2.delivering_until < %s
                      AND d2.last_status_code IS NOT NULL
                      AND d2.last_status_code >= 200
                      AND d2.last_status_code < 300
                      AND NOT EXISTS (
                            SELECT 1 FROM tenant_settings ts
                            WHERE ts.tenant_id = d2.tenant_id
                              AND ts.paused = TRUE
                              AND ts.offboarding = FALSE
                      )
                    FOR UPDATE OF d2 SKIP LOCKED
                    LIMIT %s
                )
                RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                """,
                (now, now, now, limit),
            ).fetchall()
            for u in auto_rows:
                auto_finalized.append(
                    WebhookDeliveryResult(
                        delivery=self._row_to_webhook_delivery(u),
                        success=True,
                        status_code=int(u["last_status_code"]),
                        error=None,
                    )
                )

            remaining = max(0, limit - len(auto_rows))
            if remaining < 1:
                return claimed, auto_finalized

            # 2) Set-based claim of pending + stuck delivering (no 2xx evidence).
            #    Also dead-letter pending rows that already exhausted attempts.
            claimed_rows = conn.execute(
                f"""
                WITH due AS (
                    SELECT d.id, d.status, d.attempts, d.max_attempts
                    FROM webhook_deliveries d
                    WHERE (
                            (
                                d.status = 'pending'
                            AND d.next_attempt_at <= %s
                            )
                         OR (
                                d.status = 'delivering'
                            AND d.delivering_until IS NOT NULL
                            AND d.delivering_until < %s
                            AND (
                                d.last_status_code IS NULL
                             OR d.last_status_code < 200
                             OR d.last_status_code >= 300
                            )
                         )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = d.tenant_id
                          AND ts.paused = TRUE
                    )
                    ORDER BY d.next_attempt_at ASC
                    FOR UPDATE OF d SKIP LOCKED
                    LIMIT %s
                ),
                updated AS (
                    UPDATE webhook_deliveries d
                    SET
                        status = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN 'dead'
                            ELSE 'delivering'
                        END,
                        attempts = CASE
                            WHEN d.status = 'pending'
                            THEN d.attempts + 1
                            ELSE d.attempts
                        END,
                        delivering_until = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN NULL
                            ELSE %s
                        END,
                        last_error = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN COALESCE(d.last_error, 'max attempts exceeded')
                            WHEN d.status = 'delivering'
                            THEN COALESCE(
                                d.last_error,
                                'reclaimed stuck delivering claim'
                            )
                            ELSE d.last_error
                        END,
                        completed_at = CASE
                            WHEN d.status = 'pending'
                                 AND d.attempts + 1 > d.max_attempts
                            THEN %s
                            ELSE d.completed_at
                        END,
                        updated_at = %s
                    FROM due
                    WHERE d.id = due.id
                    RETURNING
                        d.id, d.endpoint_id, d.tenant_id, d.job_id, d.event,
                        d.payload, d.status, d.attempts, d.max_attempts,
                        d.backoff_base_seconds, d.backoff_max_seconds,
                        d.next_attempt_at, d.delivering_until, d.last_error,
                        d.last_status_code, d.created_at, d.updated_at,
                        d.completed_at
                )
                SELECT
                    u.*,
                    e.url AS _ep_url,
                    e.secret AS _ep_secret,
                    e.enabled AS _ep_enabled
                FROM updated u
                LEFT JOIN webhook_endpoints e ON e.id = u.endpoint_id
                """,
                (now, now, remaining, claim_until, now, now),
            ).fetchall()

            for row in claimed_rows:
                if row["status"] == "dead":
                    auto_finalized.append(
                        WebhookDeliveryResult(
                            delivery=self._row_to_webhook_delivery(row),
                            success=False,
                            status_code=None,
                            error="max attempts exceeded",
                        )
                    )
                    continue
                delivery = self._row_to_webhook_delivery(row)
                url = row.get("_ep_url")
                secret = row.get("_ep_secret")
                enabled = row.get("_ep_enabled")
                if url is None:
                    claimed.append(
                        ClaimedWebhook(
                            delivery=delivery,
                            url=None,
                            secret=None,
                            endpoint_enabled=False,
                        )
                    )
                else:
                    claimed.append(
                        ClaimedWebhook(
                            delivery=delivery,
                            url=url,
                            secret=secret,
                            endpoint_enabled=bool(enabled),
                        )
                    )

        return claimed, auto_finalized

    def finalize_webhook_delivery(
        self,
        delivery_id: UUID,
        *,
        status_code: int | None = None,
        error: str | None = None,
        endpoint_missing: bool = False,
    ) -> Any | None:
        """Finalize a claimed (delivering) webhook after the agent POSTed (or not).

        On success, persists ``last_status_code`` first so a crash before the
        status flip still prevents double-POST on reclaim.
        Returns ``None`` if the delivery is no longer ``delivering`` (lost race).

        While a tenant is offboarding, failed deliveries are forced terminal
        (``dead``) instead of requeued to ``pending``, then the tenant is
        auto-purged once no leased jobs / delivering webhooks remain.
        """
        from job_queue.webhooks import WebhookDeliveryResult

        now = _utcnow()

        if endpoint_missing:
            with transaction(self._pool) as conn:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_error = %s,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (
                        "endpoint missing or disabled",
                        now,
                        now,
                        delivery_id,
                    ),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(
                    conn, tenant_id=updated["tenant_id"], now=now
                )
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=False,
                    status_code=None,
                    error="endpoint missing or disabled",
                )

        success = status_code is not None and 200 <= int(status_code) < 300
        detail = (error or "")[:2000] if error else None

        # Load attempts / max under lock for retry vs dead decision.
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE id = %s
                FOR UPDATE
                """,
                (delivery_id,),
            ).fetchone()
            if row is None or row["status"] != "delivering":
                return None

            delivery = self._row_to_webhook_delivery(row)
            attempts = delivery.attempts
            tenant_id = delivery.tenant_id
            offboarding = self._is_tenant_offboarding(conn, tenant_id)

            if success:
                # Single UPDATE: persist 2xx evidence and terminal status together.
                # (Crash mid-statement still leaves status=delivering; reclaim
                # auto-succeeds when last_status_code is 2xx.)
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'succeeded',
                        delivering_until = NULL,
                        last_status_code = %s,
                        last_error = NULL,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (int(status_code), now, now, delivery_id),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=True,
                    status_code=int(status_code),
                    error=None,
                )

            # Offboarding: never requeue pending webhook work after a claim.
            force_dead = offboarding or attempts >= delivery.max_attempts
            if force_dead:
                err = detail or "delivery failed"
                if offboarding and attempts < delivery.max_attempts:
                    err = (detail or "delivery failed") + " (tenant offboard)"
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_status_code = %s,
                        last_error = %s,
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                    """,
                    (
                        status_code,
                        err,
                        now,
                        now,
                        delivery_id,
                    ),
                ).fetchone()
                if updated is None:
                    return None
                self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)
                return WebhookDeliveryResult(
                    delivery=self._row_to_webhook_delivery(updated),
                    success=False,
                    status_code=status_code,
                    error=err,
                )

            delay = compute_retry_backoff_seconds(
                attempts,
                base_seconds=delivery.backoff_base_seconds,
                max_seconds=delivery.backoff_max_seconds,
            )
            next_at = now + timedelta(seconds=delay)
            updated = conn.execute(
                f"""
                UPDATE webhook_deliveries
                SET status = 'pending',
                    delivering_until = NULL,
                    last_status_code = %s,
                    last_error = %s,
                    next_attempt_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND status = 'delivering'
                RETURNING {self._WEBHOOK_DELIVERY_COLUMNS}
                """,
                (
                    status_code,
                    detail or "delivery failed",
                    next_at,
                    now,
                    delivery_id,
                ),
            ).fetchone()
            if updated is None:
                return None
            return WebhookDeliveryResult(
                delivery=self._row_to_webhook_delivery(updated),
                success=False,
                status_code=status_code,
                error=detail or "delivery failed",
            )

    def process_webhook_deliveries(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
        on_result: Any = None,
    ) -> list[Any]:
        """Local orchestrator: claim → POST → finalize (tests / in-process).

        Distributed webhook *agents* should call :meth:`claim_webhook_deliveries`
        and :meth:`finalize_webhook_delivery` themselves and never rely on the
        coordinator to POST.
        """
        from job_queue.webhooks import post_webhook

        claimed, auto = self.claim_webhook_deliveries(
            limit=limit,
            claim_seconds=claim_seconds,
            http_timeout_seconds=http_timeout_seconds,
        )
        results: list[Any] = list(auto)
        if on_result is not None:
            for rec in auto:
                on_result(rec)

        for item in claimed:
            delivery = item.delivery
            if not item.endpoint_enabled or not item.url:
                result = self.finalize_webhook_delivery(
                    delivery.id, endpoint_missing=True
                )
                if result is None:
                    continue
                results.append(result)
                if on_result is not None:
                    on_result(result)
                continue

            status_code, detail = post_webhook(
                item.url,
                delivery.payload,
                secret=item.secret,
                timeout_seconds=http_timeout_seconds,
                idempotency_key=str(delivery.id),
            )
            result = self.finalize_webhook_delivery(
                delivery.id,
                status_code=status_code,
                error=None if 200 <= status_code < 300 else detail,
            )
            if result is None:
                continue
            results.append(result)
            if on_result is not None:
                on_result(result)

        return results

    def _enqueue_job_webhooks(
        self,
        conn: Any,
        *,
        job: Job,
        event: WebhookEvent,
        now: datetime,
    ) -> None:
        """Insert pending delivery rows for all matching enabled endpoints.

        No-op when the tenant is paused (no new outbox rows until resume).
        """
        if self._is_tenant_paused(conn, job.tenant_id):
            return

        endpoints = conn.execute(
            f"""
            SELECT {self._WEBHOOK_ENDPOINT_COLUMNS}
            FROM webhook_endpoints
            WHERE tenant_id = %s
              AND enabled = TRUE
              AND %s = ANY(events)
            """,
            (job.tenant_id, event.value),
        ).fetchall()
        if not endpoints:
            return

        for ep in endpoints:
            delivery_id = uuid4()
            payload = {
                "event": event.value,
                "delivery_id": str(delivery_id),
                "job_id": str(job.id),
                "tenant_id": job.tenant_id,
                "queue_name": job.queue_name,
                "status": job.status.value,
                "payload": job.payload,
                "result": job.result,
                "attempts": job.attempts,
                "max_attempts": job.max_attempts,
                "last_error": job.last_error,
                "completed_at": (
                    job.completed_at.isoformat() if job.completed_at else None
                ),
                "timestamp": now.isoformat(),
            }
            conn.execute(
                f"""
                INSERT INTO webhook_deliveries (
                    id, endpoint_id, tenant_id, job_id, event, payload,
                    status, attempts, max_attempts, backoff_base_seconds,
                    backoff_max_seconds, next_attempt_at, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    'pending', 0, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    delivery_id,
                    ep["id"],
                    job.tenant_id,
                    job.id,
                    event.value,
                    Jsonb(payload),
                    ep["max_attempts"],
                    ep["backoff_base_seconds"],
                    ep["backoff_max_seconds"],
                    now,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _row_to_webhook_endpoint(row: dict[str, Any]) -> WebhookEndpoint:
        events = row["events"] or []
        return WebhookEndpoint(
            id=row["id"],
            tenant_id=row["tenant_id"],
            url=row["url"],
            secret=row["secret"],
            events=tuple(events),
            enabled=bool(row["enabled"]),
            max_attempts=int(row["max_attempts"]),
            backoff_base_seconds=int(row["backoff_base_seconds"]),
            backoff_max_seconds=int(row["backoff_max_seconds"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_webhook_delivery(row: dict[str, Any]) -> WebhookDelivery:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return WebhookDelivery(
            id=row["id"],
            endpoint_id=row["endpoint_id"],
            tenant_id=row["tenant_id"],
            job_id=row["job_id"],
            event=row["event"],
            payload=payload,
            status=row["status"],
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
            backoff_base_seconds=int(row["backoff_base_seconds"]),
            backoff_max_seconds=int(row["backoff_max_seconds"]),
            next_attempt_at=row["next_attempt_at"],
            delivering_until=row.get("delivering_until"),
            last_error=row["last_error"],
            last_status_code=row["last_status_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )
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
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; "
                    "schedule registration is rejected"
                )
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

        **Paused tenants are skipped entirely** — no jobs are enqueued for their
        schedules while paused, so ticks do not pile up. After ``resume_tenant``,
        overdue schedules fire once (clean pickup).

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

        # Shared filter: never tick schedules for paused tenants.
        paused_filter = """
            AND NOT EXISTS (
                SELECT 1
                FROM tenant_settings ts
                WHERE ts.tenant_id = schedules.tenant_id
                  AND ts.paused = TRUE
            )
        """

        with transaction(self._pool) as conn:
            if tenant_id is not None:
                if self._is_tenant_paused(conn, tenant_id):
                    return []
                due = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE enabled = TRUE
                      AND next_run_at <= %s
                      AND tenant_id = %s
                      {paused_filter}
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
                      {paused_filter}
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, limit),
                ).fetchall()

            for sched_row in due:
                # Race: tenant paused after SELECT.
                if self._is_tenant_paused(conn, sched_row["tenant_id"]):
                    continue

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
                        try:
                            self._assert_pending_capacity(
                                conn, sched_row["tenant_id"]
                            )
                        except ValueError:
                            # At pending cap: skip this schedule tick (no job).
                            continue
                        job_row = conn.execute(
                            f"""
                            INSERT INTO jobs (
                                id, tenant_id, queue_name, payload, status, priority,
                                attempts, max_attempts, visibility_timeout_seconds,
                                max_runtime_seconds,
                                retry_backoff_base_seconds, retry_backoff_max_seconds,
                                available_at, dedupe_key, schedule_id, depends_on,
                                created_at, updated_at
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                0, %s, %s,
                                3600,
                                1, 300,
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

    def _lock_job_for_lease(
        self,
        conn: Any,
        job_id: UUID,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Lock a job for heartbeat / ack / nack / ensure_lease.

        Locks by job id only so mid-flight ``merge_tenant`` (ownership
        transfer) does not break workers that still send the pre-merge
        tenant_id. Fencing is the lease token + worker_id, not the tenant
        string. *tenant_id* is optional and only used for a clearer error
        when the job is missing entirely.
        """
        row = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            if tenant_id is not None:
                raise LeaseError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            raise LeaseError(f"job {job_id} not found")
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
    def _is_tenant_paused(conn: Any, tenant_id: str) -> bool:
        row = conn.execute(
            """
            SELECT paused
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return False
        return bool(row["paused"])

    @staticmethod
    def _read_pending_limit(conn: Any, tenant_id: str) -> int | None:
        row = conn.execute(
            """
            SELECT max_pending_jobs
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return None
        return row["max_pending_jobs"]

    @staticmethod
    def _count_pending(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM jobs
            WHERE tenant_id = %s
              AND status = %s
            """,
            (tenant_id, JobStatus.PENDING.value),
        ).fetchone()
        return int(row["n"]) if row else 0

    def _assert_pending_capacity(self, conn: Any, tenant_id: str) -> None:
        """Raise ValueError if tenant is at its pending job cap.

        Serializes with other enqueue / schedule / replay paths for this
        tenant via a transaction-scoped advisory lock.
        """
        limit = self._read_pending_limit(conn, tenant_id)
        if limit is None:
            return
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (tenant_id,),
        )
        # Re-read under lock in case settings changed (same txn usually static).
        limit = self._read_pending_limit(conn, tenant_id)
        if limit is None:
            return
        n = self._count_pending(conn, tenant_id)
        if n >= limit:
            raise ValueError(
                f"tenant {tenant_id!r} pending job cap reached "
                f"({n}/{limit}); enqueue is rejected"
            )

    @staticmethod
    def _delete_empty_tenant_settings(conn: Any, tenant_id: str) -> None:
        conn.execute(
            """
            DELETE FROM tenant_settings
            WHERE tenant_id = %s
              AND max_concurrent_leases IS NULL
              AND max_pending_jobs IS NULL
              AND paused = FALSE
              AND offboarding = FALSE
            """,
            (tenant_id,),
        )

    @staticmethod
    def _is_tenant_offboarding(conn: Any, tenant_id: str) -> bool:
        row = conn.execute(
            """
            SELECT offboarding
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return False
        return bool(row["offboarding"])

    def _tenant_is_empty(self, conn: Any, tenant_id: str) -> bool:
        """True when tenant has no jobs, schedules, webhooks, or settings."""
        jobs = conn.execute(
            "SELECT 1 FROM jobs WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if jobs is not None:
            return False
        sched = conn.execute(
            "SELECT 1 FROM schedules WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if sched is not None:
            return False
        wh = conn.execute(
            "SELECT 1 FROM webhook_endpoints WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if wh is not None:
            return False
        deliveries = conn.execute(
            "SELECT 1 FROM webhook_deliveries WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if deliveries is not None:
            return False
        settings = conn.execute(
            "SELECT 1 FROM tenant_settings WHERE tenant_id = %s LIMIT 1",
            (tenant_id,),
        ).fetchone()
        if settings is not None:
            return False
        return True

    @staticmethod
    def _count_webhook_delivering(conn: Any, tenant_id: str) -> int:
        row = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM webhook_deliveries
            WHERE tenant_id = %s
              AND status = 'delivering'
            """,
            (tenant_id,),
        ).fetchone()
        return int(row["n"]) if row else 0

    def _finalize_offboard_locked(
        self,
        conn: Any,
        *,
        tenant_id: str,
        now: datetime,
    ) -> None:
        """Purge all remaining tenant state after in-flight work ends.

        Caller must hold the connection transaction. Refuses to wipe while
        leased jobs or claimed (``delivering``) webhooks remain.
        """
        # Safety: never wipe in-flight work.
        if self._count_leased(conn, tenant_id) > 0:
            return
        if self._count_webhook_delivering(conn, tenant_id) > 0:
            return

        conn.execute(
            "DELETE FROM jobs WHERE tenant_id = %s",
            (tenant_id,),
        )
        conn.execute(
            "DELETE FROM schedules WHERE tenant_id = %s",
            (tenant_id,),
        )
        # Deliveries first so endpoint CASCADE is a no-op for leftovers.
        conn.execute(
            "DELETE FROM webhook_deliveries WHERE tenant_id = %s",
            (tenant_id,),
        )
        conn.execute(
            "DELETE FROM webhook_endpoints WHERE tenant_id = %s",
            (tenant_id,),
        )
        conn.execute(
            "DELETE FROM tenant_settings WHERE tenant_id = %s",
            (tenant_id,),
        )
        conn.execute(
            "DELETE FROM tenant_fairness WHERE tenant_id = %s",
            (tenant_id,),
        )
        # *now* reserved for future audit rows; keep signature stable.
        _ = now

    def _maybe_finalize_offboard(
        self,
        conn: Any,
        *,
        tenant_id: str,
        now: datetime,
    ) -> None:
        """If tenant is offboarding and has no in-flight work left, purge empty."""
        if not self._is_tenant_offboarding(conn, tenant_id):
            return
        if self._count_leased(conn, tenant_id) > 0:
            return
        if self._count_webhook_delivering(conn, tenant_id) > 0:
            return
        self._finalize_offboard_locked(conn, tenant_id=tenant_id, now=now)

    def _reap_offboarding_expired_webhooks(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
    ) -> None:
        """Dead-letter expired ``delivering`` webhooks for offboarding tenants.

        Offboarding tenants are paused, so stuck claims are never reclaimed for
        a re-POST. Without this, abandoned agents leave the tenant non-empty.
        """
        if tenant_id is not None:
            if not self._is_tenant_offboarding(conn, tenant_id):
                return
            expired = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries
                WHERE tenant_id = %s
                  AND status = 'delivering'
                  AND delivering_until IS NOT NULL
                  AND delivering_until < %s
                FOR UPDATE
                """,
                (tenant_id, now),
            ).fetchall()
        else:
            expired = conn.execute(
                f"""
                SELECT {self._WEBHOOK_DELIVERY_COLUMNS}
                FROM webhook_deliveries d
                WHERE d.status = 'delivering'
                  AND d.delivering_until IS NOT NULL
                  AND d.delivering_until < %s
                  AND EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = d.tenant_id
                          AND ts.offboarding = TRUE
                  )
                FOR UPDATE OF d
                """,
                (now,),
            ).fetchall()

        tenants: set[str] = set()
        for row in expired:
            tid = row["tenant_id"]
            # Proven 2xx already posted → succeed; else dead (no re-POST).
            code = row.get("last_status_code")
            if code is not None and 200 <= int(code) < 300:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'succeeded',
                        delivering_until = NULL,
                        last_error = NULL,
                        updated_at = %s,
                        completed_at = COALESCE(completed_at, %s)
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING id
                    """,
                    (now, now, row["id"]),
                ).fetchone()
            else:
                updated = conn.execute(
                    f"""
                    UPDATE webhook_deliveries
                    SET status = 'dead',
                        delivering_until = NULL,
                        last_error = COALESCE(
                            last_error,
                            'claim expired during tenant offboard'
                        ),
                        updated_at = %s,
                        completed_at = %s
                    WHERE id = %s
                      AND status = 'delivering'
                    RETURNING id
                    """,
                    (now, now, row["id"]),
                ).fetchone()
            if updated is not None:
                tenants.add(tid)

        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)

    def _reap_offboarding_expired_leases(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
    ) -> None:
        """Cancel expired leases for offboarding tenants.

        Offboarding marks tenants paused, so expired leases are never reclaimed.
        Without this sweep, abandoned holders would leave the tenant non-empty
        forever. Cancelling expired leases lets ``_maybe_finalize_offboard``
        purge the tenant once nothing is running.
        """
        if tenant_id is not None:
            if not self._is_tenant_offboarding(conn, tenant_id):
                return
            expired = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND lease_expires_at < %s
                FOR UPDATE
                """,
                (tenant_id, JobStatus.LEASED.value, now),
            ).fetchall()
        else:
            expired = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = %s
                  AND lease_expires_at < %s
                  AND EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = jobs.tenant_id
                          AND ts.offboarding = TRUE
                  )
                FOR UPDATE OF jobs
                """,
                (JobStatus.LEASED.value, now),
            ).fetchall()

        tenants: set[str] = set()
        for row in expired:
            tid = row["tenant_id"]
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'lease expired during tenant offboard'
                    ),
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    row["id"],
                    tid,
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                continue
            tenants.add(tid)
            self._cascade_cancel_dependents(
                conn,
                parent_id=row["id"],
                tenant_id=tid,
                reason=(
                    f"parent job {row['id']} cancelled "
                    f"(lease expired during offboard)"
                ),
                now=now,
            )

        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)

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

    def _load_tenant_stats(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> TenantStats:
        counts = {
            JobStatus.PENDING.value: 0,
            JobStatus.LEASED.value: 0,
            JobStatus.COMPLETED.value: 0,
            JobStatus.FAILED.value: 0,
            JobStatus.DEAD.value: 0,
            JobStatus.CANCELLED.value: 0,
        }
        rows = conn.execute(
            """
            SELECT status, COUNT(*)::int AS n
            FROM jobs
            WHERE tenant_id = %s
            GROUP BY status
            """,
            (tenant_id,),
        ).fetchall()
        for r in rows:
            counts[r["status"]] = int(r["n"])

        settings = conn.execute(
            """
            SELECT max_concurrent_leases, max_pending_jobs, paused
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        paused = bool(settings["paused"]) if settings else False
        max_conc = settings["max_concurrent_leases"] if settings else None
        max_pending = settings["max_pending_jobs"] if settings else None

        if paused:
            claimable = 0
        else:
            claimable_row = conn.execute(
                f"""
                SELECT COUNT(*)::int AS n
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND available_at <= %s
                  AND {_DEPS_SATISFIED_SQL}
                """,
                (tenant_id, JobStatus.PENDING.value, now),
            ).fetchone()
            claimable = int(claimable_row["n"]) if claimable_row else 0

        total = sum(counts.values())
        return TenantStats(
            tenant_id=tenant_id,
            paused=paused,
            max_concurrent_leases=max_conc,
            max_pending_jobs=max_pending,
            pending=counts[JobStatus.PENDING.value],
            leased=counts[JobStatus.LEASED.value],
            completed=counts[JobStatus.COMPLETED.value],
            failed=counts[JobStatus.FAILED.value],
            dead=counts[JobStatus.DEAD.value],
            cancelled=counts[JobStatus.CANCELLED.value],
            total=total,
            claimable=claimable,
        )
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
        """Cancel pending *and leased* jobs that depend on *parent_id* (recursive).

        Running (leased) children have their fencing token cleared so the worker
        holding them cannot heartbeat/ack and complete after the parent died.
        """
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
                  AND status IN (%s, %s)
                  AND depends_on @> ARRAY[%s]::uuid[]
                FOR UPDATE
                """,
                (
                    tenant_id,
                    JobStatus.PENDING.value,
                    JobStatus.LEASED.value,
                    current,
                ),
            ).fetchall()
            for child in children:
                was_running = child["status"] == JobStatus.LEASED.value
                err = reason
                if was_running:
                    err = f"{reason} (stopped while running)"
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status IN (%s, %s)
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        err,
                        now,
                        child["id"],
                        tenant_id,
                        JobStatus.PENDING.value,
                        JobStatus.LEASED.value,
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
