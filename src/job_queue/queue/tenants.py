"""JobQueue tenants domain (TenantsMixin)."""
from __future__ import annotations

import math

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

class TenantsMixin:
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


    def set_enqueue_rate_limit(
        self,
        tenant_id: str,
        per_second: float | None,
        *,
        burst: int | None = None,
    ) -> None:
        """Cap how fast *tenant_id* may enqueue new jobs (token bucket).

        *per_second* is the steady refill rate. *burst* is the max bucket
        size (defaults to ``max(1, ceil(per_second))``). Pass ``None`` for
        *per_second* to clear (unlimited). Does not clear pause / other caps.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        rate, burst_n = self._normalize_rate_limit(per_second, burst)
        now = _utcnow()
        with transaction(self._pool) as conn:
            if rate is None:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, max_concurrent_leases, max_pending_jobs,
                        paused, offboarding, updated_at
                    )
                    VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET enqueue_rate_per_sec = NULL,
                        enqueue_burst = NULL,
                        enqueue_tokens = NULL,
                        enqueue_tokens_at = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return
            assert burst_n is not None
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    enqueue_rate_per_sec, enqueue_burst,
                    enqueue_tokens, enqueue_tokens_at,
                    updated_at
                )
                VALUES (
                    %s, NULL, NULL, FALSE, FALSE,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id) DO UPDATE
                SET enqueue_rate_per_sec = EXCLUDED.enqueue_rate_per_sec,
                    enqueue_burst = EXCLUDED.enqueue_burst,
                    enqueue_tokens = EXCLUDED.enqueue_tokens,
                    enqueue_tokens_at = EXCLUDED.enqueue_tokens_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, rate, burst_n, float(burst_n), now, now),
            )


    def get_enqueue_rate_limit(
        self, tenant_id: str
    ) -> tuple[float | None, int | None]:
        """Return ``(per_second, burst)`` or ``(None, None)`` if unlimited."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT enqueue_rate_per_sec, enqueue_burst
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None or row["enqueue_rate_per_sec"] is None:
                return None, None
            return float(row["enqueue_rate_per_sec"]), (
                int(row["enqueue_burst"])
                if row["enqueue_burst"] is not None
                else None
            )


    def set_job_retention(
        self,
        *,
        completed_seconds: int | None = None,
        dead_seconds: int | None = None,
    ) -> JobRetentionSettings:
        """Set global retention for terminal jobs (seconds).

        Pass ``None`` for a status to keep those jobs forever. ``0`` means
        eligible for purge on the next cleaner pass. Per-tenant overrides via
        :meth:`set_tenant_job_retention` take precedence when set.
        """
        if completed_seconds is not None and completed_seconds < 0:
            raise ValueError("completed_seconds must be >= 0 or None")
        if dead_seconds is not None and dead_seconds < 0:
            raise ValueError("dead_seconds must be >= 0 or None")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO queue_settings (
                    id, completed_retention_seconds, dead_retention_seconds,
                    updated_at
                ) VALUES (1, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET completed_retention_seconds = EXCLUDED.completed_retention_seconds,
                    dead_retention_seconds = EXCLUDED.dead_retention_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                (completed_seconds, dead_seconds, now),
            )
        return JobRetentionSettings(
            completed_retention_seconds=completed_seconds,
            dead_retention_seconds=dead_seconds,
        )


    def get_job_retention(self) -> JobRetentionSettings:
        """Return global completed/dead retention (None = keep forever)."""
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM queue_settings
                WHERE id = 1
                """
            ).fetchone()
            if row is None:
                return JobRetentionSettings(None, None)
            return JobRetentionSettings(
                completed_retention_seconds=row["completed_retention_seconds"],
                dead_retention_seconds=row["dead_retention_seconds"],
            )


    def set_tenant_job_retention(
        self,
        tenant_id: str,
        *,
        completed_seconds: Any = _UNSET,
        dead_seconds: Any = _UNSET,
        clear: bool = False,
    ) -> JobRetentionSettings:
        """Override retention for one tenant (or clear back to global).

        When *clear* is True both overrides are removed (inherit global).

        Otherwise pass *completed_seconds* and/or *dead_seconds*:
        ``None`` means **keep forever for this tenant** (does **not** inherit
        global). Pass a non-negative int for a finite window. Omitted keys
        keep their previous override (or inherit if never set). Use *clear*
        (not ``None``) to fall back to global defaults.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not clear:
            if completed_seconds is not _UNSET:
                if completed_seconds is not None and int(completed_seconds) < 0:
                    raise ValueError("completed_seconds must be >= 0 or None")
            if dead_seconds is not _UNSET:
                if dead_seconds is not None and int(dead_seconds) < 0:
                    raise ValueError("dead_seconds must be >= 0 or None")
            if completed_seconds is _UNSET and dead_seconds is _UNSET:
                raise ValueError(
                    "provide completed_seconds and/or dead_seconds, or clear=True"
                )
        now = _utcnow()
        with transaction(self._pool) as conn:
            if clear:
                conn.execute(
                    """
                    INSERT INTO tenant_settings (
                        tenant_id, max_concurrent_leases, max_pending_jobs,
                        paused, offboarding, updated_at
                    )
                    VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET completed_retention_seconds = NULL,
                        dead_retention_seconds = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (tenant_id, now),
                )
                self._delete_empty_tenant_settings(conn, tenant_id)
                return JobRetentionSettings(
                    None,
                    None,
                    completed_inherits=True,
                    dead_inherits=True,
                )
            # Merge with existing stored overrides.
            existing = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM tenant_settings
                WHERE tenant_id = %s
                FOR UPDATE
                """,
                (tenant_id,),
            ).fetchone()
            prev_c = (
                existing["completed_retention_seconds"] if existing else None
            )
            prev_d = existing["dead_retention_seconds"] if existing else None

            def _to_store(val: Any, prev: Any) -> int | None:
                if val is _UNSET:
                    return prev  # may be None (inherit)
                if val is None:
                    return _TENANT_RETENTION_FOREVER
                return int(val)

            c_store = _to_store(completed_seconds, prev_c)
            d_store = _to_store(dead_seconds, prev_d)
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding,
                    completed_retention_seconds, dead_retention_seconds,
                    updated_at
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET completed_retention_seconds = EXCLUDED.completed_retention_seconds,
                    dead_retention_seconds = EXCLUDED.dead_retention_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, c_store, d_store, now),
            )
            return self._decode_tenant_retention_row(
                {
                    "completed_retention_seconds": c_store,
                    "dead_retention_seconds": d_store,
                }
            )


    def get_tenant_job_retention(self, tenant_id: str) -> JobRetentionSettings:
        """Return per-tenant retention override.

        * ``completed_inherits`` / ``dead_inherits`` True → use global.
        * inherits False and seconds ``None`` → explicit forever for tenant.
        * inherits False and seconds ``>= 0`` → finite tenant window.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT completed_retention_seconds, dead_retention_seconds
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            if row is None:
                return JobRetentionSettings(
                    None,
                    None,
                    completed_inherits=True,
                    dead_inherits=True,
                )
            return self._decode_tenant_retention_row(row)


    @staticmethod
    def _decode_tenant_retention_row(row: dict[str, Any]) -> JobRetentionSettings:
        def _one(raw: Any) -> tuple[int | None, bool]:
            if raw is None:
                return None, True  # inherit
            n = int(raw)
            if n < 0:
                return None, False  # forever
            return n, False

        c_sec, c_inh = _one(row.get("completed_retention_seconds"))
        d_sec, d_inh = _one(row.get("dead_retention_seconds"))
        return JobRetentionSettings(
            completed_retention_seconds=c_sec,
            dead_retention_seconds=d_sec,
            completed_inherits=c_inh,
            dead_inherits=d_inh,
        )


    def purge_expired_jobs(
        self,
        *,
        limit: int = 500,
        tenant_id: str | None = None,
    ) -> RetentionPurgeResult:
        """Delete terminal jobs older than their retention window.

        Effective retention per job:

        * tenant override ``>= 0`` → that many seconds
        * tenant override ``-1`` (API ``None``) → keep forever (ignore global)
        * tenant override NULL / no row → global setting
        * global ``NULL`` → keep forever

        Also deletes matching ``job_side_effects`` rows. Audit is retained.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if limit > 10_000:
            raise ValueError("limit must be <= 10000")
        if tenant_id is not None and not tenant_id:
            raise ValueError("tenant_id must be non-empty when provided")

        now = _utcnow()
        half = max(1, limit // 2)
        with transaction(self._pool) as conn:
            # Ensure global row exists for JOIN.
            conn.execute(
                """
                INSERT INTO queue_settings (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING
                """
            )
            completed_ids = self._select_expired_terminal_ids(
                conn,
                status=JobStatus.COMPLETED.value,
                now=now,
                limit=half,
                tenant_id=tenant_id,
            )
            dead_ids = self._select_expired_terminal_ids(
                conn,
                status=JobStatus.DEAD.value,
                now=now,
                limit=limit - len(completed_ids),
                tenant_id=tenant_id,
            )
            all_ids = completed_ids + dead_ids
            side_n = 0
            if all_ids:
                se = conn.execute(
                    """
                    DELETE FROM job_side_effects
                    WHERE job_id = ANY(%s)
                    RETURNING job_id, effect_key
                    """,
                    (all_ids,),
                ).fetchall()
                side_n = len(se)
                conn.execute(
                    "DELETE FROM jobs WHERE id = ANY(%s)",
                    (all_ids,),
                )
            return RetentionPurgeResult(
                completed_deleted=len(completed_ids),
                dead_deleted=len(dead_ids),
                side_effects_deleted=side_n,
            )


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
                        lease_tenant_id = NULL,
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
                cancelled_row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (prow["id"],),
                ).fetchone()
                if cancelled_row is not None:
                    self._append_job_audit(
                        conn,
                        job_row=cancelled_row,
                        event_type="cancelled",
                        from_status=JobStatus.PENDING.value,
                        actor="system",
                        detail={"reason": "tenant offboard"},
                        now=now,
                    )
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

        Lease fencing tokens, ``leased_by``, and ``lease_tenant_id`` are
        preserved so in-flight workers keep heartbeating / acking with the
        pre-merge tenant (lease ops accept current owner **or** the frozen
        claim tenant). Unrelated tenants remain rejected.

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
                  AND j.status IN (
                      'pending', 'leased', 'awaiting_approval', 'awaiting_children'
                  )
                  AND EXISTS (
                        SELECT 1
                        FROM jobs t
                        WHERE t.tenant_id = %s
                          AND t.queue_name = j.queue_name
                          AND t.dedupe_key = j.dedupe_key
                          AND t.status IN (
                              'pending', 'leased',
                              'awaiting_approval', 'awaiting_children'
                          )
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
            for jm in jobs_moved:
                moved_row = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (jm["id"],),
                ).fetchone()
                if moved_row is not None:
                    self._append_job_audit(
                        conn,
                        job_row=moved_row,
                        event_type="tenant_merged",
                        from_status=jm["status"],
                        actor="system",
                        detail={
                            "from_tenant": source,
                            "to_tenant": target,
                        },
                        now=now,
                    )
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

            # Side-effect markers follow job ownership (job_id unchanged).
            conn.execute(
                """
                UPDATE job_side_effects
                SET tenant_id = %s
                WHERE tenant_id = %s
                """,
                (target, source),
            )

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
    def _normalize_rate_limit(
        per_second: float | None,
        burst: int | None,
    ) -> tuple[float | None, int | None]:
        if per_second is None:
            if burst is not None:
                raise ValueError("burst requires per_second to be set")
            return None, None
        rate = float(per_second)
        if rate <= 0:
            raise ValueError("per_second must be > 0 or None")
        if burst is None:
            burst_n = max(1, int(math.ceil(rate)))
        else:
            burst_n = int(burst)
            if burst_n < 1:
                raise ValueError("burst must be >= 1")
        return rate, burst_n


    def _consume_enqueue_rate(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> None:
        """Consume one enqueue token or raise :class:`RateLimitError`."""
        ok, retry_after = self._try_consume_rate_tokens(
            conn,
            tenant_id,
            kind="enqueue",
            now=now,
            cost=1.0,
        )
        if ok:
            return
        raise RateLimitError(
            f"tenant {tenant_id!r} enqueue rate limit exceeded"
            + (
                f"; retry after ~{retry_after:.3f}s"
                if retry_after is not None
                else ""
            ),
            tenant_id=tenant_id,
            kind="enqueue",
            retry_after_seconds=retry_after,
        )


    def _try_consume_rate_tokens(
        self,
        conn: Any,
        tenant_id: str,
        *,
        kind: str,
        now: datetime,
        cost: float = 1.0,
    ) -> tuple[bool, float | None]:
        """Token-bucket consume under the tenant advisory lock.

        Returns ``(True, None)`` if allowed (or unlimited), else
        ``(False, retry_after_seconds)``.
        """
        if kind not in ("enqueue", "lease"):
            raise ValueError(f"unknown rate kind {kind!r}")
        rate_col = f"{kind}_rate_per_sec"
        burst_col = f"{kind}_burst"
        tokens_col = f"{kind}_tokens"
        at_col = f"{kind}_tokens_at"

        # Caller often already holds the tenant lock; take it again (re-entrant
        # xact lock is fine) so standalone paths are safe.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (tenant_id,),
        )
        row = conn.execute(
            f"""
            SELECT {rate_col} AS rate, {burst_col} AS burst,
                   {tokens_col} AS tokens, {at_col} AS tokens_at
            FROM tenant_settings
            WHERE tenant_id = %s
            FOR UPDATE
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row["rate"] is None:
            return True, None

        rate = float(row["rate"])
        if rate <= 0:
            return True, None
        burst = int(row["burst"] or max(1, int(math.ceil(rate))))
        tokens = row["tokens"]
        tokens_at = row["tokens_at"]
        if tokens is None:
            tokens = float(burst)
        else:
            tokens = float(tokens)
        if tokens_at is None:
            tokens_at = now
        else:
            tokens_at = _as_utc(tokens_at)

        elapsed = max(0.0, (now - tokens_at).total_seconds())
        tokens = min(float(burst), tokens + elapsed * rate)
        if tokens + 1e-12 < cost:
            need = cost - tokens
            retry_after = need / rate if rate > 0 else None
            # Persist refilled (but not consumed) tokens so time advances.
            conn.execute(
                f"""
                UPDATE tenant_settings
                SET {tokens_col} = %s,
                    {at_col} = %s,
                    updated_at = %s
                WHERE tenant_id = %s
                """,
                (tokens, now, now, tenant_id),
            )
            return False, retry_after

        tokens -= cost
        conn.execute(
            f"""
            UPDATE tenant_settings
            SET {tokens_col} = %s,
                {at_col} = %s,
                updated_at = %s
            WHERE tenant_id = %s
            """,
            (tokens, now, now, tenant_id),
        )
        return True, None


    @staticmethod
    def _delete_empty_tenant_settings(conn: Any, tenant_id: str) -> None:
        conn.execute(
            """
            DELETE FROM tenant_settings
            WHERE tenant_id = %s
              AND max_concurrent_leases IS NULL
              AND max_pending_jobs IS NULL
              AND enqueue_rate_per_sec IS NULL
              AND lease_rate_per_sec IS NULL
              AND completed_retention_seconds IS NULL
              AND dead_retention_seconds IS NULL
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
            SELECT max_concurrent_leases, max_pending_jobs, paused,
                   enqueue_rate_per_sec, enqueue_burst,
                   lease_rate_per_sec, lease_burst
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        paused = bool(settings["paused"]) if settings else False
        max_conc = settings["max_concurrent_leases"] if settings else None
        max_pending = settings["max_pending_jobs"] if settings else None
        enq_rate = (
            float(settings["enqueue_rate_per_sec"])
            if settings and settings["enqueue_rate_per_sec"] is not None
            else None
        )
        enq_burst = (
            int(settings["enqueue_burst"])
            if settings and settings["enqueue_burst"] is not None
            else None
        )
        lease_rate = (
            float(settings["lease_rate_per_sec"])
            if settings and settings["lease_rate_per_sec"] is not None
            else None
        )
        lease_burst = (
            int(settings["lease_burst"])
            if settings and settings["lease_burst"] is not None
            else None
        )

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
            enqueue_rate_per_sec=enq_rate,
            enqueue_burst=enq_burst,
            lease_rate_per_sec=lease_rate,
            lease_burst=lease_burst,
            pending=counts[JobStatus.PENDING.value],
            leased=counts[JobStatus.LEASED.value],
            completed=counts[JobStatus.COMPLETED.value],
            failed=counts[JobStatus.FAILED.value],
            dead=counts[JobStatus.DEAD.value],
            cancelled=counts[JobStatus.CANCELLED.value],
            total=total,
            claimable=claimable,
        )


