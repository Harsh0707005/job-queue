"""JobQueue tenant lifecycle domain: pause/resume, offboard, merge, emptiness checks."""
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


class TenantsLifecycleMixin:
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
