"""JobQueue tenant offboard and emptiness checks."""
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


class TenantsOffboardMixin:
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

    def tenant_is_empty(self, tenant_id: str) -> bool:
        """True when tenant has no jobs, schedules, webhooks, or settings."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._tenant_is_empty(conn, tenant_id)

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
