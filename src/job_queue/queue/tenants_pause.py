"""JobQueue tenant pause/resume and pause flags."""
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


class TenantsPauseMixin:
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
