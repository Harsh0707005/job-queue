"""JobQueue tenant concurrency and pending capacity limits."""
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


class TenantsConcurrencyMixin:
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
