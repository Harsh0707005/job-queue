"""JobQueue tenant stats domain: get/list tenant stats."""
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


class TenantsStatsMixin:
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
