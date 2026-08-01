"""JobQueue public lease/lease_batch entrypoints."""
from __future__ import annotations

import json

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


class LeaseClaimPublicMixin:
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
