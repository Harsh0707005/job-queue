"""JobQueue list_orphaned_audit_jobs."""
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


class AuditOrphansMixin:
    def list_orphaned_audit_jobs(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 1000,
    ) -> list[UUID]:
        """Job ids that have audit history but no ``jobs`` row (rebuild candidates)."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._pool.connection() as conn:
            if tenant_id is None:
                rows = conn.execute(
                    """
                    SELECT DISTINCT e.job_id
                    FROM job_audit_events e
                    WHERE NOT EXISTS (
                        SELECT 1 FROM jobs j WHERE j.id = e.job_id
                    )
                    ORDER BY e.job_id
                    LIMIT %s
                    """,
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT e.job_id
                    FROM job_audit_events e
                    INNER JOIN (
                        SELECT job_id, MAX(seq) AS max_seq
                        FROM job_audit_events
                        GROUP BY job_id
                    ) last ON last.job_id = e.job_id AND last.max_seq = e.seq
                    WHERE e.tenant_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM jobs j WHERE j.id = e.job_id
                      )
                    ORDER BY e.job_id
                    LIMIT %s
                    """,
                    (tenant_id, limit),
                ).fetchall()
            return [r["job_id"] for r in rows]
