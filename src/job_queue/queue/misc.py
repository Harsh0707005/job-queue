"""JobQueue misc domain (MiscMixin)."""
from __future__ import annotations

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

class MiscMixin:
    @staticmethod
    def _payload_kind(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        for key in ("kind", "job_type", "type"):
            val = payload.get(key)
            if val is not None and str(val):
                return str(val)[:128]
        return ""


    def _lookup_ewma_seconds(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        kind: str,
    ) -> float | None:
        row = conn.execute(
            """
            SELECT ewma_seconds
            FROM duration_estimates
            WHERE tenant_id = %s
              AND queue_name = %s
              AND kind = %s
            """,
            (tenant_id, queue_name, kind),
        ).fetchone()
        if row is None:
            return None
        return float(row["ewma_seconds"])


    def _urgency_tuple(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> tuple[int, float, int, int]:
        """Sort key: smaller = more urgent."""
        slack = self._slack_seconds(conn, row, now=now)
        has_deadline = 0 if row.get("deadline_at") is not None else 1
        priority = -int(row.get("priority") or 0)
        preempts = int(row.get("preempt_count") or 0)
        return (has_deadline, slack, priority, preempts)


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


