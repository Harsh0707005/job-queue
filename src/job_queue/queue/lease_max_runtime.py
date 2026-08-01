"""JobQueue max-runtime release and exhausted-lease dead-letter."""
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


class LeaseMaxRuntimeMixin:
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
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    completed_at = CASE WHEN %s = %s THEN %s ELSE completed_at END,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
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
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type=(
                            "cancelled"
                            if terminal == JobStatus.CANCELLED.value
                            else "dead_lettered"
                        ),
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
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
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    last_error = %s,
                    available_at = %s,
                    updated_at = %s
                WHERE id = %s
                  AND tenant_id = %s
                  AND status = %s
                RETURNING id
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
            ).fetchone()
            if updated is not None:
                full = conn.execute(
                    f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                    (row["id"],),
                ).fetchone()
                if full is not None:
                    self._append_job_audit(
                        conn,
                        job_row=full,
                        event_type="requeued",
                        from_status=JobStatus.LEASED.value,
                        actor="system",
                        detail={"reason": error},
                        now=now,
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
                    lease_tenant_id = NULL,
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
                    lease_tenant_id = NULL,
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
            full = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (dead["id"],),
            ).fetchone()
            if full is not None:
                self._append_job_audit(
                    conn,
                    job_row=full,
                    event_type="dead_lettered",
                    from_status=JobStatus.LEASED.value,
                    actor="system",
                    detail={
                        "reason": "exceeded max_attempts after lease expiry"
                    },
                    now=now,
                )
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
                self._enqueue_job_webhooks(
                    conn,
                    job=_row_to_job(full),
                    event=WebhookEvent.JOB_DEAD,
                    now=now,
                )
            else:
                self._cascade_cancel_dependents(
                    conn,
                    parent_id=dead["id"],
                    tenant_id=tid,
                    reason=f"parent job {dead['id']} dead-lettered",
                    now=now,
                )
        for tid in tenants:
            self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)
