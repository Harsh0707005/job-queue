"""Shared constants, helpers, and exceptions for JobQueue."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from job_queue.models import (
    Job,
    JobAuditEvent,
    JobStatus,
    Schedule,
)

JOB_EVENTS_CHANNEL = "job_queue_events"

_NOTIFY_MAX_BYTES = 7000  # pg_notify limit is 8000; leave margin

_DEFAULT_ESTIMATE_SECONDS = 60.0

_EWMA_ALPHA = 0.3

_MAX_PREEMPTS_PER_JOB = 8

_DEDUPE_LOCK_CLASS = 884_422_01

_TENANT_RETENTION_FOREVER = -1

_UNSET = object()

_JOB_COLUMNS = """
    id, tenant_id, queue_name, payload, status, priority, attempts,
    max_attempts, visibility_timeout_seconds, max_runtime_seconds,
    retry_backoff_base_seconds, retry_backoff_max_seconds, available_at,
    dedupe_key, schedule_id, depends_on, parent_job_id, waiting_on, labels,
    leased_by, lease_token, lease_tenant_id, lease_expires_at,
    lease_started_at, created_at, updated_at, completed_at, result,
    checkpoint, checkpointed_at, deadline_at, estimated_duration_seconds,
    preemptible, preempt_count, last_preempted_at, approval_requested_at,
    approval_deadline_at, approval_message, approval_outcome,
    approval_resolved_at, await_children_started_at,
    await_children_deadline_at, await_children_outcome,
    await_children_resolved_at, last_error
"""

_MAX_LABELS = 32

_MAX_LABEL_LEN = 128

_CANCELABLE_STATUSES = (
    JobStatus.PENDING,
    JobStatus.LEASED,
    JobStatus.AWAITING_APPROVAL,
    JobStatus.AWAITING_CHILDREN,
)

_DEPS_SATISFIED_SQL = """
(
    COALESCE(depends_on, '{}'::uuid[]) = '{}'::uuid[]
    OR NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(depends_on, '{}'::uuid[])) AS parent_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM jobs AS parent
            WHERE parent.id = parent_id
              AND parent.tenant_id = jobs.tenant_id
              AND parent.status = 'completed'
        )
    )
)
"""

_SCHEDULE_COLUMNS = """
    id, tenant_id, name, queue_name, payload, interval_seconds, cron_expr,
    timezone, priority, max_attempts, visibility_timeout_seconds, enabled,
    next_run_at, last_enqueued_at, created_at, updated_at, labels
"""

_CANCEL_MATCHING_BATCH_DEFAULT = 500

_CANCEL_MATCHING_RETURN_CAP = 200


def _normalize_labels(labels: Sequence[str] | str | None) -> list[str]:
    """Strip, dedupe, sort labels. Empty input → []."""
    if labels is None:
        return []
    if isinstance(labels, str):
        raw = [labels]
    else:
        raw = list(labels)
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            raise ValueError("labels must be strings")
        lab = item.strip()
        if not lab:
            raise ValueError("labels must be non-empty after strip")
        if len(lab) > _MAX_LABEL_LEN:
            raise ValueError(
                f"label longer than {_MAX_LABEL_LEN} characters: {lab[:32]!r}..."
            )
        if lab in seen:
            continue
        seen.add(lab)
        out.append(lab)
    if len(out) > _MAX_LABELS:
        raise ValueError(f"at most {_MAX_LABELS} labels allowed")
    out.sort()
    return out


def _normalize_status_filter(
    status: JobStatus | str | Sequence[JobStatus | str] | None,
) -> list[str] | None:
    if status is None:
        return None
    if isinstance(status, (JobStatus, str)):
        items: list[JobStatus | str] = [status]
    else:
        items = list(status)
    out: list[str] = []
    for s in items:
        if isinstance(s, JobStatus):
            out.append(s.value)
        else:
            out.append(JobStatus(str(s)).value)
    return out


def _row_to_job(row: dict[str, Any]) -> Job:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    deps = row.get("depends_on") or []
    if isinstance(deps, str):
        # unlikely, but be defensive
        deps = []
    waiting = row.get("waiting_on") or []
    if isinstance(waiting, str):
        waiting = []
    labs = row.get("labels") or []
    if isinstance(labs, str):
        labs = []
    return Job(
        id=row["id"],
        tenant_id=row["tenant_id"],
        queue_name=row["queue_name"],
        payload=payload,
        status=JobStatus(row["status"]),
        priority=row["priority"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        visibility_timeout_seconds=row["visibility_timeout_seconds"],
        max_runtime_seconds=(
            3600
            if row.get("max_runtime_seconds") is None
            else int(row["max_runtime_seconds"])
        ),
        retry_backoff_base_seconds=(
            1
            if row.get("retry_backoff_base_seconds") is None
            else int(row["retry_backoff_base_seconds"])
        ),
        retry_backoff_max_seconds=(
            300
            if row.get("retry_backoff_max_seconds") is None
            else int(row["retry_backoff_max_seconds"])
        ),
        available_at=row["available_at"],
        dedupe_key=row["dedupe_key"],
        schedule_id=row["schedule_id"],
        depends_on=tuple(deps),
        parent_job_id=row.get("parent_job_id"),
        waiting_on=tuple(waiting),
        labels=tuple(labs),
        leased_by=row["leased_by"],
        lease_token=row["lease_token"],
        lease_tenant_id=row.get("lease_tenant_id"),
        lease_expires_at=row["lease_expires_at"],
        lease_started_at=row.get("lease_started_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        result=row.get("result"),
        checkpoint=row.get("checkpoint"),
        checkpointed_at=row.get("checkpointed_at"),
        deadline_at=row.get("deadline_at"),
        estimated_duration_seconds=(
            None
            if row.get("estimated_duration_seconds") is None
            else int(row["estimated_duration_seconds"])
        ),
        preemptible=bool(row["preemptible"])
        if row.get("preemptible") is not None
        else True,
        preempt_count=int(row["preempt_count"] or 0),
        last_preempted_at=row.get("last_preempted_at"),
        approval_requested_at=row.get("approval_requested_at"),
        approval_deadline_at=row.get("approval_deadline_at"),
        approval_message=row.get("approval_message"),
        approval_outcome=row.get("approval_outcome"),
        approval_resolved_at=row.get("approval_resolved_at"),
        await_children_started_at=row.get("await_children_started_at"),
        await_children_deadline_at=row.get("await_children_deadline_at"),
        await_children_outcome=row.get("await_children_outcome"),
        await_children_resolved_at=row.get("await_children_resolved_at"),
        last_error=row["last_error"],
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_retry_backoff_seconds(
    attempts: int,
    *,
    base_seconds: int = 1,
    max_seconds: int = 300,
) -> int:
    """Exponential delay after a failed attempt: base * 2^(attempts-1), capped.

    ``attempts`` is the lease attempt count after the failure (typically >= 1).
    """
    if base_seconds < 0 or max_seconds < 0:
        raise ValueError("backoff seconds must be >= 0")
    if attempts < 1:
        attempts = 1
    # Cap exponent to avoid huge ints before min().
    exp = min(attempts - 1, 30)
    delay = base_seconds * (2**exp)
    return int(min(max_seconds, delay))


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _row_to_schedule(row: dict[str, Any]) -> Schedule:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    interval = row.get("interval_seconds")
    cron = row.get("cron_expr")
    tz = row.get("timezone") or "UTC"
    labs = row.get("labels") or []
    if isinstance(labs, str):
        labs = []
    return Schedule(
        id=row["id"],
        tenant_id=row["tenant_id"],
        name=row["name"],
        queue_name=row["queue_name"],
        payload=payload,
        interval_seconds=int(interval) if interval is not None else None,
        cron_expr=str(cron) if cron else None,
        timezone=str(tz),
        priority=row["priority"],
        max_attempts=row["max_attempts"],
        visibility_timeout_seconds=row["visibility_timeout_seconds"],
        enabled=bool(row["enabled"]),
        next_run_at=row["next_run_at"],
        last_enqueued_at=row["last_enqueued_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        labels=tuple(labs),
    )


class LeaseError(Exception):
    """Raised when a lease operation is invalid (wrong owner/tenant/token, expired, etc.)."""


class RateLimitError(Exception):
    """Raised when a tenant exceeds its enqueue (or similar) rate limit."""

    def __init__(
        self,
        message: str,
        *,
        tenant_id: str,
        kind: str,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.tenant_id = tenant_id
        self.kind = kind
        self.retry_after_seconds = retry_after_seconds


class CancelError(Exception):
    """Raised when a job cannot be cancelled (wrong state / tenant / missing)."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class ReplayError(Exception):
    """Raised when a dead-lettered job cannot be replayed."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class DependencyError(Exception):
    """Raised when job dependency setup is invalid."""


class MergeError(Exception):
    """Raised when a tenant ownership transfer cannot complete."""


class ApprovalError(Exception):
    """Raised when an approval / rejection cannot be applied."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class ApprovalRequested(Exception):
    """Job was parked for approval; workers must stop without ack/nack."""

    def __init__(self, job: Job) -> None:
        super().__init__(f"job {job.id} awaiting approval")
        self.job = job


class ChildrenAwaited(Exception):
    """Parent parked waiting for subtasks; workers must stop without ack/nack."""

    def __init__(self, job: Job, children: Sequence[Job] = ()) -> None:
        super().__init__(f"job {job.id} awaiting {len(children)} children")
        self.job = job
        self.children = tuple(children)


class SpawnError(Exception):
    """Raised when spawn_children cannot park or enqueue subtasks."""

    def __init__(self, message: str, *, job: Job | None = None) -> None:
        super().__init__(message)
        self.job = job


class RebuildError(Exception):
    """Raised when a job cannot be safely reconstructed from audit history."""

    def __init__(
        self,
        message: str,
        *,
        job_id: UUID | None = None,
        events: list[JobAuditEvent] | None = None,
    ) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.events = events or []


class _SkipLeaseCandidate(Exception):
    """Internal: abandon a locked fair-lease candidate and try another."""

    def __init__(self, reason: str, *, tenant_id: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.tenant_id = tenant_id


class ScheduleError(Exception):
    """Raised when a schedule operation is invalid."""

    def __init__(self, message: str, *, schedule: Schedule | None = None) -> None:
        super().__init__(message)
        self.schedule = schedule


_SIDE_EFFECT_SELECT = "\n        job_id, effect_key, tenant_id, status, idempotency_key, result,\n        started_by, started_at, completed_by, lease_token,\n        created_at, completed_at\n    "

_WEBHOOK_ENDPOINT_COLUMNS = "\n        id, tenant_id, url, secret, events, enabled, max_attempts,\n        backoff_base_seconds, backoff_max_seconds, created_at, updated_at\n    "

_WEBHOOK_DELIVERY_COLUMNS = "\n        id, endpoint_id, tenant_id, job_id, event, payload, status, attempts,\n        max_attempts, backoff_base_seconds, backoff_max_seconds, next_attempt_at,\n        delivering_until, last_error, last_status_code, created_at, updated_at,\n        completed_at, signing_secret\n    "
