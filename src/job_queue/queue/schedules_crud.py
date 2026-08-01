"""JobQueue schedule CRUD: create/get/list/pause/resume/delete."""
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


class SchedulesCrudMixin:
    def create_schedule(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        cron: str | None = None,
        interval_seconds: int | None = None,
        timezone: str = "UTC",
        queue_name: str = "default",
        name: str | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        start_at: datetime | None = None,
        enabled: bool = True,
        schedule_id: UUID | None = None,
        labels: Sequence[str] | str | None = None,
    ) -> Schedule:
        """Register a recurring schedule (cron and/or fixed interval).

        Prefer *cron* (standard crontab). Examples::

            \"0 9 * * 1-5\"     # weekdays at 09:00 in *timezone*
            \"0 0 1 * *\"       # midnight on the 1st of each month
            \"*/5 * * * * *\"   # every 5 seconds (6-field)

        *interval_seconds* is still supported for fixed delays. Provide at
        least one of *cron* or *interval_seconds*.

        Call ``process_due_schedules()`` (or ``ScheduleRunner``) to materialize
        due ticks. Schedule fires **bypass** tenant pending caps and enqueue
        rate limits.

        Parameters
        ----------
        start_at:
            Base time for the first ``next_run_at``. For cron, the first fire
            is the next occurrence **after** this instant (default: now).
            For interval-only, defaults to now (due on the next tick).
        name:
            Optional unique name per tenant for lookup / idempotent registration.
        timezone:
            IANA timezone for cron evaluation (default ``UTC``).
        """
        from job_queue.cronutil import next_cron_fire, validate_cron, validate_timezone

        if not tenant_id:
            raise ValueError("tenant_id is required")
        if cron is None and interval_seconds is None:
            raise ValueError("provide cron and/or interval_seconds")
        if interval_seconds is not None and interval_seconds < 1:
            raise ValueError("interval_seconds must be >= 1 or None")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if name is not None and name == "":
            raise ValueError("name must be non-empty when provided")

        cron_expr = validate_cron(cron) if cron is not None else None
        tz_name = validate_timezone(timezone)
        label_list = _normalize_labels(labels)
        now = _utcnow()
        base = _as_utc(start_at) if start_at is not None else now
        if cron_expr is not None:
            # First fire = next occurrence after base (not base itself unless
            # it already matches a boundary and caller wants that — croniter
            # get_next is strict-after).
            first_run = next_cron_fire(cron_expr, base, timezone_name=tz_name)
        else:
            first_run = base
        sid = schedule_id or uuid4()

        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; "
                    "schedule registration is rejected"
                )
            try:
                row = conn.execute(
                    f"""
                    INSERT INTO schedules (
                        id, tenant_id, name, queue_name, payload,
                        interval_seconds, cron_expr, timezone,
                        priority, max_attempts,
                        visibility_timeout_seconds, enabled, next_run_at,
                        labels, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        sid,
                        tenant_id,
                        name,
                        queue_name,
                        Jsonb(payload),
                        interval_seconds,
                        cron_expr,
                        tz_name,
                        priority,
                        max_attempts,
                        visibility_timeout_seconds,
                        enabled,
                        first_run,
                        label_list,
                        now,
                        now,
                    ),
                ).fetchone()
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule name {name!r} already exists for tenant {tenant_id!r}"
                ) from exc
            assert row is not None
            return _row_to_schedule(row)

    def get_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule | None:
        """Fetch a schedule by id for *tenant_id* (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return _row_to_schedule(row) if row else None

    def list_schedules(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Schedule]:
        """List schedules for a tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if enabled_only:
            clauses.append("enabled = TRUE")
        where = " AND ".join(clauses)
        params.extend([limit, offset])
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE {where}
                ORDER BY created_at ASC
                LIMIT %s OFFSET %s
                """,
                params,
            ).fetchall()
            return [_row_to_schedule(r) for r in rows]

    def pause_schedule(self, schedule_id: UUID, *, tenant_id: str) -> Schedule:
        """Stop a schedule from firing (keeps the row; use resume to re-enable)."""
        return self._set_schedule_enabled(schedule_id, tenant_id=tenant_id, enabled=False)

    def resume_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        next_run_at: datetime | None = None,
    ) -> Schedule:
        """Re-enable a paused schedule.

        If *next_run_at* is omitted, the next fire is set to now so it is due
        on the following ``process_due_schedules`` call.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        run_at = _as_utc(next_run_at) if next_run_at is not None else now
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = TRUE,
                    next_run_at = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (run_at, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return _row_to_schedule(row)

    def delete_schedule(self, schedule_id: UUID, *, tenant_id: str) -> bool:
        """Permanently remove a schedule. Returns True if a row was deleted."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                DELETE FROM schedules
                WHERE id = %s AND tenant_id = %s
                RETURNING id
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            return row is not None

    def _set_schedule_enabled(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> Schedule:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                UPDATE schedules
                SET enabled = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (enabled, now, schedule_id, tenant_id),
            ).fetchone()
            if row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            return _row_to_schedule(row)

    @staticmethod
    def _schedule_next_run(sched_row: dict[str, Any], *, now: datetime) -> datetime:
        """Compute the next fire time after *now* for a schedule row."""
        cron = sched_row.get("cron_expr")
        if cron:
            from job_queue.cronutil import next_cron_fire

            tz = str(sched_row.get("timezone") or "UTC")
            return next_cron_fire(str(cron), now, timezone_name=tz)
        interval = sched_row.get("interval_seconds")
        if interval is None:
            raise ValueError("schedule has neither cron_expr nor interval_seconds")
        return now + timedelta(seconds=int(interval))
