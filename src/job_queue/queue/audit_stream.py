"""JobQueue audit event stream (LISTEN/NOTIFY + poll iterator)."""
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


class AuditStreamMixin:
    def iter_job_events(
        self,
        *,
        tenant_ids: Sequence[str],
        job_ids: Sequence[UUID] | None = None,
        labels: Sequence[str] | str | None = None,
        include_snapshot: bool = True,
        idle_timeout_seconds: float | None = None,
        heartbeat_seconds: float = 15.0,
        stop_when: Any = None,
        max_snapshot_jobs: int = 10_000,
    ) -> Any:
        """Yield :class:`JobProgressEvent` live for the given tenants.

        Uses Postgres ``LISTEN job_queue_events`` so any coordinator/worker
        mutation is visible to all subscribers (multi-node safe).

        Subscribe order is **LISTEN → snapshot → drain buffered notifies →
        live loop**. Listening first ensures a completion that races the
        snapshot is not lost (it sits in the notify buffer until drained).

        *tenant_ids* is required (one or more). Optional *job_ids* and
        *labels* (AND) filter events. When *include_snapshot* is True, yields
        current matching job rows first (``snapshot=True``).

        *max_snapshot_jobs* caps how many snapshot rows are loaded for a
        tenant-wide stream (default 10_000) so a huge backlog cannot blow
        memory. Filtered *job_ids* ignore this cap (bounded by the filter).
        Pass a larger value or rely on job_ids for full history.

        *stop_when* is an optional zero-arg callable; when it returns True the
        iterator ends. *idle_timeout_seconds* ends after that many seconds
        without events (after snapshot). Heartbeat events use ``event=ping``.
        """
        import time as _time

        import psycopg
        from psycopg.rows import dict_row

        if not tenant_ids:
            raise ValueError("tenant_ids is required")
        tenants = {str(t) for t in tenant_ids if t}
        if not tenants:
            raise ValueError("tenant_ids is required")
        if max_snapshot_jobs < 1:
            raise ValueError("max_snapshot_jobs must be >= 1")
        job_filter: set[str] | None = None
        if job_ids is not None:
            job_filter = {str(j) for j in job_ids}
            if not job_filter:
                job_filter = None
        label_list = _normalize_labels(labels)

        def _match(ev: dict[str, Any]) -> bool:
            if ev.get("tenant_id") not in tenants:
                return False
            if job_filter is not None and str(ev.get("job_id")) not in job_filter:
                return False
            if label_list:
                labs = ev.get("labels") or []
                if not set(label_list).issubset(set(labs)):
                    return False
            return True

        def _to_event(ev: dict[str, Any], *, snapshot: bool = False) -> JobProgressEvent:
            labs = ev.get("labels") or []
            ts_raw = ev.get("ts")
            if isinstance(ts_raw, str):
                try:
                    ts = datetime.fromisoformat(ts_raw)
                except ValueError:
                    ts = _utcnow()
            elif isinstance(ts_raw, datetime):
                ts = ts_raw
            else:
                ts = _utcnow()
            return JobProgressEvent(
                event=str(ev.get("event") or "update"),
                tenant_id=str(ev["tenant_id"]),
                job_id=UUID(str(ev["job_id"])),
                status=str(ev.get("status") or ""),
                from_status=(
                    None
                    if ev.get("from_status") is None
                    else str(ev.get("from_status"))
                ),
                attempts=int(ev.get("attempts") or 0),
                labels=tuple(str(x) for x in labs),
                checkpoint=ev.get("checkpoint"),
                result=ev.get("result"),
                last_error=ev.get("last_error"),
                actor=ev.get("actor"),
                ts=ts,
                snapshot=snapshot or bool(ev.get("snapshot")),
            )

        def _job_to_snapshot_event(job: Job) -> JobProgressEvent:
            return JobProgressEvent(
                event="snapshot",
                tenant_id=job.tenant_id,
                job_id=job.id,
                status=job.status.value,
                from_status=None,
                attempts=job.attempts,
                labels=job.labels,
                checkpoint=job.checkpoint,
                result=job.result,
                last_error=job.last_error,
                actor=None,
                ts=job.updated_at,
                snapshot=True,
            )

        def _drain_notifies(conn: Any, *, timeout: float) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for notify in conn.notifies(timeout=timeout):
                try:
                    data = json.loads(notify.payload)
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(data, dict) and _match(data):
                    out.append(data)
            return out

        def _load_snapshot_jobs() -> list[Job]:
            """Load matching jobs for the initial subscribe snapshot.

            Filtered *job_ids* are loaded by primary key (complete, O(n)).
            Tenant-wide snapshots paginate ``list_jobs`` so a hard 500-row
            page cannot silently drop older jobs.
            """
            jobs_out: list[Job] = []
            # Narrow path: exact jobs by id (never depend on list order/limit).
            if job_filter is not None:
                for jid_s in job_filter:
                    for tid in tenants:
                        got = self.get(UUID(jid_s), tenant_id=tid)
                        if got is None:
                            continue
                        if label_list and not set(label_list).issubset(
                            set(got.labels)
                        ):
                            break
                        jobs_out.append(got)
                        break
                return jobs_out

            page_size = 500
            remaining = int(max_snapshot_jobs)
            for tid in sorted(tenants):
                if remaining <= 0:
                    break
                offset = 0
                while remaining > 0:
                    take = min(page_size, remaining)
                    page = self.list_jobs(
                        tenant_id=tid,
                        labels=label_list or None,
                        limit=take,
                        offset=offset,
                    )
                    if not page:
                        break
                    jobs_out.extend(page)
                    remaining -= len(page)
                    if len(page) < take:
                        break
                    offset += len(page)
            return jobs_out

        started = _time.monotonic()
        last_event = started
        last_hb = started
        # last known status per job after snapshot (for reconcile)
        last_status: dict[str, str] = {}

        with psycopg.connect(self.dsn, row_factory=dict_row, autocommit=True) as conn:
            # 1) LISTEN first so completions during snapshot are buffered.
            conn.execute(f"LISTEN {JOB_EVENTS_CHANNEL}")

            # 2) Snapshot while listening (notifies queue on this connection).
            snap_jobs: list[Job] = []
            if include_snapshot:
                snap_jobs = _load_snapshot_jobs()

            # 3) Drain anything that arrived during snapshot (non-blocking).
            buffered = _drain_notifies(conn, timeout=0.0)

            # 4) Emit snapshot, then buffered live events (may include
            #    completions that raced the snapshot read).
            for job in snap_jobs:
                last_status[str(job.id)] = job.status.value
                last_event = _time.monotonic()
                last_hb = last_event
                yield _job_to_snapshot_event(job)

            for data in buffered:
                jid = str(data.get("job_id") or "")
                if jid:
                    last_status[jid] = str(data.get("status") or "")
                last_event = _time.monotonic()
                last_hb = last_event
                yield _to_event(data)

            # 5) Reconcile: for watched jobs, re-GET once. If status advanced
            #    past what we last emitted (notify lost / truncated), emit
            #    a synthetic snapshot of current state so clients are not stuck.
            if include_snapshot and job_filter is not None:
                for jid_s in job_filter:
                    for tid in tenants:
                        got = self.get(UUID(jid_s), tenant_id=tid)
                        if got is None:
                            continue
                        prev = last_status.get(jid_s)
                        if prev != got.status.value:
                            last_status[jid_s] = got.status.value
                            last_event = _time.monotonic()
                            last_hb = last_event
                            yield _job_to_snapshot_event(got)
                        break

            # 6) Live loop.
            while True:
                if stop_when is not None and stop_when():
                    return
                now_m = _time.monotonic()
                if (
                    idle_timeout_seconds is not None
                    and (now_m - last_event) >= float(idle_timeout_seconds)
                ):
                    return
                for data in _drain_notifies(conn, timeout=1.0):
                    jid = str(data.get("job_id") or "")
                    if jid:
                        last_status[jid] = str(data.get("status") or "")
                    last_event = _time.monotonic()
                    last_hb = last_event
                    yield _to_event(data)
                now_m = _time.monotonic()
                if heartbeat_seconds > 0 and (now_m - last_hb) >= float(
                    heartbeat_seconds
                ):
                    last_hb = now_m
                    yield JobProgressEvent(
                        event="ping",
                        tenant_id=next(iter(tenants)),
                        job_id=UUID("00000000-0000-0000-0000-000000000000"),
                        status="",
                        from_status=None,
                        attempts=0,
                        labels=(),
                        checkpoint=None,
                        result=None,
                        last_error=None,
                        actor=None,
                        ts=_utcnow(),
                        snapshot=False,
                    )
