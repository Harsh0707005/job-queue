"""JobQueue audit write/stream domain: snapshots, append, progress notify, SSE iterator."""
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


class AuditWriteMixin:
    @staticmethod
    def _job_snapshot(row: dict[str, Any]) -> dict[str, Any]:
        """JSON-serializable full job row for rebuild-from-audit."""

        def _dt(v: Any) -> str | None:
            if v is None:
                return None
            if isinstance(v, datetime):
                return _as_utc(v).isoformat()
            return str(v)

        def _uuid(v: Any) -> str | None:
            if v is None:
                return None
            return str(v)

        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if not isinstance(payload, dict):
            payload = {"_value": payload}

        result = row.get("result")
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                pass

        checkpoint = row.get("checkpoint")
        if isinstance(checkpoint, str):
            try:
                checkpoint = json.loads(checkpoint)
            except json.JSONDecodeError:
                pass

        deps = row.get("depends_on") or []
        if isinstance(deps, str):
            deps = []
        dep_out = [str(d) for d in deps]

        return {
            "id": str(row["id"]),
            "tenant_id": row["tenant_id"],
            "queue_name": row["queue_name"],
            "payload": payload,
            "status": row["status"],
            "priority": int(row["priority"] or 0),
            "attempts": int(row["attempts"] or 0),
            "max_attempts": int(row["max_attempts"] or 3),
            "visibility_timeout_seconds": int(
                row["visibility_timeout_seconds"] or 30
            ),
            "max_runtime_seconds": int(
                row.get("max_runtime_seconds") or 3600
            ),
            "retry_backoff_base_seconds": int(
                row.get("retry_backoff_base_seconds") or 1
            ),
            "retry_backoff_max_seconds": int(
                row.get("retry_backoff_max_seconds") or 300
            ),
            "available_at": _dt(row.get("available_at")),
            "dedupe_key": row.get("dedupe_key"),
            "schedule_id": _uuid(row.get("schedule_id")),
            "depends_on": dep_out,
            "leased_by": row.get("leased_by"),
            "lease_token": _uuid(row.get("lease_token")),
            "lease_tenant_id": row.get("lease_tenant_id"),
            "lease_expires_at": _dt(row.get("lease_expires_at")),
            "lease_started_at": _dt(row.get("lease_started_at")),
            "created_at": _dt(row.get("created_at")),
            "updated_at": _dt(row.get("updated_at")),
            "completed_at": _dt(row.get("completed_at")),
            "result": result,
            "checkpoint": checkpoint,
            "checkpointed_at": _dt(row.get("checkpointed_at")),
            "deadline_at": _dt(row.get("deadline_at")),
            "estimated_duration_seconds": (
                None
                if row.get("estimated_duration_seconds") is None
                else int(row["estimated_duration_seconds"])
            ),
            "preemptible": (
                True
                if row.get("preemptible") is None
                else bool(row["preemptible"])
            ),
            "preempt_count": int(row.get("preempt_count") or 0),
            "last_preempted_at": _dt(row.get("last_preempted_at")),
            "approval_requested_at": _dt(row.get("approval_requested_at")),
            "approval_deadline_at": _dt(row.get("approval_deadline_at")),
            "approval_message": row.get("approval_message"),
            "approval_outcome": row.get("approval_outcome"),
            "approval_resolved_at": _dt(row.get("approval_resolved_at")),
            "last_error": row.get("last_error"),
        }

    def _append_job_audit(
        self,
        conn: Any,
        *,
        job_id: UUID | None = None,
        tenant_id: str | None = None,
        event_type: str,
        from_status: str | None,
        to_status: str | None = None,
        actor: str | None = None,
        detail: dict[str, Any] | None = None,
        now: datetime | None = None,
        job_row: dict[str, Any] | None = None,
    ) -> JobAuditEvent:
        """Append one immutable audit row (same transaction as the mutation).

        When *job_row* is provided (preferred), embeds a full ``job`` snapshot
        in ``detail`` so lost rows can be rebuilt. Caller must serialize job
        updates (``FOR UPDATE``) so ``seq`` has no gaps.
        """
        ts = now or _utcnow()
        detail_obj = dict(detail or {})
        if job_row is not None:
            job_id = job_row["id"] if job_id is None else job_id
            tenant_id = job_row["tenant_id"] if tenant_id is None else tenant_id
            to_status = job_row["status"] if to_status is None else to_status
            detail_obj["job"] = self._job_snapshot(job_row)
        if job_id is None or tenant_id is None:
            raise ValueError("job_id and tenant_id are required for audit")
        if to_status is None:
            raise ValueError("to_status is required for audit")
        event_id = uuid4()
        row = conn.execute(
            """
            INSERT INTO job_audit_events (
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at
            )
            SELECT
                %s, %s, %s,
                COALESCE((
                    SELECT MAX(e.seq) FROM job_audit_events e WHERE e.job_id = %s
                ), 0) + 1,
                %s, %s, %s, %s, %s, %s
            RETURNING
                event_id, tenant_id, job_id, seq, event_type,
                from_status, to_status, actor, detail, created_at
            """,
            (
                event_id,
                tenant_id,
                job_id,
                job_id,
                event_type,
                from_status,
                to_status,
                actor,
                Jsonb(detail_obj),
                ts,
            ),
        ).fetchone()
        assert row is not None
        # Live progress: same transaction as mutation so NOTIFY is ordered.
        if job_row is not None:
            self._notify_job_progress(
                conn,
                job_row=job_row,
                event_type=event_type,
                from_status=from_status,
                actor=actor,
                now=ts,
            )
        return self._row_to_audit(row)

    def _notify_job_progress(
        self,
        conn: Any,
        *,
        job_row: dict[str, Any],
        event_type: str,
        from_status: str | None,
        actor: str | None,
        now: datetime,
    ) -> None:
        """pg_notify compact progress payload for SSE subscribers."""
        labs = job_row.get("labels") or []
        if isinstance(labs, str):
            labs = []
        cp = job_row.get("checkpoint")
        result = job_row.get("result")
        # Keep payload under NOTIFY size limit.
        payload: dict[str, Any] = {
            "v": 1,
            "event": event_type,
            "tenant_id": job_row["tenant_id"],
            "job_id": str(job_row["id"]),
            "status": job_row["status"],
            "from_status": from_status,
            "attempts": int(job_row.get("attempts") or 0),
            "labels": list(labs)[:32],
            "checkpoint": cp,
            "result": result if job_row["status"] == JobStatus.COMPLETED.value else None,
            "last_error": (
                (job_row.get("last_error") or "")[:500] or None
            ),
            "actor": actor,
            "ts": now.isoformat(),
            "snapshot": False,
        }
        raw = json.dumps(payload, default=str, separators=(",", ":"))
        if len(raw.encode("utf-8")) > _NOTIFY_MAX_BYTES:
            # Drop large fields first.
            payload["checkpoint"] = (
                {"_truncated": True}
                if cp is not None
                else None
            )
            payload["result"] = (
                {"_truncated": True}
                if payload.get("result") is not None
                else None
            )
            raw = json.dumps(payload, default=str, separators=(",", ":"))
            if len(raw.encode("utf-8")) > _NOTIFY_MAX_BYTES:
                payload["labels"] = []
                raw = json.dumps(payload, default=str, separators=(",", ":"))
        try:
            conn.execute(
                "SELECT pg_notify(%s, %s)",
                (JOB_EVENTS_CHANNEL, raw),
            )
        except Exception:  # noqa: BLE001 — never fail mutation for notify
            logger = __import__("logging").getLogger(__name__)
            logger.exception("pg_notify job progress failed")

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
