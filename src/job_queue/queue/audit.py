"""JobQueue audit domain (AuditMixin)."""
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

class AuditMixin:
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


    @staticmethod
    def _row_to_audit(row: dict[str, Any]) -> JobAuditEvent:
        detail = row.get("detail") or {}
        if isinstance(detail, str):
            try:
                detail = json.loads(detail)
            except json.JSONDecodeError:
                detail = {}
        if not isinstance(detail, dict):
            detail = {"value": detail}
        return JobAuditEvent(
            event_id=row["event_id"],
            tenant_id=row["tenant_id"],
            job_id=row["job_id"],
            seq=int(row["seq"]),
            event_type=row["event_type"],
            from_status=row.get("from_status"),
            to_status=row.get("to_status"),
            actor=row.get("actor"),
            detail=detail,
            created_at=row["created_at"],
        )


    def list_job_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        after_seq: int = 0,
        limit: int = 1000,
    ) -> list[JobAuditEvent]:
        """Replay-ordered audit events for one job (tenant-authorized).

        Authorization: the job's **current** owner must be *tenant_id*. Events
        themselves keep the tenant id at write time (immutable), so after
        ``merge_tenant`` pre-merge rows still store the source tenant. This
        method returns the **full** per-job ``seq`` stream (all tenants on
        the event rows) so history is continuous across ownership transfer.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if after_seq < 0:
            raise ValueError("after_seq must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._pool.connection() as conn:
            if not self._tenant_can_read_job_audit(
                conn, job_id=job_id, tenant_id=tenant_id
            ):
                return []
            rows = conn.execute(
                """
                SELECT
                    event_id, tenant_id, job_id, seq, event_type,
                    from_status, to_status, actor, detail, created_at
                FROM job_audit_events
                WHERE job_id = %s
                  AND seq > %s
                ORDER BY seq ASC
                LIMIT %s
                """,
                (job_id, after_seq, limit),
            ).fetchall()
            return [self._row_to_audit(r) for r in rows]


    def list_tenant_audit(
        self,
        tenant_id: str,
        *,
        after_created_at: datetime | None = None,
        after_event_id: UUID | None = None,
        limit: int = 500,
    ) -> list[JobAuditEvent]:
        """Tenant-wide audit stream ordered by time (for catch-up consumers).

        Includes:
        * events originally written under *tenant_id*, and
        * events for jobs **currently** owned by *tenant_id* (so after a
          merge, the target tenant can catch up pre-merge history of jobs
          they now own without mutating immutable event rows).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        # Events visible to this tenant: written under them, or belonging to
        # a job they currently own (covers merge without rewriting history).
        ownership = """
            (
                e.tenant_id = %s
                OR EXISTS (
                    SELECT 1 FROM jobs j
                    WHERE j.id = e.job_id AND j.tenant_id = %s
                )
            )
        """
        with self._pool.connection() as conn:
            if after_created_at is None:
                rows = conn.execute(
                    f"""
                    SELECT
                        e.event_id, e.tenant_id, e.job_id, e.seq, e.event_type,
                        e.from_status, e.to_status, e.actor, e.detail, e.created_at
                    FROM job_audit_events e
                    WHERE {ownership}
                    ORDER BY e.created_at ASC, e.event_id ASC
                    LIMIT %s
                    """,
                    (tenant_id, tenant_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                        e.event_id, e.tenant_id, e.job_id, e.seq, e.event_type,
                        e.from_status, e.to_status, e.actor, e.detail, e.created_at
                    FROM job_audit_events e
                    WHERE {ownership}
                      AND (
                            e.created_at > %s
                            OR (
                                e.created_at = %s
                                AND e.event_id > %s
                            )
                          )
                    ORDER BY e.created_at ASC, e.event_id ASC
                    LIMIT %s
                    """,
                    (
                        tenant_id,
                        tenant_id,
                        after_created_at,
                        after_created_at,
                        after_event_id or UUID(int=0),
                        limit,
                    ),
                ).fetchall()
            return [self._row_to_audit(r) for r in rows]


    def verify_job_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
    ) -> AuditConsistencyReport:
        """Replay full audit for *job_id* and compare final status to the job row.

        Uses the complete per-job ``seq`` stream (including pre-merge events
        still stamped with a prior tenant). Access requires current ownership.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            job_row = conn.execute(
                "SELECT id, tenant_id, status FROM jobs WHERE id = %s AND tenant_id = %s",
                (job_id, tenant_id),
            ).fetchone()
            if job_row is None:
                return AuditConsistencyReport(
                    job_id=job_id,
                    tenant_id=tenant_id,
                    ok=False,
                    job_status=None,
                    audit_final_status=None,
                    event_count=0,
                    gaps=(),
                    error="job not found for tenant",
                )
            # Full job history by job_id (not filtered by event.tenant_id).
            events = conn.execute(
                """
                SELECT seq, to_status, event_type, tenant_id
                FROM job_audit_events
                WHERE job_id = %s
                ORDER BY seq ASC
                """,
                (job_id,),
            ).fetchall()

        if not events:
            return AuditConsistencyReport(
                job_id=job_id,
                tenant_id=tenant_id,
                ok=False,
                job_status=job_row["status"],
                audit_final_status=None,
                event_count=0,
                gaps=(),
                error="no audit events for job",
            )

        gaps: list[int] = []
        expected = 1
        last_status: str | None = None
        for ev in events:
            seq = int(ev["seq"])
            if seq != expected:
                gaps.append(seq)
            expected = seq + 1
            if ev["to_status"] is not None:
                last_status = ev["to_status"]

        job_status = job_row["status"]
        ok = not gaps and last_status == job_status
        err = None
        if gaps:
            err = f"seq gaps near {gaps[:5]!r}"
        elif last_status != job_status:
            err = (
                f"audit final status {last_status!r} != job status {job_status!r}"
            )
        return AuditConsistencyReport(
            job_id=job_id,
            tenant_id=tenant_id,
            ok=ok,
            job_status=job_status,
            audit_final_status=last_status,
            event_count=len(events),
            gaps=tuple(gaps),
            error=err,
        )


    @staticmethod
    def _tenant_can_read_job_audit(
        conn: Any,
        *,
        job_id: UUID,
        tenant_id: str,
    ) -> bool:
        """True if *tenant_id* currently owns the job (post-merge target)."""
        row = conn.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND tenant_id = %s",
            (job_id, tenant_id),
        ).fetchone()
        return row is not None


    def verify_tenant_audit(
        self,
        tenant_id: str,
        *,
        limit: int = 10_000,
    ) -> list[AuditConsistencyReport]:
        """Verify audit consistency for up to *limit* jobs of a tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id FROM jobs
                WHERE tenant_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (tenant_id, limit),
            ).fetchall()
        return [
            self.verify_job_audit(r["id"], tenant_id=tenant_id) for r in rows
        ]


    def rebuild_job_from_audit(
        self,
        job_id: UUID,
        *,
        tenant_id: str | None = None,
    ) -> Job:
        """Reconstruct a missing job row from its immutable audit stream.

        Use after a bad migrate / partial write lost the ``jobs`` row while
        ``job_audit_events`` remain. Rebuild:

        1. Loads **all** events for *job_id* (any event.tenant_id — covers merge).
        2. Validates contiguous ``seq`` from 1, valid transitions, and that
           every event embeds a ``detail.job`` snapshot (required for rebuild).
        3. Uses the **last** snapshot as the reconstructed row (includes
           checkpoint / approval / lease fields as of that event).
        4. Refuses to insert on any inconsistency (raises :class:`RebuildError`).
        5. Refuses if a job row already exists for this id.
        6. Authorizes: last snapshot's ``tenant_id`` must match *tenant_id*
           when provided (current owner after merges).

        Appends a ``rebuilt`` audit event with the restored snapshot.
        """
        with transaction(self._pool) as conn:
            existing = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s FOR UPDATE",
                (job_id,),
            ).fetchone()
            if existing is not None:
                raise RebuildError(
                    f"job {job_id} already exists "
                    f"(tenant={existing['tenant_id']!r}, "
                    f"status={existing['status']!r}); refuse to overwrite",
                    job_id=job_id,
                )

            rows = conn.execute(
                """
                SELECT
                    event_id, tenant_id, job_id, seq, event_type,
                    from_status, to_status, actor, detail, created_at
                FROM job_audit_events
                WHERE job_id = %s
                ORDER BY seq ASC
                FOR UPDATE
                """,
                (job_id,),
            ).fetchall()
            events = [self._row_to_audit(r) for r in rows]
            snap = self._validate_audit_for_rebuild(job_id, events)

            if tenant_id is not None and snap["tenant_id"] != tenant_id:
                raise RebuildError(
                    f"job {job_id} rebuild owner is {snap['tenant_id']!r}, "
                    f"not {tenant_id!r}",
                    job_id=job_id,
                    events=events,
                )

            now = _utcnow()
            self._insert_job_from_snapshot(conn, snap)
            inserted = conn.execute(
                f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s",
                (job_id,),
            ).fetchone()
            assert inserted is not None
            self._append_job_audit(
                conn,
                job_row=inserted,
                event_type="rebuilt",
                from_status=inserted["status"],
                actor="system",
                detail={"source": "audit_rebuild", "events": len(events)},
                now=now,
            )
            return _row_to_job(inserted)


    def rebuild_jobs_from_audit(
        self,
        job_ids: list[UUID],
        *,
        tenant_id: str | None = None,
    ) -> list[Job]:
        """Rebuild multiple missing jobs; stops on first :class:`RebuildError`."""
        out: list[Job] = []
        for jid in job_ids:
            out.append(self.rebuild_job_from_audit(jid, tenant_id=tenant_id))
        return out


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


    def _validate_audit_for_rebuild(
        self,
        job_id: UUID,
        events: list[JobAuditEvent],
    ) -> dict[str, Any]:
        """Validate event stream; return last job snapshot or raise RebuildError."""
        if not events:
            raise RebuildError(
                f"job {job_id} has no audit events",
                job_id=job_id,
            )
        for i, ev in enumerate(events, start=1):
            if ev.seq != i:
                raise RebuildError(
                    f"job {job_id} audit seq gap: expected {i}, got {ev.seq}",
                    job_id=job_id,
                    events=events,
                )
            if ev.job_id != job_id:
                raise RebuildError(
                    f"job {job_id} audit event {ev.seq} has wrong job_id "
                    f"{ev.job_id}",
                    job_id=job_id,
                    events=events,
                )
        if events[0].event_type != "enqueued":
            raise RebuildError(
                f"job {job_id} first audit event must be enqueued, "
                f"got {events[0].event_type!r}",
                job_id=job_id,
                events=events,
            )
        if events[0].from_status is not None:
            raise RebuildError(
                f"job {job_id} enqueued event must have from_status=null",
                job_id=job_id,
                events=events,
            )

        prev_status: str | None = None
        last_snap: dict[str, Any] | None = None
        for ev in events:
            snap = (ev.detail or {}).get("job")
            if not isinstance(snap, dict):
                raise RebuildError(
                    f"job {job_id} audit seq={ev.seq} event_type={ev.event_type!r} "
                    f"missing detail.job snapshot (cannot rebuild safely)",
                    job_id=job_id,
                    events=events,
                )
            if str(snap.get("id")) != str(job_id):
                raise RebuildError(
                    f"job {job_id} snapshot id mismatch at seq={ev.seq}",
                    job_id=job_id,
                    events=events,
                )
            snap_status = snap.get("status")
            if snap_status != ev.to_status:
                raise RebuildError(
                    f"job {job_id} seq={ev.seq}: snapshot status {snap_status!r} "
                    f"!= event to_status {ev.to_status!r}",
                    job_id=job_id,
                    events=events,
                )
            if prev_status is not None and ev.from_status != prev_status:
                if not (
                    ev.from_status == ev.to_status
                    and ev.from_status == prev_status
                ):
                    raise RebuildError(
                        f"job {job_id} seq={ev.seq}: from_status {ev.from_status!r} "
                        f"does not continue previous to_status {prev_status!r}",
                        job_id=job_id,
                        events=events,
                    )
            if ev.event_type == "tenant_merged":
                ft = (ev.detail or {}).get("from_tenant")
                tt = (ev.detail or {}).get("to_tenant")
                if not ft or not tt:
                    raise RebuildError(
                        f"job {job_id} tenant_merged seq={ev.seq} missing "
                        f"from_tenant/to_tenant",
                        job_id=job_id,
                        events=events,
                    )
                if snap.get("tenant_id") != tt:
                    raise RebuildError(
                        f"job {job_id} tenant_merged seq={ev.seq}: snapshot "
                        f"tenant {snap.get('tenant_id')!r} != to_tenant {tt!r}",
                        job_id=job_id,
                        events=events,
                    )
            self._assert_snapshot_status_fields(job_id, snap, events)
            prev_status = ev.to_status
            last_snap = snap

        assert last_snap is not None
        return last_snap


    def _assert_snapshot_status_fields(
        self,
        job_id: UUID,
        snap: dict[str, Any],
        events: list[JobAuditEvent],
    ) -> None:
        """Reject snapshots that violate lease/status invariants."""
        status = snap.get("status")
        try:
            JobStatus(status)
        except ValueError as exc:
            raise RebuildError(
                f"job {job_id} invalid status {status!r} in snapshot",
                job_id=job_id,
                events=events,
            ) from exc

        leased_by = snap.get("leased_by")
        lease_token = snap.get("lease_token")
        if status == JobStatus.LEASED.value:
            if not leased_by or not lease_token:
                raise RebuildError(
                    f"job {job_id} leased snapshot missing leased_by/lease_token",
                    job_id=job_id,
                    events=events,
                )
        elif status in (
            JobStatus.PENDING.value,
            JobStatus.AWAITING_APPROVAL.value,
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.DEAD.value,
            JobStatus.CANCELLED.value,
        ):
            if lease_token is not None or leased_by is not None:
                raise RebuildError(
                    f"job {job_id} status={status!r} snapshot still has "
                    f"lease fields set",
                    job_id=job_id,
                    events=events,
                )
        if status == JobStatus.COMPLETED.value and not snap.get("completed_at"):
            raise RebuildError(
                f"job {job_id} completed snapshot missing completed_at",
                job_id=job_id,
                events=events,
            )
        if not snap.get("tenant_id") or not snap.get("queue_name"):
            raise RebuildError(
                f"job {job_id} snapshot missing tenant_id/queue_name",
                job_id=job_id,
                events=events,
            )
        if not isinstance(snap.get("payload"), dict):
            raise RebuildError(
                f"job {job_id} snapshot payload must be an object",
                job_id=job_id,
                events=events,
            )


    def _insert_job_from_snapshot(
        self,
        conn: Any,
        snap: dict[str, Any],
    ) -> None:
        """Insert a jobs row from a validated audit snapshot."""

        def _parse_dt(key: str) -> datetime | None:
            raw = snap.get(key)
            if raw is None:
                return None
            if isinstance(raw, datetime):
                return _as_utc(raw)
            return _as_utc(datetime.fromisoformat(str(raw).replace("Z", "+00:00")))

        def _parse_uuid(key: str) -> UUID | None:
            raw = snap.get(key)
            if raw is None:
                return None
            return UUID(str(raw))

        deps_raw = snap.get("depends_on") or []
        deps = [UUID(str(d)) for d in deps_raw]
        waiting_raw = snap.get("waiting_on") or []
        waiting = [UUID(str(d)) for d in waiting_raw]
        labels_raw = snap.get("labels") or []
        labels = [str(x) for x in labels_raw]
        payload = snap["payload"]
        result = snap.get("result")
        checkpoint = snap.get("checkpoint")

        conn.execute(
            """
            INSERT INTO jobs (
                id, tenant_id, queue_name, payload, status, priority,
                attempts, max_attempts, visibility_timeout_seconds,
                max_runtime_seconds,
                retry_backoff_base_seconds, retry_backoff_max_seconds,
                available_at, dedupe_key, schedule_id, depends_on,
                parent_job_id, waiting_on, labels,
                leased_by, lease_token, lease_tenant_id,
                lease_expires_at, lease_started_at,
                created_at, updated_at, completed_at, result,
                checkpoint, checkpointed_at,
                deadline_at, estimated_duration_seconds, preemptible,
                preempt_count, last_preempted_at,
                approval_requested_at, approval_deadline_at,
                approval_message, approval_outcome, approval_resolved_at,
                await_children_started_at, await_children_deadline_at,
                await_children_outcome, await_children_resolved_at,
                last_error
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s
            )
            """,
            (
                UUID(str(snap["id"])),
                snap["tenant_id"],
                snap["queue_name"],
                Jsonb(payload),
                snap["status"],
                int(snap.get("priority") or 0),
                int(snap.get("attempts") or 0),
                int(snap.get("max_attempts") or 3),
                int(snap.get("visibility_timeout_seconds") or 30),
                int(snap.get("max_runtime_seconds") or 3600),
                int(snap.get("retry_backoff_base_seconds") or 1),
                int(snap.get("retry_backoff_max_seconds") or 300),
                _parse_dt("available_at") or _utcnow(),
                snap.get("dedupe_key"),
                _parse_uuid("schedule_id"),
                deps,
                _parse_uuid("parent_job_id"),
                waiting,
                labels,
                snap.get("leased_by"),
                _parse_uuid("lease_token"),
                snap.get("lease_tenant_id"),
                _parse_dt("lease_expires_at"),
                _parse_dt("lease_started_at"),
                _parse_dt("created_at") or _utcnow(),
                _parse_dt("updated_at") or _utcnow(),
                _parse_dt("completed_at"),
                Jsonb(result) if result is not None else None,
                Jsonb(checkpoint) if checkpoint is not None else None,
                _parse_dt("checkpointed_at"),
                _parse_dt("deadline_at"),
                (
                    None
                    if snap.get("estimated_duration_seconds") is None
                    else int(snap["estimated_duration_seconds"])
                ),
                bool(snap.get("preemptible", True)),
                int(snap.get("preempt_count") or 0),
                _parse_dt("last_preempted_at"),
                _parse_dt("approval_requested_at"),
                _parse_dt("approval_deadline_at"),
                snap.get("approval_message"),
                snap.get("approval_outcome"),
                _parse_dt("approval_resolved_at"),
                _parse_dt("await_children_started_at"),
                _parse_dt("await_children_deadline_at"),
                snap.get("await_children_outcome"),
                _parse_dt("await_children_resolved_at"),
                snap.get("last_error"),
            ),
        )


