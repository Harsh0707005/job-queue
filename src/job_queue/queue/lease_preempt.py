"""JobQueue lease preemption domain (LeasePreemptMixin): estimates and preemption."""
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


class LeasePreemptMixin:
    def _record_job_duration(
        self,
        conn: Any,
        *,
        job: Job,
        now: datetime,
    ) -> None:
        """Update EWMA of observed runtimes for future remaining estimates."""
        started = job.lease_started_at or job.created_at
        if started is None:
            return
        duration = max(0.1, (now - _as_utc(started)).total_seconds())
        # Cap outliers so one stuck lease does not poison the estimate.
        duration = min(duration, float(job.max_runtime_seconds or 3600))
        kind = self._payload_kind(job.payload)
        existing = conn.execute(
            """
            SELECT ewma_seconds, samples
            FROM duration_estimates
            WHERE tenant_id = %s AND queue_name = %s AND kind = %s
            FOR UPDATE
            """,
            (job.tenant_id, job.queue_name, kind),
        ).fetchone()
        if existing is None:
            ewma = duration
            samples = 1
        else:
            prev = float(existing["ewma_seconds"])
            samples = int(existing["samples"]) + 1
            ewma = _EWMA_ALPHA * duration + (1.0 - _EWMA_ALPHA) * prev
        conn.execute(
            """
            INSERT INTO duration_estimates (
                tenant_id, queue_name, kind, ewma_seconds, samples, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, queue_name, kind) DO UPDATE
            SET ewma_seconds = EXCLUDED.ewma_seconds,
                samples = EXCLUDED.samples,
                updated_at = EXCLUDED.updated_at
            """,
            (job.tenant_id, job.queue_name, kind, ewma, samples, now),
        )

    def _base_estimate_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
    ) -> float:
        if row.get("estimated_duration_seconds") is not None:
            return float(row["estimated_duration_seconds"])
        payload = row.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        kind = self._payload_kind(payload)
        ewma = self._lookup_ewma_seconds(
            conn,
            tenant_id=row["tenant_id"],
            queue_name=row["queue_name"],
            kind=kind,
        )
        if ewma is not None and ewma > 0:
            return float(ewma)
        # Payload steps heuristic for demo workers.
        if isinstance(payload, dict) and payload.get("steps") is not None:
            try:
                steps = max(1, int(payload["steps"]))
                step_ms = int(payload.get("step_ms") or 200)
                return max(1.0, steps * step_ms / 1000.0)
            except (TypeError, ValueError):
                pass
        return _DEFAULT_ESTIMATE_SECONDS

    def _remaining_estimate_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> float:
        base = self._base_estimate_seconds(conn, row)
        cp = row.get("checkpoint")
        if isinstance(cp, str):
            try:
                cp = json.loads(cp)
            except json.JSONDecodeError:
                cp = None
        if isinstance(cp, dict):
            step = cp.get("step")
            total = cp.get("total")
            try:
                if step is not None and total is not None and int(total) > 0:
                    frac = min(1.0, max(0.0, int(step) / int(total)))
                    return max(0.05 * base, base * (1.0 - frac))
                if step is not None and row.get("payload"):
                    payload = row["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(payload, dict) and payload.get("steps"):
                        total_s = max(1, int(payload["steps"]))
                        frac = min(1.0, max(0.0, int(step) / total_s))
                        return max(0.05 * base, base * (1.0 - frac))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        if row.get("status") == JobStatus.LEASED.value and row.get(
            "lease_started_at"
        ):
            elapsed = (now - _as_utc(row["lease_started_at"])).total_seconds()
            return max(0.1 * base, base - elapsed)
        return base

    def _slack_seconds(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> float:
        """Seconds of schedule margin: deadline − now − remaining work.

        ``+inf`` when the job has no deadline (never deadline-urgent).
        """
        deadline = row.get("deadline_at")
        if deadline is None:
            return float("inf")
        rem = self._remaining_estimate_seconds(conn, row, now=now)
        return (_as_utc(deadline) - now).total_seconds() - rem

    def _preempt_leased_job(
        self,
        conn: Any,
        victim: dict[str, Any],
        *,
        now: datetime,
        reason: str,
    ) -> dict[str, Any] | None:
        """Park a leased job as pending; clear fence; keep checkpoint.

        Does **not** burn an attempt. Returns updated row or None if lost race.
        """
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                available_at = %s,
                preempt_count = preempt_count + 1,
                last_preempted_at = %s,
                last_error = %s,
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND lease_token IS NOT NULL
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.PENDING.value,
                now,
                now,
                reason[:500],
                now,
                victim["id"],
                JobStatus.LEASED.value,
            ),
        ).fetchone()
        if updated is not None:
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="preempted",
                from_status=JobStatus.LEASED.value,
                actor="system",
                detail={"reason": reason[:500]},
                now=now,
            )
        return updated

    def _try_preempt_for_tenant(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        now: datetime,
        urgent_hint: dict[str, Any] | None = None,
    ) -> PreemptResult | None | bool:
        """If an urgent pending job needs a slot, preempt one less-urgent lease.

        Returns :class:`PreemptResult` on success, ``None``/``False`` otherwise.
        When used as a bool (lease paths), truthy means a slot was freed.
        """
        conc = self._read_concurrency_limit(conn, tenant_id)
        if conc is None:
            return None
        leased_n = self._count_leased(conn, tenant_id)
        if leased_n < conc:
            return None

        # Claimable deadline pending ordered by urgency (low slack first).
        pending_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
              AND available_at <= %s
              AND deadline_at IS NOT NULL
              AND {_DEPS_SATISFIED_SQL}
            ORDER BY priority DESC,
              (EXTRACT(EPOCH FROM (deadline_at - %s))
                - COALESCE(estimated_duration_seconds, 60)) ASC,
              created_at ASC
            LIMIT 16
            """,
            (
                tenant_id,
                queue_name,
                JobStatus.PENDING.value,
                now,
                now,
            ),
        ).fetchall()
        if not pending_rows and urgent_hint is not None:
            if (
                urgent_hint.get("tenant_id") == tenant_id
                and urgent_hint.get("status") == JobStatus.PENDING.value
                and urgent_hint.get("deadline_at") is not None
            ):
                pending_rows = [urgent_hint]
        if not pending_rows:
            return None

        leased_rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND queue_name = %s
              AND status = %s
            FOR UPDATE
            """,
            (tenant_id, queue_name, JobStatus.LEASED.value),
        ).fetchall()
        if not leased_rows:
            return None

        # Score pending candidates.
        scored_pending: list[tuple[tuple, dict[str, Any], float]] = []
        for prow in pending_rows:
            slack = self._slack_seconds(conn, prow, now=now)
            if slack == float("inf"):
                continue
            scored_pending.append(
                (self._urgency_tuple(conn, prow, now=now), prow, slack)
            )
        if not scored_pending:
            return None
        scored_pending.sort(key=lambda x: x[0])
        urgent_key, urgent, urgent_slack = scored_pending[0]

        # Victims: preemptible, under max preempts, strictly less urgent.
        candidates: list[tuple[tuple, dict[str, Any], float]] = []
        for vrow in leased_rows:
            if not bool(vrow.get("preemptible", True)):
                continue
            if int(vrow.get("preempt_count") or 0) >= _MAX_PREEMPTS_PER_JOB:
                continue
            v_key = self._urgency_tuple(conn, vrow, now=now)
            # Strictly less urgent than the pending job.
            if v_key <= urgent_key:
                continue
            v_slack = self._slack_seconds(conn, vrow, now=now)
            rem = self._remaining_estimate_seconds(conn, vrow, now=now)
            candidates.append((v_key, vrow, rem if rem > 0 else 0.0))

        if not candidates:
            return None

        # Can we wait for the soonest less-urgent holder to finish?
        min_wait = min(c[2] for c in candidates)
        # Only preempt if waiting would risk the deadline (slack < wait).
        # Small buffer (0.5s) avoids thrash on equal estimates.
        if urgent_slack >= min_wait - 0.5:
            return None

        # Prefer victim that is least urgent, then longest remaining (free
        # more useful work later), then lowest preempt_count (anti thrash).
        def victim_rank(item: tuple) -> tuple:
            v_key, vrow, rem = item
            return (
                v_key[0],
                -v_key[1] if v_key[1] != float("inf") else -1e18,
                v_key[2],
                -rem,
                int(vrow.get("preempt_count") or 0),
            )

        # Actually sort by least urgent first: largest urgency tuple.
        candidates.sort(key=lambda x: x[0], reverse=True)
        # Among equal urgency, pick higher remaining then lower preempt_count.
        best = candidates[0]
        for c in candidates[1:]:
            if c[0] == best[0]:
                if c[2] > best[2] or (
                    c[2] == best[2]
                    and int(c[1].get("preempt_count") or 0)
                    < int(best[1].get("preempt_count") or 0)
                ):
                    best = c
            else:
                break
        _, victim, _ = best
        victim_slack = self._slack_seconds(conn, victim, now=now)

        updated = self._preempt_leased_job(
            conn,
            victim,
            now=now,
            reason=(
                f"preempted for deadline job {urgent['id']} "
                f"(urgent_slack={urgent_slack:.1f}s)"
            ),
        )
        if updated is None:
            return None
        return PreemptResult(
            tenant_id=tenant_id,
            queue_name=queue_name,
            urgent_job_id=urgent["id"],
            victim_job_id=victim["id"],
            urgent_slack_seconds=float(urgent_slack),
            victim_slack_seconds=(
                float(victim_slack)
                if victim_slack != float("inf")
                else float("inf")
            ),
        )

    def _preempt_blocked_urgent(
        self,
        conn: Any,
        *,
        queue_name: str,
        now: datetime,
        limit: int = 8,
    ) -> int:
        """Run preemption for tenants with deadline pending blocked by caps."""
        rows = conn.execute(
            """
            SELECT DISTINCT j.tenant_id
            FROM jobs j
            JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
            WHERE j.queue_name = %s
              AND j.status = %s
              AND j.deadline_at IS NOT NULL
              AND j.available_at <= %s
              AND ts.max_concurrent_leases IS NOT NULL
              AND ts.paused = FALSE
            LIMIT %s
            """,
            (
                queue_name,
                JobStatus.PENDING.value,
                now,
                max(limit * 2, 8),
            ),
        ).fetchall()
        n = 0
        for r in rows:
            if n >= limit:
                break
            tid = r["tenant_id"]
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tid,),
            )
            if self._try_preempt_for_tenant(
                conn, tenant_id=tid, queue_name=queue_name, now=now
            ):
                n += 1
        return n
