"""JobQueue duration estimates for preemption."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _DEFAULT_ESTIMATE_SECONDS,
    _EWMA_ALPHA,
    _as_utc,
)


class LeaseEstimatesMixin:
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
        payload = self._row_plaintext_payload(row, conn=conn)
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
        if row.get("status") == JobStatus.LEASED.value and row.get("lease_started_at"):
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
