"""JobQueue misc domain (MiscMixin)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.models import (
    JobStatus,
)


class MiscMixin:
    @staticmethod
    def _payload_kind(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        if payload.get("_jq_enc") == 1 or payload.get("_encrypted") is True:
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
              AND status = ANY(%s)
            """,
            (
                tenant_id,
                [JobStatus.PENDING.value, JobStatus.AWAITING_SKILLS.value],
            ),
        ).fetchone()
        return int(row["n"]) if row else 0
