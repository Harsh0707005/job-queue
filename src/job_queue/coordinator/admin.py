"""Coordinator API handlers."""

from __future__ import annotations

from typing import Any

from job_queue.codec import (
    retention_purge_to_dict,
    tenant_stats_to_dict,
)


class AdminApiMixin:
    def _purge_expired_jobs(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        fence = self._control_fence_args()
        if fence is None:
            gated = self._control_gate()
            if gated is not None:
                return gated
            fence = {}
        try:
            result = self.queue.purge_expired_jobs(
                limit=int(body.get("limit") or 500),
                tenant_id=(str(body["tenant_id"]) if body.get("tenant_id") else None),
                **fence,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, retention_purge_to_dict(result)

    def _backlog(self) -> tuple[int, dict[str, Any]]:
        with self.queue._pool.connection() as conn:
            job_row = conn.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled
                FROM jobs
                """
            ).fetchone()
            wh_row = conn.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded
                FROM webhook_deliveries
                """
            ).fetchone()
        assert job_row is not None and wh_row is not None
        return 200, {
            "jobs": dict(job_row),
            "webhooks": dict(wh_row),
        }

    def _list_stats(self, query: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant") or [None])[0]
        if tenant:
            stats = self.queue.get_tenant_stats(tenant)
            return 200, {"stats": [tenant_stats_to_dict(stats)]}
        all_stats = self.queue.list_tenant_stats()
        return 200, {"stats": [tenant_stats_to_dict(s) for s in all_stats]}

    def _fairness_check(self) -> tuple[int, dict[str, Any]]:
        """Schedule overlap + paused claimable + completed-by-tenant for soak."""
        with self.queue._pool.connection() as conn:
            active = conn.execute(
                """
                SELECT schedule_id::text AS schedule_id, COUNT(*)::int AS n
                FROM jobs
                WHERE schedule_id IS NOT NULL
                  AND status IN ('pending', 'leased')
                GROUP BY schedule_id
                """
            ).fetchall()
            # New pending work should not appear for paused tenants (schedules
            # skip pause; reclaim also skips). In-flight leased jobs may finish.
            paused_claimable = conn.execute(
                """
                SELECT j.tenant_id, COUNT(*)::int AS n
                FROM jobs j
                JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
                WHERE ts.paused = TRUE
                  AND j.status = 'pending'
                GROUP BY j.tenant_id
                """
            ).fetchall()
            by_tenant = conn.execute(
                """
                SELECT tenant_id, COUNT(*)::int AS completed
                FROM jobs
                WHERE status = 'completed'
                GROUP BY tenant_id
                ORDER BY tenant_id
                """
            ).fetchall()
        return 200, {
            "active_jobs_by_schedule": {r["schedule_id"]: int(r["n"]) for r in active},
            "paused_tenant_claimable": {
                r["tenant_id"]: int(r["n"]) for r in paused_claimable
            },
            "completed_by_tenant": {
                r["tenant_id"]: int(r["completed"]) for r in by_tenant
            },
            "max_active_per_schedule": max((int(r["n"]) for r in active), default=0),
        }

    def _completion_order(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Completed jobs in completion time order (for priority-lane soaks)."""
        limit = int((query.get("limit") or ["500"])[0])
        limit = max(1, min(limit, 5000))
        with self.queue._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id::text AS id, tenant_id, priority, completed_at
                FROM jobs
                WHERE status = 'completed'
                  AND completed_at IS NOT NULL
                ORDER BY completed_at ASC, created_at ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return 200, {
            "completed": [
                {
                    "id": r["id"],
                    "tenant_id": r["tenant_id"],
                    "priority": int(r["priority"]),
                    "completed_at": r["completed_at"].isoformat()
                    if r["completed_at"]
                    else None,
                }
                for r in rows
            ]
        }

    def _priority_stats(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Aggregate high/low lane progress (defaults: high>=50, low<50)."""
        high_min = int((query.get("high_min") or ["50"])[0])
        with self.queue._pool.connection() as conn:
            high_c = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'completed' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            low_c = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'completed' AND priority < %s
                """,
                (high_min,),
            ).fetchone()
            high_p = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'pending' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            high_l = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'leased' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            by_tenant = conn.execute(
                """
                SELECT tenant_id, COUNT(*)::int AS n
                FROM jobs
                WHERE status = 'completed' AND priority >= %s
                GROUP BY tenant_id
                ORDER BY tenant_id
                """,
                (high_min,),
            ).fetchall()
        assert high_c is not None and low_c is not None
        assert high_p is not None and high_l is not None
        return 200, {
            "high_min": high_min,
            "high_completed": int(high_c["n"]),
            "low_completed": int(low_c["n"]),
            "high_pending": int(high_p["n"]),
            "high_leased": int(high_l["n"]),
            "high_completed_by_tenant": {
                r["tenant_id"]: int(r["n"]) for r in by_tenant
            },
        }
