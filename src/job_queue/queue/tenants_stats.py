"""JobQueue tenant stats domain: get/list tenant stats."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from job_queue.models import (
    JobStatus,
    TenantStats,
)
from job_queue.queue._common import (
    _DEPS_SATISFIED_SQL,
    _as_utc,
    _utcnow,
)


class TenantsStatsMixin:
    def get_tenant_stats(self, tenant_id: str) -> TenantStats:
        """Return job counts and control flags for one tenant."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with self._pool.connection() as conn:
            return self._load_tenant_stats(conn, tenant_id, now=now)

    def list_tenant_stats(self) -> list[TenantStats]:
        """Stats for every tenant that has jobs and/or settings."""
        now = _utcnow()
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT tenant_id FROM jobs
                UNION
                SELECT tenant_id FROM tenant_settings
                ORDER BY tenant_id
                """
            ).fetchall()
            return [
                self._load_tenant_stats(conn, r["tenant_id"], now=now) for r in rows
            ]

    def _load_tenant_stats(
        self,
        conn: Any,
        tenant_id: str,
        *,
        now: datetime,
    ) -> TenantStats:
        counts = {
            JobStatus.PENDING.value: 0,
            JobStatus.LEASED.value: 0,
            JobStatus.COMPLETED.value: 0,
            JobStatus.FAILED.value: 0,
            JobStatus.DEAD.value: 0,
            JobStatus.CANCELLED.value: 0,
        }
        rows = conn.execute(
            """
            SELECT status, COUNT(*)::int AS n
            FROM jobs
            WHERE tenant_id = %s
            GROUP BY status
            """,
            (tenant_id,),
        ).fetchall()
        for r in rows:
            counts[r["status"]] = int(r["n"])

        settings = conn.execute(
            """
            SELECT max_concurrent_leases, max_pending_jobs, paused,
                   enqueue_rate_per_sec, enqueue_burst,
                   lease_rate_per_sec, lease_burst,
                   breaker_opened_at, breaker_cooldown_seconds
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        paused = bool(settings["paused"]) if settings else False
        breaker_open = False
        if settings and settings.get("breaker_opened_at") is not None:
            cooldown = settings.get("breaker_cooldown_seconds")
            if cooldown is None:
                breaker_open = True
            else:
                breaker_open = _as_utc(settings["breaker_opened_at"]) + timedelta(
                    seconds=int(cooldown)
                ) > now
        max_conc = settings["max_concurrent_leases"] if settings else None
        max_pending = settings["max_pending_jobs"] if settings else None
        enq_rate = (
            float(settings["enqueue_rate_per_sec"])
            if settings and settings["enqueue_rate_per_sec"] is not None
            else None
        )
        enq_burst = (
            int(settings["enqueue_burst"])
            if settings and settings["enqueue_burst"] is not None
            else None
        )
        lease_rate = (
            float(settings["lease_rate_per_sec"])
            if settings and settings["lease_rate_per_sec"] is not None
            else None
        )
        lease_burst = (
            int(settings["lease_burst"])
            if settings and settings["lease_burst"] is not None
            else None
        )

        if paused or breaker_open:
            claimable = 0
        else:
            claimable_row = conn.execute(
                f"""
                SELECT COUNT(*)::int AS n
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND available_at <= %s
                  AND {_DEPS_SATISFIED_SQL}
                """,
                (tenant_id, JobStatus.PENDING.value, now),
            ).fetchone()
            claimable = int(claimable_row["n"]) if claimable_row else 0

        total = sum(counts.values())
        return TenantStats(
            tenant_id=tenant_id,
            paused=paused,
            max_concurrent_leases=max_conc,
            max_pending_jobs=max_pending,
            enqueue_rate_per_sec=enq_rate,
            enqueue_burst=enq_burst,
            lease_rate_per_sec=lease_rate,
            lease_burst=lease_burst,
            pending=counts[JobStatus.PENDING.value],
            leased=counts[JobStatus.LEASED.value],
            completed=counts[JobStatus.COMPLETED.value],
            failed=counts[JobStatus.FAILED.value],
            dead=counts[JobStatus.DEAD.value],
            cancelled=counts[JobStatus.CANCELLED.value],
            total=total,
            claimable=claimable,
            awaiting_skills=int(counts.get(JobStatus.AWAITING_SKILLS.value) or 0),
            breaker_open=breaker_open,
        )
