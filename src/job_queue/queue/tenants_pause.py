"""JobQueue tenant pause/resume and pause flags."""

from __future__ import annotations

from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import (
    _utcnow,
)


class TenantsPauseMixin:
    def pause_tenant(self, tenant_id: str) -> None:
        """Pause a tenant: no new leases, no schedule fires, no reclaim.

        * Workers (scoped or fair) will not lease pending jobs for this tenant.
        * Expired leases for this tenant will not be reclaimed by anyone until
          resume (holders may still heartbeat / ack / nack while the lease lives).
        * Recurring schedules for this tenant are skipped (no tick backlog).
        * Manual enqueue still works; jobs wait as pending until resume.
        * No new webhook deliveries are enqueued; pending webhook rows are not
          claimed/sent until resume.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, TRUE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = TRUE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )

    def resume_tenant(self, tenant_id: str) -> None:
        """Resume leasing and schedule ticks for a paused tenant.

        Overdue schedules keep their past ``next_run_at`` so each
        schedule's misfire policy (skip / catch_up_one / catch_up) runs
        on the next ``process_due_schedules``. Cannot resume a tenant
        that is offboarding.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding and cannot be resumed"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is restoring and cannot be resumed"
                )
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET paused = FALSE,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now),
            )
            self._delete_empty_tenant_settings(conn, tenant_id)

    def is_tenant_paused(self, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._is_tenant_paused(conn, tenant_id)

    def is_tenant_offboarding(self, tenant_id: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._is_tenant_offboarding(conn, tenant_id)

    @staticmethod
    def _is_tenant_paused(conn: Any, tenant_id: str) -> bool:
        row = conn.execute(
            """
            SELECT paused
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return False
        return bool(row["paused"])

    @staticmethod
    def _is_tenant_restoring(conn: Any, tenant_id: str) -> bool:
        row = conn.execute(
            """
            SELECT restoring FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        return bool(row and row.get("restoring"))

    @staticmethod
    def _is_tenant_offboarding(conn: Any, tenant_id: str) -> bool:
        row = conn.execute(
            """
            SELECT offboarding
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None:
            return False
        return bool(row["offboarding"])
