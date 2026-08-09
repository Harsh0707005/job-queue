"""Per-tenant default job TTL / expiry action."""

from __future__ import annotations

from job_queue.db import transaction
from job_queue.queue._common import _utcnow
from job_queue.queue.jobs_expire import normalize_expiry_action


class TenantsTtlMixin:
    def set_tenant_job_ttl(
        self,
        tenant_id: str,
        ttl_seconds: int | None,
        *,
        expiry_action: str | None = None,
    ) -> dict[str, int | str | None]:
        """Default finish-by TTL for new jobs that omit ``ttl_seconds``.

        ``None`` clears the default (new jobs have no TTL unless set
        per-job). *expiry_action* is ``cancel`` or ``dead`` (default
        ``dead`` when a TTL is set). Pass *expiry_action* alone to change
        the action while keeping the current TTL.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if ttl_seconds is not None and int(ttl_seconds) < 1:
            raise ValueError("ttl_seconds must be >= 1 or None")
        action = (
            None if expiry_action is None else normalize_expiry_action(expiry_action)
        )
        now = _utcnow()
        with transaction(self._pool) as conn:
            existing = conn.execute(
                """
                SELECT default_job_ttl_seconds, default_expiry_action
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            new_ttl = (
                None
                if ttl_seconds is None
                else int(ttl_seconds)
            )
            # Allow set_tenant_job_ttl(tid, None) to clear TTL. If caller
            # only wants to set action, they pass the current ttl again.
            new_action = action
            if new_action is None and existing is not None:
                new_action = existing.get("default_expiry_action")
            if new_ttl is not None and new_action is None:
                new_action = "dead"
            if new_ttl is None:
                new_action = None
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, max_concurrent_leases, max_pending_jobs,
                    paused, offboarding, updated_at,
                    default_job_ttl_seconds, default_expiry_action
                )
                VALUES (%s, NULL, NULL, FALSE, FALSE, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET default_job_ttl_seconds = EXCLUDED.default_job_ttl_seconds,
                    default_expiry_action = EXCLUDED.default_expiry_action,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now, new_ttl, new_action),
            )
            if new_ttl is None:
                self._delete_empty_tenant_settings(conn, tenant_id)
        return {
            "tenant_id": tenant_id,
            "ttl_seconds": new_ttl,
            "expiry_action": new_action,
        }

    def get_tenant_job_ttl(self, tenant_id: str) -> dict[str, int | str | None]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT default_job_ttl_seconds, default_expiry_action
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
        if row is None:
            return {
                "tenant_id": tenant_id,
                "ttl_seconds": None,
                "expiry_action": None,
            }
        return {
            "tenant_id": tenant_id,
            "ttl_seconds": (
                None
                if row.get("default_job_ttl_seconds") is None
                else int(row["default_job_ttl_seconds"])
            ),
            "expiry_action": row.get("default_expiry_action"),
        }
