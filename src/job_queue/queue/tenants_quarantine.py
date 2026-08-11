"""Per-tenant default poison-job hop quarantine."""

from __future__ import annotations

from job_queue.db import transaction
from job_queue.queue._common import _utcnow
from job_queue.queue.lease_quarantine import normalize_quarantine_action


class TenantsQuarantineMixin:
    def set_tenant_quarantine(
        self,
        tenant_id: str,
        max_worker_hops: int | None,
        *,
        action: str | None = None,
    ) -> dict[str, int | str | None]:
        """Default hop cap for new jobs that omit ``max_worker_hops``.

        After this many dying claims (nack, lease timeout, max-runtime) the
        job is quarantined so it stops hopping. ``action`` is ``dead``
        (dead-letter) or ``release`` (park as ``quarantined``, not claimable).
        ``None`` hops clears the default (no hop quarantine unless set per-job).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_worker_hops is not None and int(max_worker_hops) < 1:
            raise ValueError("max_worker_hops must be >= 1 or None")
        now = _utcnow()
        with transaction(self._pool) as conn:
            existing = conn.execute(
                """
                SELECT default_max_worker_hops, default_quarantine_action
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            new_hops = None if max_worker_hops is None else int(max_worker_hops)
            new_action = (
                None if action is None else normalize_quarantine_action(action)
            )
            if new_action is None and existing is not None:
                new_action = existing.get("default_quarantine_action")
            if new_hops is not None and new_action is None:
                new_action = "dead"
            if new_hops is None:
                new_action = None
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, paused, offboarding, updated_at,
                    default_max_worker_hops, default_quarantine_action
                )
                VALUES (%s, FALSE, FALSE, %s, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET default_max_worker_hops = EXCLUDED.default_max_worker_hops,
                    default_quarantine_action = EXCLUDED.default_quarantine_action,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now, new_hops, new_action),
            )
            if new_hops is None:
                self._delete_empty_tenant_settings(conn, tenant_id)
        return {
            "tenant_id": tenant_id,
            "max_worker_hops": new_hops,
            "quarantine_action": new_action,
        }

    def get_tenant_quarantine(self, tenant_id: str) -> dict[str, int | str | None]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT default_max_worker_hops, default_quarantine_action
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
        if row is None:
            return {
                "tenant_id": tenant_id,
                "max_worker_hops": None,
                "quarantine_action": None,
            }
        return {
            "tenant_id": tenant_id,
            "max_worker_hops": (
                None
                if row.get("default_max_worker_hops") is None
                else int(row["default_max_worker_hops"])
            ),
            "quarantine_action": row.get("default_quarantine_action"),
        }
