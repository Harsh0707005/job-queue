"""Per-tenant last-success worker preference (queue opt-in + hold)."""

from __future__ import annotations

from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import (
    _DEFAULT_AFFINITY_HOLD_SECONDS,
    _utcnow,
)
from job_queue.queue.tenants_drain import _normalize_queue_name


_MAX_HOLD = 3600


class TenantsAffinityMixin:
    def set_queue_worker_affinity(
        self,
        tenant_id: str,
        queue_name: str,
        enabled: bool,
    ) -> dict[str, Any]:
        """Prefer the last worker that completed similar work on this queue.

        Preference is ranking only. If that worker is dead, unskilled, or
        the hold window has passed, any capable worker may claim the job.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        queue_name = _normalize_queue_name(queue_name)
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            queues = self._read_affinity_queues(conn, tenant_id)
            if enabled and queue_name not in queues:
                queues.append(queue_name)
                queues.sort()
                self._write_affinity_queues(conn, tenant_id, queues, now=now)
            elif not enabled and queue_name in queues:
                queues = [q for q in queues if q != queue_name]
                self._write_affinity_queues(conn, tenant_id, queues, now=now)
            return self._affinity_state(conn, tenant_id)

    def set_prefer_last_worker_hold(
        self, tenant_id: str, seconds: int | None
    ) -> dict[str, Any]:
        """Seconds a pending job ranks the last worker first. None = default 10."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if seconds is not None:
            seconds = int(seconds)
            if seconds < 0 or seconds > _MAX_HOLD:
                raise ValueError(
                    f"prefer_last_worker_hold_seconds must be 0..{_MAX_HOLD} or None"
                )
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO tenant_settings (
                    tenant_id, paused, offboarding, updated_at,
                    prefer_last_worker_hold_seconds
                )
                VALUES (%s, FALSE, FALSE, %s, %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET prefer_last_worker_hold_seconds
                        = EXCLUDED.prefer_last_worker_hold_seconds,
                    updated_at = EXCLUDED.updated_at
                """,
                (tenant_id, now, seconds),
            )
            if seconds is None:
                self._delete_empty_tenant_settings(conn, tenant_id)
            return self._affinity_state(conn, tenant_id)

    def get_queue_worker_affinity(self, tenant_id: str) -> dict[str, Any]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._affinity_state(conn, tenant_id)

    def _affinity_state(self, conn: Any, tenant_id: str) -> dict[str, Any]:
        queues = self._read_affinity_queues(conn, tenant_id)
        row = conn.execute(
            """
            SELECT prefer_last_worker_hold_seconds
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        hold = _DEFAULT_AFFINITY_HOLD_SECONDS
        if row is not None and row.get("prefer_last_worker_hold_seconds") is not None:
            hold = int(row["prefer_last_worker_hold_seconds"])
        return {
            "tenant_id": tenant_id,
            "queues": queues,
            "hold_seconds": hold,
        }

    @staticmethod
    def _read_affinity_queues(conn: Any, tenant_id: str) -> list[str]:
        row = conn.execute(
            """
            SELECT prefer_last_worker_queues
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row.get("prefer_last_worker_queues") is None:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for raw in row["prefer_last_worker_queues"]:
            name = str(raw or "").strip()
            if name and name not in seen:
                seen.add(name)
                out.append(name)
        out.sort()
        return out

    def _write_affinity_queues(
        self,
        conn: Any,
        tenant_id: str,
        queues: list[str],
        *,
        now,
    ) -> None:
        stored = queues or None
        conn.execute(
            """
            INSERT INTO tenant_settings (
                tenant_id, paused, offboarding, updated_at,
                prefer_last_worker_queues
            )
            VALUES (%s, FALSE, FALSE, %s, %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET prefer_last_worker_queues = EXCLUDED.prefer_last_worker_queues,
                updated_at = EXCLUDED.updated_at
            """,
            (tenant_id, now, stored),
        )
        if not queues:
            self._delete_empty_tenant_settings(conn, tenant_id)

    def _job_allows_affinity_hop(
        self, conn: Any, row: dict[str, Any], worker_id: str
    ) -> bool:
        """True when this worker is the live last-success holder for the job."""
        enabled = bool(row.get("prefer_last_worker"))
        if not enabled:
            qs = conn.execute(
                """
                SELECT prefer_last_worker_queues
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (row["tenant_id"],),
            ).fetchone()
            opted = list((qs or {}).get("prefer_last_worker_queues") or [])
            enabled = str(row["queue_name"]) in opted
        if not enabled:
            return False
        hit = conn.execute(
            """
            SELECT 1
            FROM job_worker_affinity a
            JOIN worker_nodes wn ON wn.worker_id = a.last_worker_id
            WHERE a.tenant_id = %s
              AND a.queue_name = %s
              AND a.kind = %s
              AND a.last_worker_id = %s
              AND wn.status = 'active'
            """,
            (
                row["tenant_id"],
                row["queue_name"],
                str(row.get("payload_kind") or ""),
                worker_id,
            ),
        ).fetchone()
        return hit is not None

    def _record_last_success_worker(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str,
        kind: str,
        worker_id: str,
        now,
    ) -> None:
        if not tenant_id or not worker_id:
            return
        conn.execute(
            """
            INSERT INTO job_worker_affinity (
                tenant_id, queue_name, kind, last_worker_id, last_success_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, queue_name, kind) DO UPDATE
            SET last_worker_id = EXCLUDED.last_worker_id,
                last_success_at = EXCLUDED.last_success_at
            """,
            (tenant_id, queue_name, kind or "", worker_id, now),
        )
