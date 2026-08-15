"""Per-tenant per-queue drain: stop intake, let existing work finish."""

from __future__ import annotations

from typing import Any

from job_queue.db import transaction
from job_queue.queue._common import (
    QueueDrainError,
    _utcnow,
)


_MAX_QUEUE_NAME = 128


def _normalize_queue_name(queue_name: str) -> str:
    name = (queue_name or "").strip()
    if not name:
        raise ValueError("queue_name is required")
    if len(name) > _MAX_QUEUE_NAME:
        raise ValueError(f"queue_name must be <= {_MAX_QUEUE_NAME} chars")
    return name


class TenantsDrainMixin:
    def drain_tenant_queue(self, tenant_id: str, queue_name: str) -> list[str]:
        """Stop new intake on one queue. Existing jobs still lease and finish.

        New enqueues, clones, in-place queue moves, DAG creates, spawn
        children, dead-letter replays, workflow starts / next forward
        steps, and schedule fires into *queue_name* are rejected or skipped. Other queues on the same
        tenant are unchanged. This is not a tenant pause: pending and
        in-flight work on the drained queue keep running.
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
            current = self._read_draining_queues(conn, tenant_id)
            if queue_name not in current:
                current.append(queue_name)
                current.sort()
                self._write_draining_queues(conn, tenant_id, current, now=now)
            return list(current)

    def undrain_tenant_queue(self, tenant_id: str, queue_name: str) -> list[str]:
        """Re-open intake on a drained queue.

        Overdue schedules for that queue are aligned to *now* so the next
        tick fires once (no catch-up flood).
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
            current = self._read_draining_queues(conn, tenant_id)
            if queue_name in current:
                current = [q for q in current if q != queue_name]
                self._write_draining_queues(conn, tenant_id, current, now=now)
                conn.execute(
                    """
                    UPDATE schedules
                    SET next_run_at = %s,
                        updated_at = %s
                    WHERE tenant_id = %s
                      AND queue_name = %s
                      AND enabled = TRUE
                      AND next_run_at < %s
                    """,
                    (now, now, tenant_id, queue_name, now),
                )
            return list(current)

    def list_draining_queues(self, tenant_id: str) -> list[str]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            return self._read_draining_queues(conn, tenant_id)

    def is_queue_draining(self, tenant_id: str, queue_name: str) -> bool:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        queue_name = _normalize_queue_name(queue_name)
        with self._pool.connection() as conn:
            return self._is_queue_draining(conn, tenant_id, queue_name)

    @staticmethod
    def _read_draining_queues(conn: Any, tenant_id: str) -> list[str]:
        row = conn.execute(
            """
            SELECT draining_queues
            FROM tenant_settings
            WHERE tenant_id = %s
            """,
            (tenant_id,),
        ).fetchone()
        if row is None or row.get("draining_queues") is None:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for raw in row["draining_queues"]:
            name = str(raw or "").strip()
            if name and name not in seen:
                seen.add(name)
                out.append(name)
        out.sort()
        return out

    @staticmethod
    def _is_queue_draining(conn: Any, tenant_id: str, queue_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM tenant_settings
            WHERE tenant_id = %s
              AND draining_queues IS NOT NULL
              AND %s = ANY(draining_queues)
            """,
            (tenant_id, queue_name),
        ).fetchone()
        return row is not None

    def _write_draining_queues(
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
                tenant_id, paused, offboarding, updated_at, draining_queues
            )
            VALUES (%s, FALSE, FALSE, %s, %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET draining_queues = EXCLUDED.draining_queues,
                updated_at = EXCLUDED.updated_at
            """,
            (tenant_id, now, stored),
        )
        if not queues:
            self._delete_empty_tenant_settings(conn, tenant_id)

    def _assert_queue_accepts_enqueue(
        self, conn: Any, tenant_id: str, queue_name: str
    ) -> None:
        if self._is_queue_draining(conn, tenant_id, queue_name):
            raise QueueDrainError(
                f"tenant {tenant_id!r} queue {queue_name!r} is draining; "
                f"new work is rejected",
                tenant_id=tenant_id,
                queue_name=queue_name,
            )
