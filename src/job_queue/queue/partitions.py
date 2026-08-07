"""Queue partitions: shard jobs and assign ownership to workers.

Jobs receive a stable ``partition_id`` at enqueue. Registered workers own a
subset of partitions (rebalanced when workers join/leave/die) and only lease
work from those shards, reducing claim contention as the fleet grows.

Unregistered workers (unit tests / legacy) may still lease any partition.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.queue._common import (
    DEFAULT_PARTITION_COUNT,
    WORKER_HEARTBEAT_TTL_SECONDS,
    _utcnow,
)


class PartitionsMixin:
    def get_partition_count(self) -> int:
        with self._pool.connection() as conn:
            return self._partition_count(conn)

    def set_partition_count(self, count: int) -> int:
        """Set global partition count and rebalance active workers."""
        n = int(count)
        if n < 1 or n > 4096:
            raise ValueError("partition_count must be in 1..4096")
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE queue_settings
                SET partition_count = %s, updated_at = %s
                WHERE id = 1
                """,
                (n, now),
            )
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
        return n

    def register_worker(
        self,
        worker_id: str,
        *,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Register or revive a worker and return its owned partitions."""
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        now = _utcnow()
        ttl = (
            int(heartbeat_ttl_seconds)
            if heartbeat_ttl_seconds is not None
            else WORKER_HEARTBEAT_TTL_SECONDS
        )
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO worker_nodes (worker_id, status, last_seen_at, created_at, updated_at)
                VALUES (%s, 'active', %s, %s, %s)
                ON CONFLICT (worker_id) DO UPDATE
                SET status = 'active',
                    last_seen_at = EXCLUDED.last_seen_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (wid, now, now, now),
            )
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            parts = self._owned_partitions_locked(conn, wid)
        return {
            "worker_id": wid,
            "status": "active",
            "partitions": parts,
            "partition_count": self.get_partition_count(),
            "generation": self._max_generation(),
        }

    def worker_heartbeat(
        self,
        worker_id: str,
        *,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Refresh liveness and return current partition ownership."""
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        now = _utcnow()
        ttl = (
            int(heartbeat_ttl_seconds)
            if heartbeat_ttl_seconds is not None
            else WORKER_HEARTBEAT_TTL_SECONDS
        )
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                UPDATE worker_nodes
                SET last_seen_at = %s,
                    status = 'active',
                    updated_at = %s
                WHERE worker_id = %s
                RETURNING worker_id
                """,
                (now, now, wid),
            ).fetchone()
            if row is None:
                # Auto-register on first heartbeat.
                conn.execute(
                    """
                    INSERT INTO worker_nodes (worker_id, status, last_seen_at, created_at, updated_at)
                    VALUES (%s, 'active', %s, %s, %s)
                    """,
                    (wid, now, now, now),
                )
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            parts = self._owned_partitions_locked(conn, wid)
            gen = conn.execute(
                "SELECT COALESCE(MAX(generation), 0)::bigint AS g FROM partition_assignments"
            ).fetchone()
        return {
            "worker_id": wid,
            "status": "active",
            "partitions": parts,
            "partition_count": n,
            "generation": int(gen["g"] if gen else 0),
        }

    def deregister_worker(self, worker_id: str) -> dict[str, Any]:
        """Mark worker dead and rebalance its partitions to survivors."""
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                UPDATE worker_nodes
                SET status = 'dead', updated_at = %s
                WHERE worker_id = %s
                """,
                (now, wid),
            )
            self._rebalance_partitions_locked(conn, now=now)
        return {"worker_id": wid, "status": "dead"}

    def list_worker_partitions(self, worker_id: str) -> list[int]:
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        with self._pool.connection() as conn:
            return self._owned_partitions_locked(conn, str(worker_id).strip())

    def list_partition_assignments(self) -> list[dict[str, Any]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT partition_id, worker_id, generation, updated_at
                FROM partition_assignments
                ORDER BY partition_id ASC
                """
            ).fetchall()
        return [
            {
                "partition_id": int(r["partition_id"]),
                "worker_id": r.get("worker_id"),
                "generation": int(r["generation"] or 0),
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]

    def process_worker_timeouts(
        self, *, heartbeat_ttl_seconds: int | None = None
    ) -> int:
        """Mark stale workers dead and rebalance. Returns number marked dead."""
        now = _utcnow()
        ttl = (
            int(heartbeat_ttl_seconds)
            if heartbeat_ttl_seconds is not None
            else WORKER_HEARTBEAT_TTL_SECONDS
        )
        with transaction(self._pool) as conn:
            n = self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            if n:
                self._rebalance_partitions_locked(conn, now=now)
            return n

    # --- internals ---

    def _partition_count(self, conn: Any) -> int:
        row = conn.execute(
            "SELECT partition_count FROM queue_settings WHERE id = 1"
        ).fetchone()
        if row is None or row.get("partition_count") is None:
            return DEFAULT_PARTITION_COUNT
        return max(1, int(row["partition_count"]))

    def _compute_partition_id(
        self, conn: Any, *, tenant_id: str, job_id: UUID
    ) -> int:
        n = self._partition_count(conn)
        row = conn.execute(
            "SELECT abs(hashtext(%s)) %% %s AS p",
            (f"{tenant_id}:{job_id}", n),
        ).fetchone()
        return int(row["p"] if row else 0)

    def _ensure_partition_rows(
        self, conn: Any, count: int, *, now: datetime
    ) -> None:
        # Drop obsolete high partitions.
        conn.execute(
            "DELETE FROM partition_assignments WHERE partition_id >= %s",
            (count,),
        )
        for p in range(count):
            conn.execute(
                """
                INSERT INTO partition_assignments (partition_id, worker_id, generation, updated_at)
                VALUES (%s, NULL, 0, %s)
                ON CONFLICT (partition_id) DO NOTHING
                """,
                (p, now),
            )

    def _expire_stale_workers_locked(
        self, conn: Any, *, now: datetime, ttl_seconds: int
    ) -> int:
        cutoff = now - timedelta(seconds=max(1, int(ttl_seconds)))
        rows = conn.execute(
            """
            UPDATE worker_nodes
            SET status = 'dead', updated_at = %s
            WHERE status = 'active'
              AND last_seen_at < %s
            RETURNING worker_id
            """,
            (now, cutoff),
        ).fetchall()
        return len(rows)

    def _rebalance_partitions_locked(self, conn: Any, *, now: datetime) -> None:
        """Assign partitions round-robin across active workers (stable sort)."""
        n = self._partition_count(conn)
        self._ensure_partition_rows(conn, n, now=now)
        active = conn.execute(
            """
            SELECT worker_id
            FROM worker_nodes
            WHERE status = 'active'
            ORDER BY worker_id ASC
            """
        ).fetchall()
        workers = [r["worker_id"] for r in active]
        if not workers:
            conn.execute(
                """
                UPDATE partition_assignments
                SET worker_id = NULL,
                    generation = generation + 1,
                    updated_at = %s
                WHERE worker_id IS NOT NULL
                """,
                (now,),
            )
            return
        for p in range(n):
            owner = workers[p % len(workers)]
            conn.execute(
                """
                UPDATE partition_assignments
                SET worker_id = %s,
                    generation = CASE
                        WHEN worker_id IS DISTINCT FROM %s THEN generation + 1
                        ELSE generation
                    END,
                    updated_at = %s
                WHERE partition_id = %s
                """,
                (owner, owner, now, p),
            )

    def _owned_partitions_locked(self, conn: Any, worker_id: str) -> list[int]:
        rows = conn.execute(
            """
            SELECT partition_id
            FROM partition_assignments
            WHERE worker_id = %s
            ORDER BY partition_id ASC
            """,
            (worker_id,),
        ).fetchall()
        return [int(r["partition_id"]) for r in rows]

    def _worker_partition_filter(
        self, conn: Any, worker_id: str
    ) -> list[int] | None:
        """Return owned partitions, empty list if dead, or None if unregistered.

        * ``None`` — never registered (unit tests / legacy): lease any partition.
        * ``[]`` — registered but dead/inactive: lease nothing (do not steal
          shards after rebalance or timeout).
        * non-empty list — active owner: lease only those partitions.
        """
        reg = conn.execute(
            """
            SELECT status FROM worker_nodes
            WHERE worker_id = %s
            LIMIT 1
            """,
            (worker_id,),
        ).fetchone()
        if reg is None:
            return None
        if str(reg["status"]) != "active":
            return []
        return self._owned_partitions_locked(conn, worker_id)

    def _max_generation(self) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(generation), 0)::bigint AS g FROM partition_assignments"
            ).fetchone()
        return int(row["g"] if row else 0)
