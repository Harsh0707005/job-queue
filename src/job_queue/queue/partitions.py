"""Queue partitions: shard jobs and assign ownership to workers.

Jobs receive a stable ``partition_id`` at enqueue. Registered workers own a
subset of partitions (rebalanced when workers join/leave/die) and only lease
work from those shards, reducing claim contention as the fleet grows.

Unregistered workers cannot lease any shard (membership is required). When
``partition_count`` shrinks, non-terminal jobs are rematerialized into the new
range so work is never stranded on deleted shards.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import JobStatus
from job_queue.queue._common import (
    DEFAULT_PARTITION_COUNT,
    WORKER_HEARTBEAT_TTL_SECONDS,
    _normalize_skills,
    _utcnow,
)

# Statuses that can still be claimed / reclaimed / resumed — must stay inside
# the live partition range after shrink.
_REMAP_STATUSES = (
    JobStatus.PENDING.value,
    JobStatus.LEASED.value,
    JobStatus.AWAITING_APPROVAL.value,
    JobStatus.AWAITING_CHILDREN.value,
    JobStatus.FAILED.value,
)


class PartitionsMixin:
    def get_partition_count(self) -> int:
        with self._pool.connection() as conn:
            return self._partition_count(conn)

    def set_partition_count(self, count: int) -> int:
        """Set global partition count, remap stranded jobs, rebalance workers."""
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
            # Shrink (or any change) must not leave claimable jobs on deleted
            # partition ids; rematerialize into [0, n).
            self._remap_job_partitions_locked(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
        return n

    def register_worker(
        self,
        worker_id: str,
        *,
        skills: Sequence[str] | str | None = None,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Register or revive a worker and return its owned partitions.

        *skills* is the worker's capability set. Jobs listing
        ``required_skills`` are only claimable when that set is a subset.
        Re-registering replaces the advertised skills.
        """
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        skill_list = _normalize_skills(skills) if skills is not None else None
        now = _utcnow()
        ttl = (
            int(heartbeat_ttl_seconds)
            if heartbeat_ttl_seconds is not None
            else WORKER_HEARTBEAT_TTL_SECONDS
        )
        with transaction(self._pool) as conn:
            if skill_list is None:
                row = conn.execute(
                    """
                    INSERT INTO worker_nodes (
                        worker_id, status, last_seen_at, created_at, updated_at,
                        skills
                    )
                    VALUES (%s, 'active', %s, %s, %s, '{}'::text[])
                    ON CONFLICT (worker_id) DO UPDATE
                    SET status = 'active',
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = EXCLUDED.updated_at
                    RETURNING skills
                    """,
                    (wid, now, now, now),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    INSERT INTO worker_nodes (
                        worker_id, status, last_seen_at, created_at, updated_at,
                        skills
                    )
                    VALUES (%s, 'active', %s, %s, %s, %s)
                    ON CONFLICT (worker_id) DO UPDATE
                    SET status = 'active',
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = EXCLUDED.updated_at,
                        skills = EXCLUDED.skills
                    RETURNING skills
                    """,
                    (wid, now, now, now, skill_list),
                ).fetchone()
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            parts = self._owned_partitions_locked(conn, wid)
        skills_out = list((row or {}).get("skills") or skill_list or [])
        return {
            "worker_id": wid,
            "status": "active",
            "skills": skills_out,
            "partitions": parts,
            "partition_count": self.get_partition_count(),
            "generation": self._max_generation(),
        }

    def worker_heartbeat(
        self,
        worker_id: str,
        *,
        skills: Sequence[str] | str | None = None,
        heartbeat_ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Refresh liveness and return current partition ownership.

        Pass *skills* to update advertised capabilities without a full
        re-register. Omit to keep the last declared set.
        """
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        skill_list = (
            _normalize_skills(skills) if skills is not None else None
        )
        now = _utcnow()
        ttl = (
            int(heartbeat_ttl_seconds)
            if heartbeat_ttl_seconds is not None
            else WORKER_HEARTBEAT_TTL_SECONDS
        )
        with transaction(self._pool) as conn:
            if skill_list is not None:
                row = conn.execute(
                    """
                    UPDATE worker_nodes
                    SET last_seen_at = %s,
                        status = 'active',
                        updated_at = %s,
                        skills = %s
                    WHERE worker_id = %s
                    RETURNING worker_id, skills
                    """,
                    (now, now, skill_list, wid),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    UPDATE worker_nodes
                    SET last_seen_at = %s,
                        status = 'active',
                        updated_at = %s
                    WHERE worker_id = %s
                    RETURNING worker_id, skills
                    """,
                    (now, now, wid),
                ).fetchone()
            if row is None:
                # Auto-register on first heartbeat.
                inserted = conn.execute(
                    """
                    INSERT INTO worker_nodes (
                        worker_id, status, last_seen_at, created_at, updated_at,
                        skills
                    )
                    VALUES (%s, 'active', %s, %s, %s, %s)
                    RETURNING worker_id, skills
                    """,
                    (wid, now, now, now, skill_list or []),
                ).fetchone()
                row = inserted
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            parts = self._owned_partitions_locked(conn, wid)
            gen = conn.execute(
                "SELECT COALESCE(MAX(generation), 0)::bigint AS g FROM partition_assignments"
            ).fetchone()
        skills_out = list((row or {}).get("skills") or skill_list or [])
        return {
            "worker_id": wid,
            "status": "active",
            "skills": skills_out,
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

    def _remap_job_partitions_locked(
        self, conn: Any, count: int, *, now: datetime
    ) -> int:
        """Recompute partition_id for non-terminal jobs into ``[0, count)``.

        Uses the same ``hashtext(tenant:job_id) % count`` formula as enqueue so
        assignment stays consistent with newly inserted work. Returns the number
        of rows updated (including no-ops that rewrote the same value).
        """
        n = max(1, int(count))
        # Any claimable/in-flight row, plus anything already outside the range
        # (defensive if status set was incomplete).
        rows = conn.execute(
            """
            UPDATE jobs
            SET partition_id = abs(hashtext(tenant_id || ':' || id::text)) %% %s,
                updated_at = %s
            WHERE status = ANY(%s)
               OR partition_id >= %s
            RETURNING id
            """,
            (n, now, list(_REMAP_STATUSES), n),
        ).fetchall()
        return len(rows)

    def remap_job_partitions(self) -> int:
        """Public repair: rematerialize non-terminal jobs into current range."""
        now = _utcnow()
        with transaction(self._pool) as conn:
            n = self._partition_count(conn)
            return self._remap_job_partitions_locked(conn, n, now=now)

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

    def _owned_partition_generations_locked(
        self, conn: Any, worker_id: str
    ) -> dict[int, int]:
        """Map partition_id → assignment generation for this worker."""
        rows = conn.execute(
            """
            SELECT partition_id, generation
            FROM partition_assignments
            WHERE worker_id = %s
            ORDER BY partition_id ASC
            """,
            (worker_id,),
        ).fetchall()
        return {int(r["partition_id"]): int(r["generation"] or 0) for r in rows}

    def _worker_partition_filter(
        self, conn: Any, worker_id: str
    ) -> list[int] | None:
        """Return owned partitions for lease claim filtering.

        * ``[]`` — unregistered, or registered but currently owns no shard.
          Unskilled claim is denied; skilled jobs may still match via
          :meth:`_partition_claim_predicate`.
        * non-empty list — active owner: unskilled jobs only from those
          partitions; skilled jobs may cross shards.
        * ``None`` — only when ``require_worker_registration`` is False (unit
          tests that never join membership): lease any partition.

        Production default requires registration so a free-for-all worker cannot
        steal *unskilled* shards from a partitioned fleet.

        Callers must still enforce live ownership inside the claim transaction
        (see :meth:`_partition_claim_predicate` / :meth:`_guard_partition_claim`)
        because this snapshot can go stale across rebalance.
        """
        require = bool(getattr(self, "require_worker_registration", True))
        if not require:
            return None
        reg = conn.execute(
            """
            SELECT status FROM worker_nodes
            WHERE worker_id = %s
            LIMIT 1
            """,
            (worker_id,),
        ).fetchone()
        if reg is None:
            return [] if require else None
        if str(reg["status"]) != "active":
            return []
        return self._owned_partitions_locked(conn, worker_id)

    def _partition_claim_predicate(
        self,
        *,
        worker_id: str,
        owned_parts: list[int] | None,
        jobs_alias: str = "jobs",
    ) -> tuple[str, list[Any]]:
        """SQL fragment that enforces *live* ownership at claim time.

        When *owned_parts* is ``None`` (open/unpartitioned test mode) no filter
        is applied.

        Unskilled jobs (empty ``required_skills``) stay pinned to shards this
        worker currently owns. Skilled jobs may be claimed by any *active
        registered* worker that has the skills — otherwise a GPU job hashed
        onto a CPU-owned partition would sit pending forever. Unregistered
        workers still match nothing.
        """
        if owned_parts is None:
            return "", []
        skills_arr = f"COALESCE({jobs_alias}.required_skills, '{{}}'::text[])"
        unskilled = f"{skills_arr} = '{{}}'::text[]"
        skilled = f"{skills_arr} <> '{{}}'::text[]"
        active = """
              EXISTS (
                    SELECT 1
                    FROM worker_nodes wn
                    WHERE wn.worker_id = %s
                      AND wn.status = 'active'
              )
        """
        if not owned_parts:
            # Active but currently owns no shard (more workers than
            # partitions): still eligible for skilled work only.
            sql = f"""
              AND {skilled}
              AND {active}
            """
            return sql, [worker_id]
        sql = f"""
              AND (
                    (
                        {unskilled}
                        AND {jobs_alias}.partition_id = ANY(%s)
                        AND EXISTS (
                            SELECT 1
                            FROM partition_assignments pa
                            WHERE pa.partition_id = {jobs_alias}.partition_id
                              AND pa.worker_id = %s
                        )
                    )
                 OR (
                        {skilled}
                        AND {active}
                    )
              )
        """
        return sql, [list(owned_parts), worker_id, worker_id]

    def _guard_partition_claim(
        self,
        conn: Any,
        *,
        worker_id: str,
        partition_id: int,
        expected_generation: int | None = None,
        require_registration: bool | None = None,
    ) -> None:
        """Abort claim if the worker no longer owns *partition_id* (rebalance).

        Takes ``FOR SHARE`` on the assignment row so concurrent rebalance
        (which updates owner/generation) serializes correctly with the claim.
        When *expected_generation* is set, a generation mismatch also aborts —
        the worker's view of the shard is stale even if ownership flipped back.
        """
        from job_queue.queue._common import LeaseError

        require = (
            bool(require_registration)
            if require_registration is not None
            else bool(getattr(self, "require_worker_registration", True))
        )
        if not require:
            # Open mode (unit tests): no membership fence.
            reg = conn.execute(
                """
                SELECT 1 FROM worker_nodes
                WHERE worker_id = %s AND status = 'active'
                LIMIT 1
                """,
                (worker_id,),
            ).fetchone()
            if reg is None:
                return

        row = conn.execute(
            """
            SELECT worker_id, generation
            FROM partition_assignments
            WHERE partition_id = %s
            FOR SHARE
            """,
            (int(partition_id),),
        ).fetchone()
        if row is None or row.get("worker_id") != worker_id:
            raise LeaseError(
                f"partition {partition_id} not owned by worker {worker_id!r} "
                f"(rebalanced or unassigned)"
            )
        if expected_generation is not None:
            cur = int(row.get("generation") or 0)
            if cur != int(expected_generation):
                raise LeaseError(
                    f"partition {partition_id} generation mismatch "
                    f"(worker={expected_generation} current={cur}); rebalance fence"
                )

    def _max_generation(self) -> int:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(generation), 0)::bigint AS g FROM partition_assignments"
            ).fetchone()
        return int(row["g"] if row else 0)
