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
    WorkerError,
    _JOB_COLUMNS,
    _normalize_skills,
    _utcnow,
)
from job_queue.queue.skills_wait import SkillsWaitMixin

# Statuses that can still be claimed / reclaimed / resumed — must stay inside
# the live partition range after shrink.
_REMAP_STATUSES = (
    JobStatus.PENDING.value,
    JobStatus.LEASED.value,
    JobStatus.AWAITING_APPROVAL.value,
    JobStatus.AWAITING_CHILDREN.value,
    JobStatus.AWAITING_SKILLS.value,
    JobStatus.FAILED.value,
)


class PartitionsMixin(SkillsWaitMixin):
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
                    SET status = CASE
                            WHEN worker_nodes.maintenance THEN 'draining'
                            ELSE 'active'
                        END,
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = EXCLUDED.updated_at
                    RETURNING worker_id, skills, status, maintenance, last_seen_at
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
                    SET status = CASE
                            WHEN worker_nodes.maintenance THEN 'draining'
                            ELSE 'active'
                        END,
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = EXCLUDED.updated_at,
                        skills = EXCLUDED.skills
                    RETURNING worker_id, skills, status, maintenance, last_seen_at
                    """,
                    (wid, now, now, now, skill_list),
                ).fetchone()
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            skills_out = list((row or {}).get("skills") or skill_list or [])
            self._sync_skills_wait_locked(
                conn, now=now, worker_skills=skills_out
            )
            snap = self._worker_snapshot_locked(conn, wid, row=row)
            snap["partition_count"] = n
            snap["generation"] = self._max_generation_locked(conn)
        self._rescan_awaiting_skills_after_commit()
        return snap

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
                        status = CASE
                            WHEN maintenance THEN 'draining'
                            ELSE 'active'
                        END,
                        updated_at = %s,
                        skills = %s
                    WHERE worker_id = %s
                    RETURNING worker_id, skills, status, maintenance, last_seen_at
                    """,
                    (now, now, skill_list, wid),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    UPDATE worker_nodes
                    SET last_seen_at = %s,
                        status = CASE
                            WHEN maintenance THEN 'draining'
                            ELSE 'active'
                        END,
                        updated_at = %s
                    WHERE worker_id = %s
                    RETURNING worker_id, skills, status, maintenance, last_seen_at
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
                    RETURNING worker_id, skills, status, maintenance, last_seen_at
                    """,
                    (wid, now, now, now, skill_list or []),
                ).fetchone()
                row = inserted
            self._expire_stale_workers_locked(conn, now=now, ttl_seconds=ttl)
            n = self._partition_count(conn)
            self._ensure_partition_rows(conn, n, now=now)
            self._rebalance_partitions_locked(conn, now=now)
            skills_out = list((row or {}).get("skills") or skill_list or [])
            self._sync_skills_wait_locked(
                conn, now=now, worker_skills=skills_out
            )
            snap = self._worker_snapshot_locked(conn, wid, row=row)
            snap["partition_count"] = n
            snap["generation"] = self._max_generation_locked(conn)
        self._rescan_awaiting_skills_after_commit()
        return snap

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
            self._sync_skills_wait_locked(conn, now=now)
        self._rescan_awaiting_skills_after_commit()
        return {"worker_id": wid, "status": "dead"}

    def set_worker_maintenance(
        self,
        worker_id: str,
        *,
        draining: bool,
        release_held: bool | None = None,
    ) -> dict[str, Any]:
        """Stop or resume new leases for a registered worker.

        Draining workers keep heartbeating, keep their skills, and take no new
        work. They are dropped from partition assignment so peers take new
        shard work. By default they finish jobs they already hold. Pass
        ``release_held=True`` (or enable the queue default) to return those
        leases to ``pending`` immediately so another worker can claim them.
        The old lease token is cleared, so a late ack/heartbeat from the
        draining worker fails closed (no double-complete, no nack-loss).
        ``release_held=None`` inherits :meth:`get_worker_drain_release_held`.
        Resume clears the flag and they start leasing again without
        re-registering.
        """
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        now = _utcnow()
        released: list[dict[str, Any]] = []
        want_release = False
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                SELECT worker_id, skills, status, maintenance, last_seen_at
                FROM worker_nodes
                WHERE worker_id = %s
                FOR UPDATE
                """,
                (wid,),
            ).fetchone()
            if row is None:
                raise WorkerError(
                    f"worker {wid!r} is not registered",
                    code="worker_not_found",
                )
            # Dead rows keep their liveness; only flip the operator flag so a
            # later heartbeat revives as draining (or active) with skills intact.
            if draining:
                conn.execute(
                    """
                    UPDATE worker_nodes
                    SET maintenance = TRUE,
                        status = CASE
                            WHEN status = 'dead' THEN 'dead'
                            ELSE 'draining'
                        END,
                        updated_at = %s
                    WHERE worker_id = %s
                    """,
                    (now, wid),
                )
                if release_held is None:
                    want_release = self._worker_drain_release_held_locked(conn)
                else:
                    want_release = bool(release_held)
                if want_release:
                    released = self._release_worker_held_leases_locked(
                        conn, wid, now=now
                    )
            else:
                conn.execute(
                    """
                    UPDATE worker_nodes
                    SET maintenance = FALSE,
                        status = CASE
                            WHEN status = 'dead' THEN 'dead'
                            ELSE 'active'
                        END,
                        updated_at = %s
                    WHERE worker_id = %s
                    """,
                    (now, wid),
                )
            self._rebalance_partitions_locked(conn, now=now)
            self._sync_skills_wait_locked(conn, now=now)
            snap = self._worker_snapshot_locked(conn, wid)
            snap["partition_count"] = self._partition_count(conn)
            snap["generation"] = self._max_generation_locked(conn)
            snap["released"] = len(released)
            snap["released_job_ids"] = [str(r["id"]) for r in released]
            snap["release_held"] = bool(want_release)
        self._rescan_awaiting_skills_after_commit()
        return snap

    def release_worker_held_leases(self, worker_id: str) -> dict[str, Any]:
        """Return every job this worker currently holds to ``pending``.

        Only a worker already in maintenance may drop inflight leases.
        Dropping them on an *active* worker would let the same process
        reclaim the jobs while its old handler is still running.
        The lease token is cleared under ``FOR UPDATE``, so a late ack
        from the draining worker fails with :class:`LeaseError`.
        """
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                """
                SELECT worker_id, status, maintenance
                FROM worker_nodes
                WHERE worker_id = %s
                FOR UPDATE
                """,
                (wid,),
            ).fetchone()
            if row is None:
                raise WorkerError(
                    f"worker {wid!r} is not registered",
                    code="worker_not_found",
                )
            if not (
                str(row["status"]) == "draining" or bool(row.get("maintenance"))
            ):
                raise WorkerError(
                    f"worker {wid!r} is not in maintenance; "
                    "drain it before releasing held leases",
                    code="worker_not_draining",
                )
            released = self._release_worker_held_leases_locked(
                conn, wid, now=now
            )
            snap = self._worker_snapshot_locked(conn, wid)
        snap["released"] = len(released)
        snap["released_job_ids"] = [str(r["id"]) for r in released]
        return snap

    def get_worker_drain_release_held(self) -> bool:
        with self._pool.connection() as conn:
            return self._worker_drain_release_held_locked(conn)

    def set_worker_drain_release_held(self, enabled: bool) -> bool:
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                """
                INSERT INTO queue_settings (id, worker_drain_release_held, updated_at)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET worker_drain_release_held = EXCLUDED.worker_drain_release_held,
                    updated_at = EXCLUDED.updated_at
                """,
                (bool(enabled), now),
            )
            return self._worker_drain_release_held_locked(conn)

    def get_worker(self, worker_id: str) -> dict[str, Any] | None:
        if not worker_id or not str(worker_id).strip():
            raise ValueError("worker_id is required")
        wid = str(worker_id).strip()
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                SELECT worker_id, skills, status, maintenance, last_seen_at
                FROM worker_nodes
                WHERE worker_id = %s
                """,
                (wid,),
            ).fetchone()
            if row is None:
                return None
            return self._worker_snapshot_locked(conn, wid, row=row)

    def list_workers(self) -> list[dict[str, Any]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT worker_id, skills, status, maintenance, last_seen_at
                FROM worker_nodes
                ORDER BY worker_id ASC
                """
            ).fetchall()
            return [
                self._worker_snapshot_locked(conn, str(r["worker_id"]), row=r)
                for r in rows
            ]

    def list_draining_workers(self) -> list[str]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT worker_id
                FROM worker_nodes
                WHERE status = 'draining'
                ORDER BY worker_id ASC
                """
            ).fetchall()
        return [str(r["worker_id"]) for r in rows]

    def worker_is_draining(self, worker_id: str) -> bool:
        if not worker_id or not str(worker_id).strip():
            return False
        with self._pool.connection() as conn:
            return self._worker_is_draining_locked(conn, str(worker_id).strip())

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
                self._sync_skills_wait_locked(conn, now=now)
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
            WHERE status IN ('active', 'draining')
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
            # Open/unpartitioned test mode: still refuse maintenance.
            return (
                """
              AND NOT EXISTS (
                    SELECT 1
                    FROM worker_nodes wn
                    WHERE wn.worker_id = %s
                      AND (wn.status = 'draining' OR wn.maintenance)
              )
                """,
                [worker_id],
            )
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
        # Jobs that already died (or whose lease expired) may hop to any
        # active worker so the previous holder cannot monopolize retries.
        hop_open = f"""
            (
                COALESCE(array_length({jobs_alias}.failed_workers, 1), 0) > 0
                OR {jobs_alias}.status = 'leased'
            )
        """
        # Last-success worker may claim an opted-in similar job off-shard
        # while they are still the live preferred holder.
        affinity_open = f"""
            (
                (
                    COALESCE({jobs_alias}.prefer_last_worker, FALSE)
                    OR EXISTS (
                        SELECT 1
                        FROM tenant_settings ts
                        WHERE ts.tenant_id = {jobs_alias}.tenant_id
                          AND ts.prefer_last_worker_queues IS NOT NULL
                          AND {jobs_alias}.queue_name
                                = ANY(ts.prefer_last_worker_queues)
                    )
                )
                AND EXISTS (
                    SELECT 1
                    FROM job_worker_affinity a
                    JOIN worker_nodes wn ON wn.worker_id = a.last_worker_id
                    WHERE a.tenant_id = {jobs_alias}.tenant_id
                      AND a.queue_name = {jobs_alias}.queue_name
                      AND a.kind = COALESCE({jobs_alias}.payload_kind, '')
                      AND a.last_worker_id = %s
                      AND wn.status = 'active'
                )
            )
        """
        if not owned_parts:
            # Active but currently owns no shard (more workers than
            # partitions): still eligible for skilled work and hop-open jobs.
            sql = f"""
              AND (
                    {skilled}
                    OR {hop_open}
                    OR {affinity_open}
              )
              AND {active}
            """
            return sql, [worker_id, worker_id]
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
                 OR (
                        {hop_open}
                        AND {active}
                    )
                 OR (
                        {affinity_open}
                        AND {active}
                    )
              )
        """
        return sql, [
            list(owned_parts),
            worker_id,
            worker_id,
            worker_id,
            worker_id,
            worker_id,
        ]

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
            return self._max_generation_locked(conn)

    @staticmethod
    def _max_generation_locked(conn: Any) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(generation), 0)::bigint AS g FROM partition_assignments"
        ).fetchone()
        return int(row["g"] if row else 0)

    @staticmethod
    def _worker_drain_release_held_locked(conn: Any) -> bool:
        row = conn.execute(
            """
            SELECT worker_drain_release_held
            FROM queue_settings
            WHERE id = 1
            """
        ).fetchone()
        if row is None or row.get("worker_drain_release_held") is None:
            return False
        return bool(row["worker_drain_release_held"])

    def _release_worker_held_leases_locked(
        self, conn: Any, worker_id: str, *, now: datetime
    ) -> list[dict[str, Any]]:
        """Fence-clear every live lease for *worker_id* back to pending.

        ``FOR UPDATE`` serializes with ack/heartbeat. Jobs that already
        completed under the same lock are skipped (not lost, not doubled).
        """
        locked = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE leased_by = %s
              AND status = %s
            FOR UPDATE
            """,
            (worker_id, JobStatus.LEASED.value),
        ).fetchall()
        if not locked:
            return []
        released: list[dict[str, Any]] = []
        tenants: set[str] = set()
        for row in locked:
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    leased_by = NULL,
                    lease_token = NULL,
                    lease_tenant_id = NULL,
                    lease_expires_at = NULL,
                    lease_started_at = NULL,
                    available_at = %s,
                    last_error = COALESCE(%s, last_error),
                    updated_at = %s
                WHERE id = %s
                  AND lease_token = %s
                  AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.PENDING.value,
                    now,
                    "maintenance_release",
                    now,
                    row["id"],
                    row["lease_token"],
                    JobStatus.LEASED.value,
                ),
            ).fetchone()
            if updated is None:
                # Concurrent ack won the row; job is completed, not lost.
                continue
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="released",
                    from_status=JobStatus.LEASED.value,
                    actor=worker_id,
                    detail={"reason": "maintenance_release"},
                    now=now,
                )
            if hasattr(self, "_record_lease_end_usage"):
                self._record_lease_end_usage(
                    conn, row, now=now, releases=1
                )
            tenants.add(str(updated["tenant_id"]))
            released.append(dict(updated))
        if hasattr(self, "_maybe_finalize_offboard"):
            for tid in tenants:
                self._maybe_finalize_offboard(conn, tenant_id=tid, now=now)
        return released

    @staticmethod
    def _worker_is_draining_locked(conn: Any, worker_id: str) -> bool:
        row = conn.execute(
            """
            SELECT status, maintenance
            FROM worker_nodes
            WHERE worker_id = %s
            LIMIT 1
            """,
            (worker_id,),
        ).fetchone()
        if row is None:
            return False
        return str(row["status"]) == "draining" or bool(row.get("maintenance"))

    def _worker_snapshot_locked(
        self,
        conn: Any,
        worker_id: str,
        *,
        row: Any | None = None,
    ) -> dict[str, Any]:
        if row is None:
            row = conn.execute(
                """
                SELECT worker_id, skills, status, maintenance, last_seen_at
                FROM worker_nodes
                WHERE worker_id = %s
                """,
                (worker_id,),
            ).fetchone()
        if row is None:
            raise WorkerError(
                f"worker {worker_id!r} is not registered",
                code="worker_not_found",
            )
        status = str(row["status"])
        skills = list(row.get("skills") or [])
        last_seen = row.get("last_seen_at")
        leased = conn.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM jobs
            WHERE leased_by = %s
              AND status = %s
            """,
            (worker_id, JobStatus.LEASED.value),
        ).fetchone()
        return {
            "worker_id": str(row["worker_id"]),
            "status": status,
            "draining": status == "draining",
            "maintenance": bool(row.get("maintenance")),
            "skills": skills,
            "partitions": self._owned_partitions_locked(conn, worker_id),
            "leased": int(leased["n"] if leased else 0),
            "last_seen_at": last_seen,
        }
