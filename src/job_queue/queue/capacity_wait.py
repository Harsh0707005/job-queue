"""Park jobs no worker can cover; revive when advertised capacity arrives."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _resource_map_from_row,
    _utcnow,
)
from job_queue.queue.skills_wait import SkillsWaitMixin


_CAPACITY_WAIT_MSG = "waiting for worker capacity"

# One *single* active worker must have the job's skills *and* advertise
# enough of every cost key. Skills on one node and capacity on another
# is not cover — nobody can actually lease the job.
_CAPACITY_COVERED_SQL = """
    EXISTS (
        SELECT 1
        FROM worker_nodes wn
        WHERE wn.status = 'active'
          AND COALESCE(j.required_skills, '{}'::text[])
              <@ COALESCE(wn.skills, '{}'::text[])
          AND NOT EXISTS (
                SELECT 1
                FROM jsonb_each_text(COALESCE(j.resource_cost, '{}'::jsonb)) e
                WHERE (e.value)::numeric > 0
                  AND (
                        NOT (COALESCE(wn.capacity, '{}'::jsonb) ? e.key)
                        OR COALESCE((wn.capacity->>e.key)::numeric, 0)
                            < (e.value)::numeric
                  )
          )
    )
"""

_HAS_COST_SQL = """
    EXISTS (
        SELECT 1
        FROM jsonb_each_text(COALESCE(j.resource_cost, '{}'::jsonb)) e
        WHERE (e.value)::numeric > 0
    )
"""


class CapacityWaitMixin(SkillsWaitMixin):
    def process_awaiting_skills(
        self, *, limit: int = 200, max_passes: int = 32
    ) -> dict[str, int]:
        totals = super().process_awaiting_skills(
            limit=limit, max_passes=max_passes
        )
        cap = self.process_awaiting_capacity(
            limit=limit, max_passes=max_passes
        )
        totals["parked"] += int(cap.get("parked") or 0)
        totals["revived"] += int(cap.get("revived") or 0)
        totals["dead"] += int(cap.get("dead") or 0)
        return totals

    def process_awaiting_capacity(
        self, *, limit: int = 200, max_passes: int = 32
    ) -> dict[str, int]:
        """Park uncoverable costed jobs; revive when a worker can cover them."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if max_passes < 1:
            raise ValueError("max_passes must be >= 1")
        totals = {"dead": 0, "parked": 0, "revived": 0}
        for _ in range(int(max_passes)):
            now = _utcnow()
            with transaction(self._pool) as conn:
                dead = self._expire_awaiting_capacity_deadlines_locked(
                    conn, now=now, limit=limit
                )
                parked = self._park_unfulfillable_capacity_locked(
                    conn, now=now, limit=limit
                )
                revived = self._revive_awaiting_capacity_for_fleet_locked(
                    conn, now=now, limit=limit
                )
            totals["dead"] += len(dead)
            totals["parked"] += parked
            totals["revived"] += revived
            if not dead and parked == 0 and revived == 0:
                break
        return totals

    def list_awaiting_capacity(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s AND status = %s
                ORDER BY created_at ASC, id ASC
                LIMIT %s OFFSET %s
                """,
                (tenant_id, JobStatus.AWAITING_CAPACITY.value, limit, offset),
            ).fetchall()
            return [self._job(r) for r in rows]

    def _enqueue_status_for_skills(
        self,
        conn: Any,
        skills: list[str],
        resource_cost: dict[str, int] | None = None,
    ) -> str:
        status = super()._enqueue_status_for_skills(conn, skills)
        if status != JobStatus.PENDING.value:
            return status
        cost = {
            str(k): int(v)
            for k, v in (resource_cost or {}).items()
            if int(v) > 0
        }
        if cost and not self._capacity_is_covered(
            conn, cost, skills=skills
        ):
            return JobStatus.AWAITING_CAPACITY.value
        return JobStatus.PENDING.value

    def _sync_skills_wait_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        worker_skills: list[str] | None = None,
        limit: int = 200,
    ) -> None:
        super()._sync_skills_wait_locked(
            conn, now=now, worker_skills=worker_skills, limit=limit
        )
        self._sync_capacity_wait_locked(conn, now=now, limit=limit)

    def _sync_capacity_wait_locked(
        self, conn: Any, *, now: datetime, limit: int = 200
    ) -> None:
        self._revive_awaiting_capacity_for_fleet_locked(
            conn, now=now, limit=limit
        )
        self._park_unfulfillable_capacity_locked(conn, now=now, limit=limit)

    def _recheck_awaiting_skills_row_locked(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> dict[str, Any]:
        row = super()._recheck_awaiting_skills_row_locked(conn, row, now=now)
        return self._recheck_awaiting_capacity_row_locked(conn, row, now=now)

    def _recheck_awaiting_capacity_row_locked(
        self,
        conn: Any,
        row: dict[str, Any],
        *,
        now: datetime,
    ) -> dict[str, Any]:
        if row is None:
            return row
        status = str(row.get("status") or "")
        cost = _resource_map_from_row(row.get("resource_cost"))
        skills = [str(s) for s in (row.get("required_skills") or [])]
        if status == JobStatus.PENDING.value:
            if cost and not self._capacity_is_covered(
                conn, cost, skills=skills
            ):
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s AND status = %s
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.AWAITING_CAPACITY.value,
                        _CAPACITY_WAIT_MSG,
                        now,
                        row["id"],
                        JobStatus.PENDING.value,
                    ),
                ).fetchone()
                if updated is None:
                    return row
                if hasattr(self, "_append_job_audit"):
                    self._append_job_audit(
                        conn,
                        job_row=updated,
                        event_type="awaiting_capacity",
                        from_status=JobStatus.PENDING.value,
                        actor=None,
                        detail={"reason": "no_active_worker_covers_job"},
                        now=now,
                    )
                return updated
            return row
        if status != JobStatus.AWAITING_CAPACITY.value:
            return row
        if cost and not self._capacity_is_covered(
            conn, cost, skills=skills
        ):
            if row.get("last_error") == _CAPACITY_WAIT_MSG:
                return row
            stamped = conn.execute(
                f"""
                UPDATE jobs
                SET last_error = %s,
                    updated_at = %s
                WHERE id = %s AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    _CAPACITY_WAIT_MSG,
                    now,
                    row["id"],
                    JobStatus.AWAITING_CAPACITY.value,
                ),
            ).fetchone()
            return stamped or row
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                last_error = NULL,
                updated_at = %s
            WHERE id = %s AND status = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                JobStatus.PENDING.value,
                now,
                row["id"],
                JobStatus.AWAITING_CAPACITY.value,
            ),
        ).fetchone()
        if updated is None:
            return row
        if hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="capacity_available",
                from_status=JobStatus.AWAITING_CAPACITY.value,
                actor=None,
                detail={"revived": True, "source": "enqueue_recheck"},
                now=now,
            )
        return updated

    @staticmethod
    def _capacity_is_covered(
        conn: Any,
        cost: dict[str, int],
        skills: list[str] | None = None,
    ) -> bool:
        needed = {k: int(v) for k, v in (cost or {}).items() if int(v) > 0}
        if not needed:
            return True
        needed_skills = {str(s) for s in (skills or [])}
        rows = conn.execute(
            """
            SELECT capacity, skills
            FROM worker_nodes
            WHERE status = 'active'
            """
        ).fetchall()
        for row in rows:
            have = {str(s) for s in (row.get("skills") or [])}
            if needed_skills and not needed_skills <= have:
                continue
            cap = _resource_map_from_row(row.get("capacity"))
            if not cap:
                continue
            if all(
                int(cap.get(name, 0)) >= amount
                for name, amount in needed.items()
            ):
                return True
        return False

    def _park_unfulfillable_capacity_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        limit: int = 200,
    ) -> int:
        rows = conn.execute(
            f"""
            WITH stuck AS (
                SELECT j.id
                FROM jobs j
                WHERE j.status = %s
                  AND {_HAS_COST_SQL}
                  AND NOT ({_CAPACITY_COVERED_SQL})
                ORDER BY j.created_at ASC
                LIMIT %s
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE jobs j
            SET status = %s,
                last_error = %s,
                updated_at = %s
            FROM stuck
            WHERE j.id = stuck.id
            RETURNING j.*
            """,
            (
                JobStatus.PENDING.value,
                int(limit),
                JobStatus.AWAITING_CAPACITY.value,
                _CAPACITY_WAIT_MSG,
                now,
            ),
        ).fetchall()
        for row in rows:
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=row,
                    event_type="awaiting_capacity",
                    from_status=JobStatus.PENDING.value,
                    actor=None,
                    detail={"reason": "no_active_worker_covers_job"},
                    now=now,
                )
        return len(rows)

    def _revive_awaiting_capacity_for_fleet_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        limit: int = 200,
    ) -> int:
        rows = conn.execute(
            f"""
            WITH due AS (
                SELECT j.id
                FROM jobs j
                WHERE j.status = %s
                  AND (
                        NOT ({_HAS_COST_SQL})
                        OR {_CAPACITY_COVERED_SQL}
                  )
                ORDER BY j.created_at ASC
                LIMIT %s
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE jobs j
            SET status = %s,
                last_error = NULL,
                updated_at = %s
            FROM due
            WHERE j.id = due.id
            RETURNING j.*
            """,
            (
                JobStatus.AWAITING_CAPACITY.value,
                int(limit),
                JobStatus.PENDING.value,
                now,
            ),
        ).fetchall()
        for row in rows:
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=row,
                    event_type="capacity_available",
                    from_status=JobStatus.AWAITING_CAPACITY.value,
                    actor=None,
                    detail={"revived": True, "source": "fleet"},
                    now=now,
                )
        return len(rows)

    def _expire_awaiting_capacity_deadlines_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> list[Job]:
        if tenant_id is not None:
            sel = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status = %s
                  AND deadline_at IS NOT NULL
                  AND deadline_at < %s
                ORDER BY deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    tenant_id,
                    JobStatus.AWAITING_CAPACITY.value,
                    now,
                    int(limit),
                ),
            ).fetchall()
        else:
            sel = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE status = %s
                  AND deadline_at IS NOT NULL
                  AND deadline_at < %s
                ORDER BY deadline_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (
                    JobStatus.AWAITING_CAPACITY.value,
                    now,
                    int(limit),
                ),
            ).fetchall()
        out: list[Job] = []
        for row in sel:
            cost = _resource_map_from_row(row.get("resource_cost"))
            detail = ",".join(f"{k}={v}" for k, v in cost.items())
            err = "no worker covers resource cost before deadline: " + detail
            updated = conn.execute(
                f"""
                UPDATE jobs
                SET status = %s,
                    last_error = %s,
                    completed_at = %s,
                    updated_at = %s
                WHERE id = %s AND status = %s
                RETURNING {_JOB_COLUMNS}
                """,
                (
                    JobStatus.DEAD.value,
                    err[:2000],
                    now,
                    now,
                    row["id"],
                    JobStatus.AWAITING_CAPACITY.value,
                ),
            ).fetchone()
            if updated is None:
                continue
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="dead_lettered",
                    from_status=JobStatus.AWAITING_CAPACITY.value,
                    actor=None,
                    detail={"reason": "capacity_deadline"},
                    now=now,
                )
            out.append(self._job(updated))
        return out
