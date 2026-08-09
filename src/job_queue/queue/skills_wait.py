"""Park skilled jobs no worker can run; revive when capability arrives."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from job_queue.models import Job, JobStatus
from job_queue.db import transaction
from job_queue.queue._common import _JOB_COLUMNS, _utcnow


_SKILLS_WAIT_MSG = "waiting for worker skills"


class SkillsWaitMixin:
    def process_awaiting_skills(self, *, limit: int = 200) -> dict[str, int]:
        """Park unfulfillable skilled jobs, revive those now coverable, expire.

        Safe under load (``FOR UPDATE SKIP LOCKED``, bounded *limit*). Lease
        and cleaner loops call this so a missing-skill flood cannot stall the
        claim path, and a newly joined capable worker unblocks the parked set.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        now = _utcnow()
        with transaction(self._pool) as conn:
            dead = self._expire_awaiting_skills_deadlines_locked(
                conn, now=now, limit=limit
            )
            parked = self._park_unfulfillable_skills_locked(
                conn, now=now, limit=limit
            )
            revived = self._revive_awaiting_skills_for_fleet_locked(
                conn, now=now, limit=limit
            )
        return {
            "dead": len(dead),
            "parked": parked,
            "revived": revived,
        }

    def list_awaiting_skills(
        self,
        *,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs parked until a worker with the required skills joins."""
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
                (tenant_id, JobStatus.AWAITING_SKILLS.value, limit, offset),
            ).fetchall()
            return [self._job(r) for r in rows]

    def _sync_skills_wait_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        worker_skills: list[str] | None = None,
        limit: int = 200,
    ) -> None:
        """Revive jobs this fleet/worker can run; park ones nobody can."""
        if worker_skills:
            self._revive_awaiting_skills_locked(
                conn, skills=list(worker_skills), now=now, limit=limit
            )
        else:
            self._revive_awaiting_skills_for_fleet_locked(
                conn, now=now, limit=limit
            )
        self._park_unfulfillable_skills_locked(conn, now=now, limit=limit)

    def _enqueue_status_for_skills(
        self, conn: Any, skills: list[str]
    ) -> str:
        if not skills:
            return JobStatus.PENDING.value
        if self._skills_are_covered(conn, skills):
            return JobStatus.PENDING.value
        return JobStatus.AWAITING_SKILLS.value

    @staticmethod
    def _skills_are_covered(conn: Any, skills: list[str]) -> bool:
        if not skills:
            return True
        row = conn.execute(
            """
            SELECT 1
            FROM worker_nodes wn
            WHERE wn.status = 'active'
              AND %s::text[] <@ COALESCE(wn.skills, '{}'::text[])
            LIMIT 1
            """,
            (list(skills),),
        ).fetchone()
        return row is not None

    def _park_unfulfillable_skills_locked(
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
                  AND COALESCE(j.required_skills, '{{}}'::text[])
                      <> '{{}}'::text[]
                  AND NOT EXISTS (
                        SELECT 1
                        FROM worker_nodes wn
                        WHERE wn.status = 'active'
                          AND COALESCE(j.required_skills, '{{}}'::text[])
                              <@ COALESCE(wn.skills, '{{}}'::text[])
                  )
                ORDER BY j.created_at ASC
                LIMIT %s
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE jobs j
            SET status = %s,
                last_error = %s || ': ' || array_to_string(
                    COALESCE(j.required_skills, '{{}}'::text[]), ','
                ),
                updated_at = %s
            FROM stuck
            WHERE j.id = stuck.id
            RETURNING j.*
            """,
            (
                JobStatus.PENDING.value,
                int(limit),
                JobStatus.AWAITING_SKILLS.value,
                _SKILLS_WAIT_MSG,
                now,
            ),
        ).fetchall()
        for row in rows:
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=row,
                    event_type="awaiting_skills",
                    from_status=JobStatus.PENDING.value,
                    actor=None,
                    detail={"reason": "no_active_worker_with_skills"},
                    now=now,
                )
        return len(rows)

    def _revive_awaiting_skills_locked(
        self,
        conn: Any,
        *,
        skills: list[str],
        now: datetime,
        limit: int = 200,
    ) -> int:
        if not skills:
            return 0
        rows = conn.execute(
            f"""
            WITH due AS (
                SELECT j.id
                FROM jobs j
                WHERE j.status = %s
                  AND COALESCE(j.required_skills, '{{}}'::text[])
                      <> '{{}}'::text[]
                  AND COALESCE(j.required_skills, '{{}}'::text[])
                      <@ %s::text[]
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
                JobStatus.AWAITING_SKILLS.value,
                list(skills),
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
                    event_type="skills_available",
                    from_status=JobStatus.AWAITING_SKILLS.value,
                    actor=None,
                    detail={"revived": True},
                    now=now,
                )
        return len(rows)

    def _revive_awaiting_skills_for_fleet_locked(
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
                  AND EXISTS (
                        SELECT 1
                        FROM worker_nodes wn
                        WHERE wn.status = 'active'
                          AND COALESCE(j.required_skills, '{{}}'::text[])
                              <@ COALESCE(wn.skills, '{{}}'::text[])
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
                JobStatus.AWAITING_SKILLS.value,
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
                    event_type="skills_available",
                    from_status=JobStatus.AWAITING_SKILLS.value,
                    actor=None,
                    detail={"revived": True, "source": "fleet"},
                    now=now,
                )
        return len(rows)

    def _expire_awaiting_skills_deadlines_locked(
        self,
        conn: Any,
        *,
        now: datetime,
        tenant_id: str | None = None,
        limit: int = 200,
    ) -> list[Job]:
        """Dead-letter parked skill waits that passed ``deadline_at``."""
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
                    JobStatus.AWAITING_SKILLS.value,
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
                    JobStatus.AWAITING_SKILLS.value,
                    now,
                    int(limit),
                ),
            ).fetchall()
        out: list[Job] = []
        for row in sel:
            skills = list(row.get("required_skills") or [])
            err = (
                "no worker with required skills before deadline: "
                + ",".join(skills)
            )
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
                    JobStatus.AWAITING_SKILLS.value,
                ),
            ).fetchone()
            if updated is None:
                continue
            if hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=updated,
                    event_type="dead_lettered",
                    from_status=JobStatus.AWAITING_SKILLS.value,
                    actor=None,
                    detail={"reason": "skills_deadline"},
                    now=now,
                )
            out.append(self._job(updated))
        return out
