"""Fold pending jobs that share a coalesce_key into one survivor."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import CoalesceResult, JobStatus
from job_queue.queue._common import (
    CoalesceError,
    _JOB_COLUMNS,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
)
from job_queue.queue.tenants_drain import _normalize_queue_name

_FOLDABLE = {
    JobStatus.PENDING.value,
    JobStatus.AWAITING_SKILLS.value,
    JobStatus.AWAITING_CAPACITY.value,
    JobStatus.FROZEN.value,
    JobStatus.QUARANTINED.value,
}
_SURVIVOR_OK = _FOLDABLE | {JobStatus.LEASED.value}
_BLOCK_STATUSES = {
    JobStatus.AWAITING_APPROVAL.value,
    JobStatus.AWAITING_CHILDREN.value,
}
_SURVIVOR_RANK = {
    JobStatus.LEASED.value: 0,
    JobStatus.PENDING.value: 1,
    JobStatus.AWAITING_SKILLS.value: 2,
    JobStatus.AWAITING_CAPACITY.value: 2,
    JobStatus.FROZEN.value: 3,
    JobStatus.QUARANTINED.value: 4,
}


class JobsCoalesceMixin:
    def coalesce_jobs(
        self,
        tenant_id: str,
        coalesce_key: str,
        *,
        queue_name: str = "default",
        keep_job_id: UUID | None = None,
    ) -> CoalesceResult:
        """Absorb extra live jobs with *coalesce_key* into one survivor.

        ``coalesce_key`` is not unique, so the same logical work can pile
        up as separate pending rows. This folds them in one transaction
        while the fleet keeps running.

        The survivor is the leased job if exactly one is leased, otherwise
        the oldest pending (or *keep_job_id*). Extras are cancelled with
        ``coalesced`` audit and do **not** cascade-cancel dependents.
        Jobs that waited on an extra now wait on the survivor. The
        survivor inherits the union of extras' ``depends_on`` (minus the
        fold set), labels, and skills.

        Fail closed when two jobs are already leased (would double-run
        or require killing a worker), when a match is a workflow step,
        or when a match is parked on approval / child join.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        key = (coalesce_key or "").strip()
        if not key:
            raise ValueError("coalesce_key is required")
        qn = _normalize_queue_name(queue_name)
        now = _utcnow()
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise CoalesceError(
                    f"tenant {tenant_id!r} is offboarding; coalesce is rejected"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise CoalesceError(
                    f"tenant {tenant_id!r} is restoring; coalesce is rejected"
                )
            self._assert_tenant_mutable(
                conn, tenant_id, action="coalesce", error=CoalesceError
            )
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND queue_name = %s
                  AND coalesce_key = %s
                  AND status = ANY(%s)
                ORDER BY id
                FOR UPDATE
                """,
                (tenant_id, qn, key, list(_SURVIVOR_OK | _BLOCK_STATUSES)),
            ).fetchall()
            if not rows:
                return CoalesceResult(
                    tenant_id=tenant_id,
                    queue_name=qn,
                    coalesce_key=key,
                    survivor_job_id=None,
                    jobs_folded=0,
                    folded_job_ids=(),
                )
            blocked = [r for r in rows if r["status"] in _BLOCK_STATUSES]
            if blocked:
                raise CoalesceError(
                    f"coalesce {key!r} on {tenant_id!r}/{qn} cannot fold "
                    f"job {blocked[0]['id']} "
                    f"(status={blocked[0]['status']})",
                )
            live = [r for r in rows if r["status"] in _SURVIVOR_OK]
            if len(live) < 2:
                sid = live[0]["id"] if live else None
                return CoalesceResult(
                    tenant_id=tenant_id,
                    queue_name=qn,
                    coalesce_key=key,
                    survivor_job_id=sid,
                    jobs_folded=0,
                    folded_job_ids=(),
                )
            wf = conn.execute(
                """
                SELECT job_id
                FROM workflow_step_runs
                WHERE job_id = ANY(%s)
                LIMIT 1
                """,
                ([r["id"] for r in live],),
            ).fetchone()
            if wf is not None:
                raise CoalesceError(
                    f"coalesce {key!r} on {tenant_id!r}/{qn} cannot fold "
                    f"workflow step {wf['job_id']}"
                )
            leased = [r for r in live if r["status"] == JobStatus.LEASED.value]
            if len(leased) > 1:
                raise CoalesceError(
                    f"coalesce {key!r} on {tenant_id!r}/{qn} has "
                    f"{len(leased)} leased jobs; refusing to kill a runner "
                    f"or let extras stay claimable",
                )
            survivor_row = self._coalesce_pick_survivor(
                live, keep_job_id=keep_job_id
            )
            survivor_id = survivor_row["id"]
            extras = [r for r in live if r["id"] != survivor_id]
            extra_ids = [r["id"] for r in extras]
            extra_set = set(extra_ids)

            neighbors = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND id <> ALL(%s)
                  AND (
                      depends_on && %s::uuid[]
                      OR parent_job_id = ANY(%s)
                      OR waiting_on && %s::uuid[]
                  )
                ORDER BY id
                FOR UPDATE
                """,
                (tenant_id, extra_ids, extra_ids, extra_ids, extra_ids),
            ).fetchall()

            if survivor_row["status"] != JobStatus.LEASED.value:
                self._coalesce_merge_survivor(
                    conn,
                    survivor=survivor_row,
                    extras=extras,
                    extra_set=extra_set,
                    now=now,
                )

            for neigh in neighbors:
                self._coalesce_remap_neighbor(
                    conn,
                    row=neigh,
                    extra_set=extra_set,
                    survivor_id=survivor_id,
                    now=now,
                )

            folded: list[UUID] = []
            for extra in extras:
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status = ANY(%s)
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        f"coalesced into {survivor_id}",
                        now,
                        extra["id"],
                        tenant_id,
                        list(_FOLDABLE),
                    ),
                ).fetchone()
                if updated is None:
                    raise CoalesceError(
                        f"coalesce {key!r} raced: job {extra['id']} "
                        f"changed status",
                    )
                if hasattr(self, "_append_job_audit"):
                    self._append_job_audit(
                        conn,
                        job_row=updated,
                        event_type="coalesced",
                        from_status=extra["status"],
                        actor=None,
                        detail={
                            "folded_into": str(survivor_id),
                            "coalesce_key": key,
                        },
                        now=now,
                    )
                folded.append(extra["id"])

            survivor_now = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (survivor_id, tenant_id),
            ).fetchone()
            if survivor_now is not None and hasattr(self, "_append_job_audit"):
                self._append_job_audit(
                    conn,
                    job_row=survivor_now,
                    event_type="coalesce_absorbed",
                    from_status=survivor_now["status"],
                    actor=None,
                    detail={
                        "folded_job_ids": [str(j) for j in folded],
                        "coalesce_key": key,
                    },
                    now=now,
                )
            return CoalesceResult(
                tenant_id=tenant_id,
                queue_name=qn,
                coalesce_key=key,
                survivor_job_id=survivor_id,
                jobs_folded=len(folded),
                folded_job_ids=tuple(folded),
            )

    @staticmethod
    def _coalesce_pick_survivor(
        rows: list[dict[str, Any]],
        *,
        keep_job_id: UUID | None,
    ) -> dict[str, Any]:
        if keep_job_id is not None:
            for row in rows:
                if row["id"] == keep_job_id:
                    if row["status"] not in _SURVIVOR_OK:
                        raise CoalesceError(
                            f"keep_job_id {keep_job_id} cannot be the "
                            f"survivor (status={row['status']})"
                        )
                    return row
            raise CoalesceError(
                f"keep_job_id {keep_job_id} is not in the coalesce set"
            )
        return sorted(
            rows,
            key=lambda r: (
                _SURVIVOR_RANK.get(r["status"], 9),
                r["created_at"],
                r["id"].int,
            ),
        )[0]

    def _coalesce_merge_survivor(
        self,
        conn: Any,
        *,
        survivor: dict[str, Any],
        extras: list[dict[str, Any]],
        extra_set: set[UUID],
        now: Any,
    ) -> None:
        deps = self._coalesce_rewrite_ids(
            list(survivor.get("depends_on") or []),
            extra_set,
            survivor["id"],
        )
        for extra in extras:
            deps = self._coalesce_rewrite_ids(
                deps + list(extra.get("depends_on") or []),
                extra_set,
                survivor["id"],
            )
        deps = [d for d in deps if d != survivor["id"]]
        labels = _normalize_labels(
            list(survivor.get("labels") or [])
            + [x for e in extras for x in (e.get("labels") or [])]
        )
        skills = _normalize_skills(
            list(survivor.get("required_skills") or [])
            + [x for e in extras for x in (e.get("required_skills") or [])]
        )
        priority = int(survivor["priority"] or 0)
        available_at = survivor["available_at"]
        deadline_at = survivor.get("deadline_at")
        expires_at = survivor.get("expires_at")
        for extra in extras:
            priority = max(priority, int(extra["priority"] or 0))
            if extra["available_at"] < available_at:
                available_at = extra["available_at"]
            extra_dead = extra.get("deadline_at")
            if extra_dead is not None and (
                deadline_at is None or extra_dead < deadline_at
            ):
                deadline_at = extra_dead
            extra_exp = extra.get("expires_at")
            if extra_exp is not None and (
                expires_at is None or extra_exp < expires_at
            ):
                expires_at = extra_exp
        conn.execute(
            """
            UPDATE jobs
            SET depends_on = %s,
                labels = %s,
                required_skills = %s,
                priority = %s,
                available_at = %s,
                deadline_at = %s,
                expires_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                deps,
                labels,
                skills,
                priority,
                available_at,
                deadline_at,
                expires_at,
                now,
                survivor["id"],
            ),
        )

    def _coalesce_remap_neighbor(
        self,
        conn: Any,
        *,
        row: dict[str, Any],
        extra_set: set[UUID],
        survivor_id: UUID,
        now: Any,
    ) -> None:
        deps = self._coalesce_rewrite_ids(
            list(row.get("depends_on") or []), extra_set, survivor_id
        )
        waiting = self._coalesce_rewrite_ids(
            list(row.get("waiting_on") or []), extra_set, survivor_id
        )
        parent = row.get("parent_job_id")
        if parent in extra_set:
            parent = survivor_id
        if parent == row["id"]:
            parent = None
        conn.execute(
            """
            UPDATE jobs
            SET depends_on = %s,
                waiting_on = %s,
                parent_job_id = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (deps, waiting, parent, now, row["id"]),
        )

    @staticmethod
    def _coalesce_rewrite_ids(
        ids: list[UUID], extra_set: set[UUID], survivor_id: UUID
    ) -> list[UUID]:
        out: list[UUID] = []
        seen: set[UUID] = set()
        for item in ids:
            mapped = survivor_id if item in extra_set else item
            if mapped in seen:
                continue
            seen.add(mapped)
            out.append(mapped)
        return out
