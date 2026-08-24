"""Replay related dead-letter jobs as one graph-closed set."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    ReplayGraphBlocker,
    ReplayGraphPlan,
    ReplayGraphResult,
)
from job_queue.queue._common import (
    QueueDrainError,
    RateLimitError,
    ReplayError,
    _JOB_COLUMNS,
    _as_utc,
    _utcnow,
)

_REPLAYABLE = {
    JobStatus.DEAD.value,
    JobStatus.QUARANTINED.value,
    JobStatus.CANCELLED.value,
    JobStatus.FAILED.value,
}
_STAY = {
    JobStatus.PENDING.value,
    JobStatus.LEASED.value,
    JobStatus.AWAITING_APPROVAL.value,
    JobStatus.AWAITING_CHILDREN.value,
    JobStatus.AWAITING_SKILLS.value,
    JobStatus.AWAITING_CAPACITY.value,
    JobStatus.COMPLETED.value,
    JobStatus.FROZEN.value,
}


class DeadLettersGraphMixin:
    def plan_replay_dead_letter_graph(
        self,
        tenant_id: str,
        *,
        job_ids: Sequence[UUID] | None = None,
        queue_name: str | None = None,
    ) -> ReplayGraphPlan:
        """Dry-run a graph-closed dead-letter replay. No rows change."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        seeds = [i for i in (job_ids or ()) if isinstance(i, UUID)]
        with self._pool.connection() as conn:
            return self._plan_replay_graph_on_conn(
                conn,
                tenant_id=tenant_id,
                seed_ids=seeds,
                queue_name=queue_name,
            )

    def replay_dead_letter_graph(
        self,
        tenant_id: str,
        *,
        job_ids: Sequence[UUID] | None = None,
        queue_name: str | None = None,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> ReplayGraphResult:
        """Replay a related dead-letter graph in one transaction.

        Seeds are the given *job_ids*, or every dead/quarantined job for
        the tenant (optionally *queue_name*). The set expands along
        ``depends_on`` / ``parent_job_id`` / ``waiting_on`` so cancelled
        dependents and spawn joins come back together. Live and completed
        neighbors stay put.

        The fleet is not paused. If the closed set is not safe (missing
        neighbor, frozen parent, workflow step, drain, pending cap,
        dedupe clash) nothing is replayed. Past ``expires_at`` values are
        refreshed so a TTL-killed graph is not cleaned again on the next
        expire pass.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        seeds = [i for i in (job_ids or ()) if isinstance(i, UUID)]
        now = _utcnow()
        if available_at is not None:
            run_at = _as_utc(available_at)
        elif delay_seconds is not None:
            run_at = now + timedelta(seconds=float(delay_seconds))
        else:
            run_at = now
        with transaction(self._pool) as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (tenant_id,),
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ReplayError(
                    f"tenant {tenant_id!r} is offboarding; replay is rejected"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise ReplayError(
                    f"tenant {tenant_id!r} is restoring; replay is rejected"
                )
            plan = self._plan_replay_graph_on_conn(
                conn,
                tenant_id=tenant_id,
                seed_ids=seeds,
                queue_name=queue_name,
            )
            if not plan.ok:
                raise ReplayError(
                    f"replay graph on {tenant_id!r} is not closed "
                    f"({len(plan.blockers)} blocker(s))",
                    plan=plan,
                )
            if not plan.replay_job_ids:
                return ReplayGraphResult(
                    tenant_id=tenant_id,
                    jobs_replayed=0,
                    job_ids=(),
                    rejoin_job_ids=(),
                )
            ids = list(plan.replay_job_ids)
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s AND id = ANY(%s)
                ORDER BY id
                FOR UPDATE
                """,
                (tenant_id, ids),
            ).fetchall()
            by_id = {r["id"]: dict(r) for r in rows}
            if len(by_id) != len(ids):
                raise ReplayError(
                    f"replay graph on {tenant_id!r} raced: a selected job vanished",
                    plan=plan,
                )
            still: list[dict[str, Any]] = []
            for jid in ids:
                row = by_id[jid]
                if row["status"] not in _REPLAYABLE:
                    continue
                still.append(row)
            if not still:
                return ReplayGraphResult(
                    tenant_id=tenant_id,
                    jobs_replayed=0,
                    job_ids=(),
                    rejoin_job_ids=(),
                )
            rejoin = {
                r["id"] for r in still if self._replay_should_restore_join(r)
            }
            pending_extra = sum(1 for r in still if r["id"] not in rejoin)
            queues = {str(r["queue_name"]) for r in still}
            if hasattr(self, "_assert_queue_accepts_enqueue"):
                for qname in sorted(queues):
                    try:
                        self._assert_queue_accepts_enqueue(conn, tenant_id, qname)
                    except QueueDrainError as exc:
                        raise ReplayError(str(exc), plan=plan) from exc
            if pending_extra and hasattr(self, "_assert_pending_capacity"):
                try:
                    # Charge the extras as a batch: check remaining room.
                    limit = self._read_pending_limit(conn, tenant_id)
                    if limit is not None:
                        have = self._count_pending(conn, tenant_id)
                        if have + pending_extra > int(limit):
                            raise ReplayError(
                                f"tenant {tenant_id!r} pending job cap would be "
                                f"exceeded ({have}+{pending_extra} > {limit})",
                                plan=plan,
                            )
                except ValueError as exc:
                    raise ReplayError(str(exc), plan=plan) from exc
            dead_extra = sum(
                1
                for r in still
                if r["status"] == JobStatus.DEAD.value and r["id"] not in rejoin
            )
            if dead_extra:
                try:
                    if hasattr(self, "_consume_enqueue_rate"):
                        for _ in range(dead_extra):
                            self._consume_enqueue_rate(conn, tenant_id, now=now)
                    if hasattr(self, "_consume_spend_enqueues"):
                        self._consume_spend_enqueues(
                            conn, tenant_id, extra=dead_extra, now=now
                        )
                except (ValueError, RateLimitError) as exc:
                    raise ReplayError(str(exc), plan=plan) from exc
            for row in still:
                if row.get("dedupe_key"):
                    self._lock_dedupe_key(
                        conn,
                        tenant_id=tenant_id,
                        queue_name=str(row["queue_name"]),
                        dedupe_key=str(row["dedupe_key"]),
                    )
                    conflict = self._find_active_dedupe(
                        conn,
                        tenant_id=tenant_id,
                        queue_name=str(row["queue_name"]),
                        dedupe_key=str(row["dedupe_key"]),
                    )
                    if conflict is not None and conflict["id"] not in by_id:
                        raise ReplayError(
                            f"job {row['id']} cannot be replayed: dedupe_key "
                            f"{row['dedupe_key']!r} is already active on job "
                            f"{conflict['id']}",
                            plan=plan,
                        )
            replayed: list[UUID] = []
            rejoined: list[UUID] = []
            try:
                for row in still:
                    updated = self._replay_locked_row(
                        conn,
                        row=row,
                        tenant_id=tenant_id,
                        now=now,
                        run_at=run_at,
                        reset_attempts=reset_attempts,
                        max_attempts=max_attempts,
                        clear_error=clear_error,
                        restore_join=row["id"] in rejoin,
                    )
                    replayed.append(updated["id"])
                    if row["id"] in rejoin:
                        rejoined.append(updated["id"])
            except UniqueViolation as exc:
                raise ReplayError(
                    f"replay graph on {tenant_id!r} hit a unique conflict",
                    plan=plan,
                ) from exc
            return ReplayGraphResult(
                tenant_id=tenant_id,
                jobs_replayed=len(replayed),
                job_ids=tuple(replayed),
                rejoin_job_ids=tuple(rejoined),
            )

    def _plan_replay_graph_on_conn(
        self,
        conn: Any,
        *,
        tenant_id: str,
        seed_ids: list[UUID],
        queue_name: str | None,
    ) -> ReplayGraphPlan:
        blockers: list[ReplayGraphBlocker] = []
        seen_block: set[tuple[UUID, str]] = set()

        def add_blocker(
            job_id: UUID, reason: str, related: UUID | None = None
        ) -> None:
            key = (job_id, reason)
            if key in seen_block:
                return
            seen_block.add(key)
            blockers.append(
                ReplayGraphBlocker(
                    job_id=job_id, reason=reason, related_job_id=related
                )
            )

        if seed_ids:
            seeds = self._replay_load_ids(conn, tenant_id=tenant_id, ids=seed_ids)
            for sid in seed_ids:
                row = seeds.get(sid)
                if row is None:
                    add_blocker(sid, "missing_seed")
                    continue
                if row["status"] in _REPLAYABLE:
                    continue
                if row["status"] in _STAY:
                    continue
                add_blocker(sid, f"unreplayable_status:{row['status']}")
        else:
            clauses = ["tenant_id = %s", "status IN (%s, %s)"]
            params: list[Any] = [
                tenant_id,
                JobStatus.DEAD.value,
                JobStatus.QUARANTINED.value,
            ]
            if queue_name is not None:
                clauses.append("queue_name = %s")
                params.append(queue_name)
            rows = conn.execute(
                f"""
                SELECT id, status, queue_name, labels, required_skills,
                       depends_on, parent_job_id, waiting_on, checkpoint,
                       await_children_outcome
                FROM jobs
                WHERE {" AND ".join(clauses)}
                """,
                tuple(params),
            ).fetchall()
            seeds = {r["id"]: dict(r) for r in rows}

        known: dict[UUID, dict[str, Any]] = dict(seeds)
        replay: set[UUID] = {
            jid for jid, row in seeds.items() if row["status"] in _REPLAYABLE
        }
        stay: set[UUID] = set()
        frontier = set(replay)
        while frontier:
            batch = list(frontier)
            frontier = set()
            extra_ids: set[UUID] = set()
            for jid in batch:
                row = known.get(jid)
                if row is None:
                    extra_ids.add(jid)
                    continue
                extra_ids |= self._replay_forward_ids(row)
            reverse = self._replay_reverse_neighbors(
                conn, tenant_id=tenant_id, ids=batch
            )
            known.update(reverse)
            extra_ids |= set(reverse)
            missing = [i for i in extra_ids if i not in known]
            known.update(
                self._replay_load_ids(conn, tenant_id=tenant_id, ids=missing)
            )
            for nid in extra_ids:
                if nid in replay or nid in stay:
                    continue
                nrow = known.get(nid)
                if nrow is None:
                    add_blocker(nid, "missing_neighbor")
                    continue
                status = nrow["status"]
                if status in _REPLAYABLE:
                    replay.add(nid)
                    frontier.add(nid)
                    continue
                if status in _STAY:
                    stay.add(nid)
                    if status == JobStatus.FROZEN.value:
                        add_blocker(nid, "frozen_neighbor", nid)
                    continue
                add_blocker(nid, f"unreplayable_status:{status}", nid)

        if replay:
            wf = conn.execute(
                """
                SELECT job_id
                FROM workflow_step_runs
                WHERE job_id = ANY(%s)
                LIMIT 5
                """,
                (list(replay),),
            ).fetchall()
            for row in wf:
                add_blocker(row["job_id"], "workflow_step")
                replay.discard(row["job_id"])

        rejoin = tuple(
            sorted(
                (
                    jid
                    for jid in replay
                    if self._replay_should_restore_join(known[jid])
                ),
                key=lambda u: u.int,
            )
        )
        blockers_t = tuple(blockers)
        return ReplayGraphPlan(
            tenant_id=tenant_id,
            ok=len(blockers_t) == 0,
            replay_job_ids=tuple(sorted(replay, key=lambda u: u.int)),
            stay_job_ids=tuple(sorted(stay, key=lambda u: u.int)),
            rejoin_job_ids=rejoin,
            blockers=blockers_t,
        )

    def _replay_load_ids(
        self, conn: Any, *, tenant_id: str, ids: list[UUID]
    ) -> dict[UUID, dict[str, Any]]:
        if not ids:
            return {}
        rows = conn.execute(
            """
            SELECT id, status, queue_name, labels, required_skills,
                   depends_on, parent_job_id, waiting_on, checkpoint,
                   await_children_outcome
            FROM jobs
            WHERE tenant_id = %s AND id = ANY(%s)
            """,
            (tenant_id, ids),
        ).fetchall()
        return {r["id"]: dict(r) for r in rows}

    def _replay_reverse_neighbors(
        self, conn: Any, *, tenant_id: str, ids: list[UUID]
    ) -> dict[UUID, dict[str, Any]]:
        if not ids:
            return {}
        rows = conn.execute(
            """
            SELECT id, status, queue_name, labels, required_skills,
                   depends_on, parent_job_id, waiting_on, checkpoint,
                   await_children_outcome
            FROM jobs
            WHERE tenant_id = %s
              AND (
                  parent_job_id = ANY(%s)
                  OR depends_on && %s::uuid[]
                  OR waiting_on && %s::uuid[]
              )
            """,
            (tenant_id, ids, ids, ids),
        ).fetchall()
        return {r["id"]: dict(r) for r in rows}

    @staticmethod
    def _replay_forward_ids(row: dict[str, Any]) -> set[UUID]:
        out: set[UUID] = set()
        for dep in row.get("depends_on") or []:
            out.add(dep)
        parent = row.get("parent_job_id")
        if parent is not None:
            out.add(parent)
        for kid in row.get("waiting_on") or []:
            out.add(kid)
        return out

    @staticmethod
    def _replay_should_restore_join(row: dict[str, Any]) -> bool:
        waiting = list(row.get("waiting_on") or [])
        if not waiting:
            return False
        if row.get("await_children_outcome") == "joined":
            return False
        cp = row.get("checkpoint")
        if isinstance(cp, dict) and cp.get("phase") == "children_joined":
            return False
        return True

    def _replay_locked_row(
        self,
        conn: Any,
        *,
        row: dict[str, Any],
        tenant_id: str,
        now: datetime,
        run_at: datetime,
        reset_attempts: bool,
        max_attempts: int | None,
        clear_error: bool,
        restore_join: bool,
    ) -> dict[str, Any]:
        job = self._job(row, conn=conn)
        new_attempts = 0 if reset_attempts else int(row["attempts"] or 0)
        new_max = (
            max_attempts if max_attempts is not None else int(row["max_attempts"])
        )
        new_error = None if clear_error else row.get("last_error")
        new_expires = self._replay_refresh_expires_at(
            conn,
            row=row,
            tenant_id=tenant_id,
            now=now,
            run_at=run_at,
        )
        if restore_join:
            new_status = JobStatus.AWAITING_CHILDREN.value
        elif hasattr(self, "_enqueue_status_for_skills"):
            new_status = self._enqueue_status_for_skills(
                conn, list(job.required_skills or ())
            )
        else:
            new_status = JobStatus.PENDING.value
        updated = conn.execute(
            f"""
            UPDATE jobs
            SET status = %s,
                attempts = %s,
                max_attempts = %s,
                available_at = %s,
                expires_at = %s,
                leased_by = NULL,
                lease_token = NULL,
                lease_tenant_id = NULL,
                lease_expires_at = NULL,
                lease_started_at = NULL,
                completed_at = NULL,
                last_error = %s,
                failed_workers = ARRAY[]::text[],
                await_children_outcome = CASE
                    WHEN %s THEN NULL
                    ELSE await_children_outcome
                END,
                await_children_resolved_at = CASE
                    WHEN %s THEN NULL
                    ELSE await_children_resolved_at
                END,
                updated_at = %s
            WHERE id = %s
              AND tenant_id = %s
              AND status = %s
            RETURNING {_JOB_COLUMNS}
            """,
            (
                new_status,
                new_attempts,
                new_max,
                run_at,
                new_expires,
                new_error,
                restore_join,
                restore_join,
                now,
                job.id,
                tenant_id,
                row["status"],
            ),
        ).fetchone()
        if updated is None:
            raise ReplayError(
                f"job {job.id} could not be replayed (status changed)",
                job=job,
            )
        if hasattr(self, "_append_job_audit"):
            self._append_job_audit(
                conn,
                job_row=updated,
                event_type="replayed",
                from_status=row["status"],
                actor=None,
                detail={
                    "reset_attempts": reset_attempts,
                    "graph": True,
                    "restore_join": restore_join,
                    "expires_at": (
                        new_expires.isoformat() if new_expires is not None else None
                    ),
                },
                now=now,
            )
        return updated
