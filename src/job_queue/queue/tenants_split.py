"""Split a live tenant: plan a graph-safe subset move to another tenant."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.crypto import result_aad
from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    TenantSplitBlocker,
    TenantSplitPlan,
    TenantSplitResult,
)
from job_queue.queue._common import (
    ScheduleError,
    TenantSplitError,
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    _normalize_labels,
    _normalize_skills,
    _utcnow,
)
from job_queue.queue.tenants_drain import _normalize_queue_name

_MOVABLE = {
    JobStatus.PENDING.value,
    JobStatus.AWAITING_APPROVAL.value,
    JobStatus.AWAITING_CHILDREN.value,
    JobStatus.AWAITING_SKILLS.value,
    JobStatus.AWAITING_CAPACITY.value,
    JobStatus.QUARANTINED.value,
    JobStatus.FROZEN.value,
}
_TERMINAL = {
    JobStatus.COMPLETED.value,
    JobStatus.FAILED.value,
    JobStatus.DEAD.value,
    JobStatus.CANCELLED.value,
}


class TenantsSplitMixin:
    def plan_tenant_split(
        self,
        source_tenant_id: str,
        target_tenant_id: str,
        *,
        queue_name: str | None = None,
        labels: Sequence[str] | str | None = None,
        required_skills: Sequence[str] | str | None = None,
    ) -> TenantSplitPlan:
        """Dry-run a subset move from *source* to *target*. No rows change.

        Select by *queue_name*, *labels* (AND), and/or *required_skills*
        (AND). At least one filter is required. Leased matches stay on the
        source. A job is only listed to move when every live or historical
        graph neighbor it still needs is also in the move set (or is a
        same-filter terminal we can take with it). Anything that would
        half-split a DAG, leave a child waiting, or pull a leased parent
        is a blocker and ``ok`` is False.
        """
        source = (source_tenant_id or "").strip()
        dest = (target_tenant_id or "").strip()
        if not source or not dest:
            raise ValueError("source_tenant_id and target_tenant_id are required")
        if source == dest:
            raise ValueError("source_tenant_id and target_tenant_id must differ")
        qn = _normalize_queue_name(queue_name) if queue_name is not None else None
        label_list = _normalize_labels(labels)
        skill_list = _normalize_skills(required_skills)
        if qn is None and not label_list and not skill_list:
            raise ValueError("pass at least one of queue_name, labels, required_skills")

        with self._pool.connection() as conn:
            return self._plan_tenant_split_on_conn(
                conn,
                source=source,
                dest=dest,
                queue_name=qn,
                label_list=label_list,
                skill_list=skill_list,
            )

    def split_tenant(
        self,
        source_tenant_id: str,
        target_tenant_id: str,
        *,
        queue_name: str | None = None,
        labels: Sequence[str] | str | None = None,
        required_skills: Sequence[str] | str | None = None,
    ) -> TenantSplitResult:
        """Move a graph-closed subset from *source* to *target* in one txn.

        The fleet is not paused. Jobs are locked, then the same plan as
        :meth:`plan_tenant_split` is recomputed. Matching schedules move
        as a closed unit (live workflow tick, steps, signals, generations)
        even when a tick job is leased. A leftover standalone job that
        becomes leased mid-flight still aborts the split.
        """
        source = (source_tenant_id or "").strip()
        dest = (target_tenant_id or "").strip()
        if not source or not dest:
            raise ValueError("source_tenant_id and target_tenant_id are required")
        if source == dest:
            raise ValueError("source_tenant_id and target_tenant_id must differ")
        qn = _normalize_queue_name(queue_name) if queue_name is not None else None
        label_list = _normalize_labels(labels)
        skill_list = _normalize_skills(required_skills)
        if qn is None and not label_list and not skill_list:
            raise ValueError("pass at least one of queue_name, labels, required_skills")
        now = _utcnow()
        with transaction(self._pool) as conn:
            for tid in sorted((source, dest)):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (tid,),
                )
            for tid, label in ((source, "source"), (dest, "destination")):
                if self._is_tenant_offboarding(conn, tid):
                    raise TenantSplitError(
                        f"{label} tenant {tid!r} is offboarding; split is rejected"
                    )
                if self._is_tenant_restoring(conn, tid):
                    raise TenantSplitError(
                        f"{label} tenant {tid!r} is restoring; split is rejected"
                    )
            plan = self._plan_tenant_split_on_conn(
                conn,
                source=source,
                dest=dest,
                queue_name=qn,
                label_list=label_list,
                skill_list=skill_list,
            )
            if not plan.ok:
                raise TenantSplitError(
                    f"split {source!r} -> {dest!r} is not graph-safe "
                    f"({len(plan.blockers)} blocker(s))",
                    plan=plan,
                )
            if not plan.move_job_ids and not plan.move_schedule_ids:
                return TenantSplitResult(
                    source_tenant_id=source,
                    target_tenant_id=dest,
                    jobs_moved=0,
                    job_ids=(),
                )
            ids = list(plan.move_job_ids)
            sched_ids = list(plan.move_schedule_ids)
            sched_job_ids = set()
            if sched_ids and hasattr(self, "_schedule_owned_job_ids"):
                for sid in sched_ids:
                    sched_job_ids.update(
                        self._schedule_owned_job_ids(
                            conn, tenant_id=source, schedule_id=sid
                        )
                    )
                if hasattr(self, "_workflow_run_ids_for_schedule"):
                    for sid in sched_ids:
                        rids = self._workflow_run_ids_for_schedule(
                            conn, tenant_id=source, schedule_id=sid
                        )
                        if rids and hasattr(self, "_workflow_step_job_ids"):
                            sched_job_ids.update(
                                self._workflow_step_job_ids(conn, run_ids=rids)
                            )
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s AND id = ANY(%s)
                ORDER BY id
                FOR UPDATE
                """,
                (source, ids),
            ).fetchall()
            if len(rows) != len(ids):
                raise TenantSplitError(
                    f"split {source!r} -> {dest!r} raced: a selected job "
                    f"changed owner or vanished",
                    plan=plan,
                )
            pending_extra = 0
            queues: set[str] = set()
            for row in rows:
                if (
                    row["status"] == JobStatus.LEASED.value
                    and row["id"] not in sched_job_ids
                ):
                    raise TenantSplitError(
                        f"split {source!r} -> {dest!r} raced: job "
                        f"{row['id']} is leased",
                        plan=plan,
                    )
                queues.add(str(row["queue_name"]))
                if row["status"] in (
                    JobStatus.PENDING.value,
                    JobStatus.AWAITING_SKILLS.value,
                    JobStatus.AWAITING_CAPACITY.value,
                ):
                    pending_extra += 1
            if hasattr(self, "_assert_queue_accepts_enqueue"):
                for qname in sorted(queues):
                    self._assert_queue_accepts_enqueue(conn, dest, qname)
            limit = self._read_pending_limit(conn, dest)
            if limit is not None and pending_extra:
                have = self._count_pending(conn, dest)
                if have + pending_extra > int(limit):
                    raise TenantSplitError(
                        f"destination tenant {dest!r} pending job cap would be "
                        f"exceeded ({have}+{pending_extra} > {limit})",
                        plan=plan,
                    )
            for row in rows:
                job = self._job(row, conn=conn)
                if job.dedupe_key is not None:
                    self._lock_dedupe_key(
                        conn,
                        tenant_id=dest,
                        queue_name=job.queue_name,
                        dedupe_key=job.dedupe_key,
                    )
                    conflict = self._find_active_dedupe(
                        conn,
                        tenant_id=dest,
                        queue_name=job.queue_name,
                        dedupe_key=job.dedupe_key,
                    )
                    if conflict is not None and conflict["id"] != job.id:
                        raise TenantSplitError(
                            f"job {job.id} cannot split to {dest!r}: "
                            f"dedupe_key {job.dedupe_key!r} is already active "
                            f"on job {conflict['id']}",
                            plan=plan,
                        )
                sealed, payload_key_id = self._seal_job_payload(
                    conn,
                    tenant_id=dest,
                    job_id=job.id,
                    payload=job.payload,
                )
                # Terminals that ride along (completed parent, etc.) keep a
                # result sealed under the source DEK. Dest get/list decrypts
                # with the new owner, so reseal or dest hits PayloadCryptoError.
                sealed_result = None
                if job.result is not None:
                    sealed_result, _ = self._seal_json_value(
                        conn,
                        tenant_id=dest,
                        value=job.result,
                        aad=result_aad(job.id),
                    )
                part_id = self._compute_partition_id(
                    conn, tenant_id=dest, job_id=job.id
                )
                keep_sched = job.id in sched_job_ids
                lease_tid = row.get("lease_tenant_id")
                if (
                    keep_sched
                    and job.status == JobStatus.LEASED
                    and lease_tid is None
                ):
                    lease_tid = source
                try:
                    with conn.transaction():
                        if keep_sched:
                            updated = conn.execute(
                                f"""
                                UPDATE jobs
                                SET tenant_id = %s,
                                    payload = %s,
                                    result = %s,
                                    payload_key_id = %s,
                                    partition_id = %s,
                                    lease_tenant_id = %s,
                                    updated_at = %s
                                WHERE id = %s
                                  AND tenant_id = %s
                                RETURNING {_JOB_COLUMNS}
                                """,
                                (
                                    dest,
                                    Jsonb(sealed),
                                    None
                                    if sealed_result is None
                                    else Jsonb(sealed_result),
                                    payload_key_id,
                                    part_id,
                                    lease_tid,
                                    now,
                                    job.id,
                                    source,
                                ),
                            ).fetchone()
                        else:
                            updated = conn.execute(
                                f"""
                                UPDATE jobs
                                SET tenant_id = %s,
                                    payload = %s,
                                    result = %s,
                                    payload_key_id = %s,
                                    partition_id = %s,
                                    schedule_id = NULL,
                                    lease_tenant_id = NULL,
                                    updated_at = %s
                                WHERE id = %s
                                  AND tenant_id = %s
                                  AND status <> %s
                                RETURNING {_JOB_COLUMNS}
                                """,
                                (
                                    dest,
                                    Jsonb(sealed),
                                    None
                                    if sealed_result is None
                                    else Jsonb(sealed_result),
                                    payload_key_id,
                                    part_id,
                                    now,
                                    job.id,
                                    source,
                                    JobStatus.LEASED.value,
                                ),
                            ).fetchone()
                except UniqueViolation as exc:
                    raise TenantSplitError(
                        f"split {source!r} -> {dest!r} unique conflict "
                        f"on job {job.id}",
                        plan=plan,
                    ) from exc
                if updated is None:
                    raise TenantSplitError(
                        f"split {source!r} -> {dest!r} raced: job {job.id} "
                        f"could not be updated",
                        plan=plan,
                    )
                conn.execute(
                    """
                    UPDATE job_side_effects
                    SET tenant_id = %s
                    WHERE job_id = %s
                    """,
                    (dest, job.id),
                )
                if hasattr(self, "_append_job_audit"):
                    self._append_job_audit(
                        conn,
                        job_row=updated,
                        event_type="tenant_split",
                        from_status=updated["status"],
                        actor=None,
                        detail={
                            "from_tenant": source,
                            "to_tenant": dest,
                        },
                        now=now,
                    )
            if sched_ids:
                self._split_move_schedules(
                    conn,
                    source=source,
                    dest=dest,
                    schedule_ids=sched_ids,
                    now=now,
                )
            return TenantSplitResult(
                source_tenant_id=source,
                target_tenant_id=dest,
                jobs_moved=len(ids),
                job_ids=tuple(ids),
                schedules_moved=len(sched_ids),
                schedule_ids=tuple(sched_ids),
            )

    def _plan_tenant_split_on_conn(
        self,
        conn: Any,
        *,
        source: str,
        dest: str,
        queue_name: str | None,
        label_list: list[str],
        skill_list: list[str],
    ) -> TenantSplitPlan:
        sched_ids, sched_jobs, sched_blockers = self._split_close_schedules(
            conn,
            source=source,
            dest=dest,
            queue_name=queue_name,
            labels=set(label_list),
            skills=set(skill_list),
        )
        matched = self._split_load_matching(
            conn,
            tenant_id=source,
            queue_name=queue_name,
            labels=label_list,
            skills=skill_list,
        )
        move_ids, leased_ids, blockers = self._split_close_graph(
            conn,
            tenant_id=source,
            matched=matched,
            queue_name=queue_name,
            labels=set(label_list),
            skills=set(skill_list),
            pre_move=set(sched_jobs),
            allow_leased=set(sched_jobs),
        )
        blockers.extend(sched_blockers)
        if move_ids:
            wf = conn.execute(
                """
                SELECT job_id
                FROM workflow_step_runs
                WHERE job_id = ANY(%s)
                LIMIT 5
                """,
                (list(move_ids),),
            ).fetchall()
            for row in wf:
                jid = row["job_id"]
                if jid in sched_jobs:
                    continue
                blockers.append(
                    TenantSplitBlocker(job_id=jid, reason="workflow_step")
                )
                move_ids.discard(jid)
        blockers_t = tuple(blockers)
        return TenantSplitPlan(
            source_tenant_id=source,
            target_tenant_id=dest,
            queue_name=queue_name,
            labels=tuple(label_list),
            required_skills=tuple(skill_list),
            ok=len(blockers_t) == 0,
            move_job_ids=tuple(sorted(move_ids, key=lambda u: u.int)),
            stay_leased_ids=tuple(sorted(leased_ids, key=lambda u: u.int)),
            blockers=blockers_t,
            move_schedule_ids=tuple(sorted(sched_ids, key=lambda u: u.int)),
        )

    def _split_match_row(
        self,
        row: dict[str, Any],
        *,
        queue_name: str | None,
        labels: set[str],
        skills: set[str],
    ) -> bool:
        if queue_name is not None and row.get("queue_name") != queue_name:
            return False
        if labels and not labels.issubset(set(row.get("labels") or [])):
            return False
        if skills and not skills.issubset(set(row.get("required_skills") or [])):
            return False
        return True

    def _split_load_matching(
        self,
        conn: Any,
        *,
        tenant_id: str,
        queue_name: str | None,
        labels: list[str],
        skills: list[str],
    ) -> dict[UUID, dict[str, Any]]:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if labels:
            clauses.append("labels @> %s::text[]")
            params.append(labels)
        if skills:
            clauses.append("required_skills @> %s::text[]")
            params.append(skills)
        rows = conn.execute(
            f"""
            SELECT id, status, queue_name, labels, required_skills,
                   depends_on, parent_job_id, waiting_on
            FROM jobs
            WHERE {" AND ".join(clauses)}
            """,
            tuple(params),
        ).fetchall()
        return {r["id"]: dict(r) for r in rows}

    def _split_load_ids(
        self, conn: Any, *, tenant_id: str, ids: list[UUID]
    ) -> dict[UUID, dict[str, Any]]:
        if not ids:
            return {}
        rows = conn.execute(
            """
            SELECT id, status, queue_name, labels, required_skills,
                   depends_on, parent_job_id, waiting_on
            FROM jobs
            WHERE tenant_id = %s AND id = ANY(%s)
            """,
            (tenant_id, ids),
        ).fetchall()
        return {r["id"]: dict(r) for r in rows}

    def _split_reverse_neighbors(
        self, conn: Any, *, tenant_id: str, ids: list[UUID]
    ) -> dict[UUID, dict[str, Any]]:
        if not ids:
            return {}
        rows = conn.execute(
            """
            SELECT id, status, queue_name, labels, required_skills,
                   depends_on, parent_job_id, waiting_on
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
    def _split_forward_ids(row: dict[str, Any]) -> set[UUID]:
        out: set[UUID] = set()
        for dep in row.get("depends_on") or []:
            out.add(dep)
        parent = row.get("parent_job_id")
        if parent is not None:
            out.add(parent)
        for kid in row.get("waiting_on") or []:
            out.add(kid)
        return out

    def _split_close_graph(
        self,
        conn: Any,
        *,
        tenant_id: str,
        matched: dict[UUID, dict[str, Any]],
        queue_name: str | None,
        labels: set[str],
        skills: set[str],
        pre_move: set[UUID] | None = None,
        allow_leased: set[UUID] | None = None,
    ) -> tuple[set[UUID], tuple[UUID, ...], list[TenantSplitBlocker]]:
        allow_leased = set(allow_leased or ())
        pre_move = set(pre_move or ())
        leased = tuple(
            sorted(
                (
                    jid
                    for jid, row in matched.items()
                    if row["status"] == JobStatus.LEASED.value
                    and jid not in allow_leased
                ),
                key=lambda u: u.int,
            )
        )
        move: set[UUID] = {
            jid for jid, row in matched.items() if row["status"] in _MOVABLE
        }
        move |= pre_move
        known: dict[UUID, dict[str, Any]] = dict(matched)
        blockers: list[TenantSplitBlocker] = []
        seen_block: set[tuple[UUID, str]] = set()

        def add_blocker(job_id: UUID, reason: str, related: UUID | None = None) -> None:
            key = (job_id, reason)
            if key in seen_block:
                return
            seen_block.add(key)
            blockers.append(
                TenantSplitBlocker(
                    job_id=job_id, reason=reason, related_job_id=related
                )
            )

        frontier = set(move)
        while frontier:
            batch = list(frontier)
            frontier = set()
            extra_ids: set[UUID] = set()
            for jid in batch:
                row = known.get(jid)
                if row is None:
                    extra_ids.add(jid)
                    continue
                extra_ids |= self._split_forward_ids(row)
            reverse = self._split_reverse_neighbors(
                conn, tenant_id=tenant_id, ids=batch
            )
            known.update(reverse)
            extra_ids |= set(reverse)
            missing = [i for i in extra_ids if i not in known]
            loaded = self._split_load_ids(conn, tenant_id=tenant_id, ids=missing)
            known.update(loaded)
            for nid in extra_ids:
                if nid in move:
                    continue
                nrow = known.get(nid)
                if nrow is None:
                    add_blocker(nid, "missing_neighbor")
                    continue
                matches = self._split_match_row(
                    nrow, queue_name=queue_name, labels=labels, skills=skills
                )
                status = nrow["status"]
                if status == JobStatus.LEASED.value:
                    if nid in allow_leased:
                        move.add(nid)
                        frontier.add(nid)
                        continue
                    add_blocker(nid, "leased_graph_neighbor", nid)
                    continue
                if matches and (status in _MOVABLE or status in _TERMINAL):
                    move.add(nid)
                    frontier.add(nid)
                    continue
                if not matches:
                    add_blocker(nid, "graph_outside_filter", nid)
                    continue
                add_blocker(nid, f"unmovable_status:{status}", nid)

        # Drop any selected job still linked to something outside the set.
        changed = True
        while changed:
            changed = False
            for jid in list(move):
                row = known.get(jid)
                if row is None:
                    move.discard(jid)
                    changed = True
                    continue
                needed = self._split_forward_ids(row)
                for other in known.values():
                    if other["id"] == jid:
                        continue
                    if other["id"] in move:
                        continue
                    deps = set(other.get("depends_on") or [])
                    waiting = set(other.get("waiting_on") or [])
                    if (
                        other.get("parent_job_id") == jid
                        or jid in deps
                        or jid in waiting
                    ):
                        needed.add(other["id"])
                if any(n not in move for n in needed):
                    move.discard(jid)
                    changed = True

        # Isolated leased matches are stays, not blockers, unless already flagged.
        return move, leased, blockers

    def _split_match_schedule(
        self,
        row: dict[str, Any],
        *,
        queue_name: str | None,
        labels: set[str],
        skills: set[str],
    ) -> bool:
        if queue_name is not None and row.get("queue_name") != queue_name:
            return False
        if labels and not labels.issubset(set(row.get("labels") or [])):
            return False
        if skills and not skills.issubset(set(row.get("required_skills") or [])):
            return False
        return True

    def _split_close_schedules(
        self,
        conn: Any,
        *,
        source: str,
        dest: str,
        queue_name: str | None,
        labels: set[str],
        skills: set[str],
    ) -> tuple[list[UUID], set[UUID], list[TenantSplitBlocker]]:
        rows = conn.execute(
            f"""
            SELECT {_SCHEDULE_COLUMNS}
            FROM schedules
            WHERE tenant_id = %s
            ORDER BY id
            """,
            (source,),
        ).fetchall()
        by_id = {r["id"]: dict(r) for r in rows}
        matching = {
            sid
            for sid, row in by_id.items()
            if self._split_match_schedule(
                row, queue_name=queue_name, labels=labels, skills=skills
            )
        }
        blockers: list[TenantSplitBlocker] = []
        if not matching:
            return [], set(), blockers

        needed = set(matching)
        for sid in list(matching):
            row = by_id[sid]
            for raw in list(row.get("waits_on") or []) + list(
                row.get("joins_on") or []
            ):
                nid = raw if isinstance(raw, UUID) else UUID(str(raw))
                if nid in by_id:
                    needed.add(nid)
        extra = needed - matching
        for sid in extra:
            blockers.append(
                TenantSplitBlocker(
                    job_id=sid,
                    reason="schedule_graph_outside_filter",
                    schedule_id=sid,
                )
            )
        leftover = set(by_id) - needed
        for sid in leftover:
            row = by_id[sid]
            deps = set(row.get("waits_on") or []) | set(row.get("joins_on") or [])
            hit = deps & needed
            if hit:
                related = next(iter(hit))
                blockers.append(
                    TenantSplitBlocker(
                        job_id=sid,
                        reason="leftover_schedule_depends_on_moved",
                        related_job_id=related
                        if isinstance(related, UUID)
                        else UUID(str(related)),
                        schedule_id=sid,
                    )
                )

        dest_names = {
            r["name"]
            for r in conn.execute(
                """
                SELECT name FROM schedules
                WHERE tenant_id = %s AND name IS NOT NULL
                """,
                (dest,),
            ).fetchall()
            if r.get("name")
        }
        for sid in matching:
            name = by_id[sid].get("name")
            if name and name in dest_names:
                blockers.append(
                    TenantSplitBlocker(
                        job_id=sid,
                        reason="schedule_name_conflict",
                        schedule_id=sid,
                    )
                )
            group = by_id[sid].get("mutex_group")
            if group and hasattr(self, "_schedule_mutex_blocked"):
                if self._schedule_mutex_blocked(
                    conn, tenant_id=dest, mutex_group=str(group)
                ):
                    blockers.append(
                        TenantSplitBlocker(
                            job_id=sid,
                            reason="dest_mutex_inflight",
                            schedule_id=sid,
                        )
                    )

        sched_jobs: set[UUID] = set()
        if not extra and hasattr(self, "_schedule_owned_job_ids"):
            for sid in needed:
                try:
                    owned = self._schedule_owned_job_ids(
                        conn, tenant_id=source, schedule_id=sid
                    )
                except ScheduleError as exc:
                    blockers.append(
                        TenantSplitBlocker(
                            job_id=sid,
                            reason="schedule_tick_not_closed",
                            schedule_id=sid,
                        )
                    )
                    _ = exc
                    continue
                sched_jobs.update(owned)
                if hasattr(self, "_workflow_run_ids_for_schedule"):
                    rids = self._workflow_run_ids_for_schedule(
                        conn, tenant_id=source, schedule_id=sid
                    )
                    if rids and hasattr(self, "_workflow_step_job_ids"):
                        sched_jobs.update(
                            self._workflow_step_job_ids(conn, run_ids=rids)
                        )

        if blockers:
            return [], set(), blockers
        return sorted(needed, key=lambda u: u.int), sched_jobs, blockers

    def _split_move_schedules(
        self,
        conn: Any,
        *,
        source: str,
        dest: str,
        schedule_ids: list[UUID],
        now: Any,
    ) -> None:
        for sid in schedule_ids:
            row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (sid, source),
            ).fetchone()
            if row is None:
                raise TenantSplitError(
                    f"schedule {sid} vanished while splitting "
                    f"{source!r} -> {dest!r}"
                )
            if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
                conn, dest, str(row["queue_name"])
            ):
                raise TenantSplitError(
                    f"queue {row['queue_name']!r} is draining on {dest!r}"
                )
            sealed, payload_key_id = self._seal_schedule_payload(
                conn,
                tenant_id=dest,
                schedule_id=sid,
                payload=self._open_schedule_payload(
                    row["payload"],
                    tenant_id=source,
                    schedule_id=sid,
                    conn=conn,
                ),
            )
            try:
                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET tenant_id = %s,
                        payload = %s,
                        payload_key_id = %s,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    RETURNING id
                    """,
                    (dest, Jsonb(sealed), payload_key_id, now, sid, source),
                ).fetchone()
            except UniqueViolation as exc:
                raise TenantSplitError(
                    f"schedule name {row.get('name')!r} already exists "
                    f"for tenant {dest!r}"
                ) from exc
            if updated is None:
                raise TenantSplitError(
                    f"schedule {sid} cannot split (owner changed)"
                )
            conn.execute(
                """
                UPDATE job_dag_batches
                SET tenant_id = %s
                WHERE schedule_id = %s AND tenant_id = %s
                """,
                (dest, sid, source),
            )
            conn.execute(
                """
                UPDATE schedule_blackouts
                SET tenant_id = %s,
                    updated_at = %s
                WHERE schedule_id = %s AND tenant_id = %s
                """,
                (dest, now, sid, source),
            )
            if hasattr(self, "_move_workflow_runs_for_schedule"):
                self._move_workflow_runs_for_schedule(
                    conn,
                    schedule_id=sid,
                    from_tenant=source,
                    to_tenant=dest,
                    now=now,
                )
            if hasattr(self, "_reseal_schedule_generations"):
                self._reseal_schedule_generations(
                    conn,
                    schedule_id=sid,
                    from_tenant=source,
                    to_tenant=dest,
                )

