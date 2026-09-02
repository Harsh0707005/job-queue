"""Cancel and replay one fan-out generation as a closed set."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation

from job_queue.db import transaction
from job_queue.models import Job, ScheduleGenerationOp
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    _SCHEDULE_INFLIGHT_STATUSES,
    ScheduleError,
    SpendCapError,
    _utcnow,
)


class SchedulesGenerationOpsMixin:
    def cancel_schedule_generation(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        iteration: int | None = None,
    ) -> ScheduleGenerationOp:
        """Fail generation *iteration* and cancel every same-tenant job in it.

        The closed set is the named schedule plus same-tenant upstreams,
        waiters, and joins for that fan-out. Foreign-tenant members are
        ignored (no leak). A missing iteration defaults to the schedule's
        current ``tick_iteration``.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            named, members, iteration = self._lock_generation_set(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                iteration=iteration,
                require_closed=False,
            )
            cancelled = self._void_and_cancel_generation(
                conn,
                tenant_id=tenant_id,
                members=members,
                iteration=iteration,
                rewind_ticks=False,
                now=now,
            )
            self._record_metric(
                conn,
                tenant_id=tenant_id,
                metric="schedule_generation_cancel",
                count=1,
                now=now,
            )
            updated = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            assert updated is not None
            return ScheduleGenerationOp(
                schedule=self._schedule(updated),
                iteration=iteration,
                action="cancel",
                members=tuple(members),
                cancelled_job_ids=tuple(cancelled),
            )

    def replay_schedule_generation(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
        iteration: int | None = None,
    ) -> ScheduleGenerationOp:
        """Rematerialize generation *iteration* for the closed fan-out set.

        Cancels leftover jobs, voids the payload, rewinds waiter/join
        watermarks for that iteration only, then enqueues the upstream
        tick again. Later generations are refused so catch_up cannot dump
        a backlog. An incomplete set (moved or deleted member) is refused
        so a partial join cannot fire.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            named, members, iteration = self._lock_generation_set(
                conn,
                schedule_id=schedule_id,
                tenant_id=tenant_id,
                iteration=iteration,
                require_closed=True,
            )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is offboarding; replay is refused"
                )
            self._assert_tenant_mutable(
                conn, tenant_id, action="replay", error=ScheduleError
            )
            if self._tenant_claims_blocked(conn, tenant_id, now=now):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is paused or the breaker is open"
                )
            rows = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE tenant_id = %s AND id = ANY(%s)
                """,
                (tenant_id, list(members)),
            ).fetchall()
            by_id = {row["id"]: row for row in rows}
            latest = max(int(r.get("tick_iteration") or 0) for r in rows)
            if latest > iteration:
                raise ScheduleError(
                    f"schedule {schedule_id} cannot replay generation "
                    f"{iteration}: a later generation is already live"
                )
            roots = self._fanout_roots(rows)
            if not roots:
                roots = [named["id"]]
            for rid in roots:
                root = by_id.get(rid)
                if root is None:
                    raise ScheduleError(
                        f"schedule {schedule_id} cannot replay generation "
                        f"{iteration}: the closed set is incomplete"
                    )
                if not bool(root["enabled"]):
                    raise ScheduleError(
                        f"schedule {rid} is disabled"
                    )
                qname = str(root["queue_name"])
                if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
                    conn, tenant_id, qname
                ):
                    raise ScheduleError(f"queue {qname!r} is draining")
                if self._is_schedule_blacked_out(conn, tenant_id, rid, now):
                    raise ScheduleError(
                        f"schedule {rid} is inside a blackout window"
                    )
                group = root.get("mutex_group")
                if group:
                    if not self._try_lock_schedule_mutex_group(
                        conn, tenant_id, str(group)
                    ) or self._schedule_mutex_blocked(
                        conn, tenant_id=tenant_id, mutex_group=str(group)
                    ):
                        raise ScheduleError(
                            f"schedule {rid} is blocked by mutex "
                            f"group {group!r}"
                        )

            cancelled = self._void_and_cancel_generation(
                conn,
                tenant_id=tenant_id,
                members=members,
                iteration=iteration,
                rewind_ticks=True,
                now=now,
            )

            new_jobs: list[Job] = []
            for rid in roots:
                root = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE id = %s AND tenant_id = %s
                    FOR UPDATE
                    """,
                    (rid, tenant_id),
                ).fetchone()
                assert root is not None
                try:
                    first, jobs, _refs, _bid, _bkey = (
                        self._materialize_schedule_tick(
                            conn,
                            root,
                            now=now,
                            scheduled_for=now,
                            policy=str(
                                root.get("misfire_policy") or "catch_up_one"
                            ),
                            skipped=0,
                            extra_detail={
                                "fire": "replay",
                                "iteration": iteration,
                            },
                            tick_kind="replay",
                            iteration=iteration,
                        )
                    )
                except UniqueViolation as exc:
                    raise ScheduleError(
                        f"schedule {rid} already has an in-flight job"
                    ) from exc
                except SpendCapError:
                    raise
                conn.execute(
                    """
                    UPDATE schedules
                    SET last_enqueued_at = %s,
                        tick_iteration = %s,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (now, iteration, now, rid, tenant_id),
                )
                new_jobs.extend(jobs or (first,))
            self._record_metric(
                conn,
                tenant_id=tenant_id,
                metric="schedule_generation_replay",
                count=1,
                now=now,
            )
            updated = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            assert updated is not None
            return ScheduleGenerationOp(
                schedule=self._schedule(updated),
                iteration=iteration,
                action="replay",
                members=tuple(members),
                cancelled_job_ids=tuple(cancelled),
                job=new_jobs[0] if new_jobs else None,
                jobs=tuple(new_jobs),
            )

    def _lock_generation_set(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        tenant_id: str,
        iteration: int | None,
        require_closed: bool,
    ) -> tuple[Any, list[UUID], int]:
        named = conn.execute(
            f"""
            SELECT {_SCHEDULE_COLUMNS}
            FROM schedules
            WHERE id = %s AND tenant_id = %s
            FOR UPDATE
            """,
            (schedule_id, tenant_id),
        ).fetchone()
        if named is None:
            raise ScheduleError(
                f"schedule {schedule_id} not found for tenant {tenant_id!r}"
            )
        resolved = (
            int(iteration)
            if iteration is not None
            else int(named.get("tick_iteration") or 0)
        )
        if resolved < 1:
            raise ScheduleError(
                f"schedule {schedule_id} has no fan-out generation to "
                f"{'replay' if require_closed else 'cancel'}"
            )
        members, closed = self._fanout_member_ids(conn, named)
        if require_closed and not closed:
            raise ScheduleError(
                f"schedule {schedule_id} cannot replay generation "
                f"{resolved}: the closed set is incomplete"
            )
        if members:
            conn.execute(
                f"""
                SELECT id
                FROM schedules
                WHERE tenant_id = %s AND id = ANY(%s)
                ORDER BY id
                FOR UPDATE
                """,
                (tenant_id, list(members)),
            )
        return named, members, resolved

    @staticmethod
    def _fanout_roots(rows: list[Any]) -> list[UUID]:
        waited: set[UUID] = set()
        for row in rows:
            waited.update(row.get("waits_on") or [])
        roots: list[UUID] = []
        for row in rows:
            sid = row["id"]
            waits = list(row.get("waits_on") or [])
            joins = list(row.get("joins_on") or [])
            if sid in waited or (not waits and not joins):
                roots.append(sid)
        return roots

    def _fanout_member_ids(
        self,
        conn: Any,
        sched_row: Any,
    ) -> tuple[list[UUID], bool]:
        """Same-tenant fan-out members. *closed* is False if any id is missing."""
        tenant_id = str(sched_row["tenant_id"])
        sid = sched_row["id"]
        waits = list(sched_row.get("waits_on") or [])
        joins = list(sched_row.get("joins_on") or [])

        def _present(ids: list[UUID]) -> list[UUID]:
            if not ids:
                return []
            rows = conn.execute(
                """
                SELECT id
                FROM schedules
                WHERE tenant_id = %s AND id = ANY(%s)
                """,
                (tenant_id, list(ids)),
            ).fetchall()
            return [r["id"] for r in rows]

        if joins:
            waiter_ids = _present(list(joins))
            if len(waiter_ids) != len(joins):
                return sorted({sid, *waiter_ids}, key=str), False
            common: set[UUID] | None = None
            wrows = conn.execute(
                """
                SELECT id, waits_on
                FROM schedules
                WHERE tenant_id = %s AND id = ANY(%s)
                """,
                (tenant_id, waiter_ids),
            ).fetchall()
            for row in wrows:
                ids = set(row.get("waits_on") or [])
                common = ids if common is None else (common & ids)
            ups = list(common or [])
            up_ids = _present(ups)
            closed = bool(ups) and len(up_ids) == len(ups)
            return sorted({sid, *waiter_ids, *up_ids}, key=str), closed

        if waits:
            up_ids = _present(list(waits))
            closed = len(up_ids) == len(waits)
            siblings = conn.execute(
                """
                SELECT id
                FROM schedules
                WHERE tenant_id = %s AND waits_on && %s
                """,
                (tenant_id, list(waits)),
            ).fetchall()
            waiter_ids = [r["id"] for r in siblings] or [sid]
            join_rows = conn.execute(
                """
                SELECT id, joins_on
                FROM schedules
                WHERE tenant_id = %s AND joins_on && %s
                """,
                (tenant_id, waiter_ids),
            ).fetchall()
            join_ids: list[UUID] = []
            for row in join_rows:
                needed = list(row.get("joins_on") or [])
                found = _present(needed)
                if len(found) != len(needed):
                    closed = False
                    continue
                join_ids.append(row["id"])
            return sorted({sid, *up_ids, *waiter_ids, *join_ids}, key=str), closed

        waiter_rows = conn.execute(
            """
            SELECT id
            FROM schedules
            WHERE tenant_id = %s AND %s = ANY(waits_on)
            """,
            (tenant_id, sid),
        ).fetchall()
        waiter_ids = [r["id"] for r in waiter_rows]
        join_rows = conn.execute(
            """
            SELECT id, joins_on
            FROM schedules
            WHERE tenant_id = %s AND joins_on && %s
            """,
            (tenant_id, waiter_ids or [sid]),
        ).fetchall()
        join_ids: list[UUID] = []
        closed = True
        for row in join_rows:
            needed = list(row.get("joins_on") or [])
            found = _present(needed)
            if len(found) != len(needed):
                closed = False
                continue
            join_ids.append(row["id"])
        return sorted({sid, *waiter_ids, *join_ids}, key=str), closed

    def _void_schedule_generation(
        self,
        conn: Any,
        *,
        schedule_id: UUID,
        tenant_id: str,
        iteration: int,
        now: Any,
    ) -> None:
        conn.execute(
            """
            INSERT INTO schedule_generations (
                schedule_id, iteration, tenant_id, status,
                payload, payload_key_id, source_job_ids,
                created_at, updated_at
            ) VALUES (%s, %s, %s, 'failed', NULL, NULL, '{}', %s, %s)
            ON CONFLICT (schedule_id, iteration) DO UPDATE
            SET status = 'failed',
                payload = NULL,
                payload_key_id = NULL,
                tenant_id = EXCLUDED.tenant_id,
                updated_at = EXCLUDED.updated_at
            WHERE schedule_generations.tenant_id = EXCLUDED.tenant_id
            """,
            (schedule_id, iteration, tenant_id, now, now),
        )

    def _void_and_cancel_generation(
        self,
        conn: Any,
        *,
        tenant_id: str,
        members: list[UUID],
        iteration: int,
        rewind_ticks: bool,
        now: Any,
    ) -> list[UUID]:
        inflight = list(_SCHEDULE_INFLIGHT_STATUSES)
        rows = conn.execute(
            f"""
            SELECT {_JOB_COLUMNS}
            FROM jobs
            WHERE tenant_id = %s
              AND schedule_id = ANY(%s)
              AND (
                tick_iteration = %s
                OR (
                    tick_iteration IS NULL
                    AND status = ANY(%s)
                    AND EXISTS (
                        SELECT 1
                        FROM schedules s
                        WHERE s.id = jobs.schedule_id
                          AND s.tenant_id = %s
                          AND s.tick_iteration = %s
                    )
                )
            )
            ORDER BY id
            FOR UPDATE
            """,
            (
                tenant_id,
                list(members),
                iteration,
                inflight,
                tenant_id,
                iteration,
            ),
        ).fetchall()
        cancelled: list[UUID] = []
        for row in rows:
            job = self._cancel_locked_row(
                conn,
                row=row,
                tenant_id=tenant_id,
                now=now,
                reason=f"fan-out generation {iteration} cancelled",
            )
            if job is not None:
                cancelled.append(job.id)
        for mid in members:
            self._void_schedule_generation(
                conn,
                schedule_id=mid,
                tenant_id=tenant_id,
                iteration=iteration,
                now=now,
            )
            if rewind_ticks:
                conn.execute(
                    """
                    UPDATE schedules
                    SET last_completed_iteration = CASE
                            WHEN last_completed_iteration IS NULL THEN NULL
                            WHEN last_completed_iteration = %s
                                THEN NULLIF(%s, 0)
                            ELSE last_completed_iteration
                        END,
                        tick_iteration = CASE
                            WHEN tick_iteration = %s THEN %s
                            ELSE tick_iteration
                        END,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (
                        iteration,
                        iteration - 1,
                        iteration,
                        iteration - 1,
                        now,
                        mid,
                        tenant_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE schedules
                    SET last_completed_iteration = CASE
                            WHEN last_completed_iteration IS NULL THEN NULL
                            WHEN last_completed_iteration = %s
                                THEN NULLIF(%s, 0)
                            ELSE last_completed_iteration
                        END,
                        updated_at = %s
                    WHERE id = %s AND tenant_id = %s
                    """,
                    (iteration, iteration - 1, now, mid, tenant_id),
                )
        return cancelled
