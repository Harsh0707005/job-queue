"""JobQueue process_due_schedules."""

from __future__ import annotations

from uuid import UUID

from psycopg.errors import UniqueViolation

from job_queue.db import transaction
from job_queue.models import (
    ScheduleFire,
)
from job_queue.queue._common import (
    _SCHEDULE_COLUMNS,
    _SCHEDULE_INFLIGHT_STATUSES,
    ScheduleError,
    SpendCapError,
    _utcnow,
)
from job_queue.queue.schedules_blackout import _BLACKOUT_EXISTS_SQL
from job_queue.queue.schedules_crud import (
    MISFIRE_CATCH_UP,
    MISFIRE_CATCH_UP_ONE,
    MISFIRE_SKIP,
    normalize_misfire_policy,
)
from job_queue.queue.tenants_breaker import _TENANT_CLAIMS_BLOCKED_SQL


class SchedulesProcessMixin:
    def process_due_schedules(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        leader_token: UUID | None = None,
        leader_epoch: int | None = None,
    ) -> list[ScheduleFire]:
        """Enqueue jobs for schedules that are due and advance their next run.

        Safe for concurrent callers: due rows are claimed with
        ``FOR UPDATE SKIP LOCKED``. Each claimed schedule enqueues at most one
        tick (one job, or one whole DAG) in the same transaction.

        **Misfire policy** (per schedule) decides what happens when
        ``next_run_at`` is already in the past (tenant pause, coordinator
        down, or a long gap):

        * ``skip`` — record every missed tick, enqueue nothing, jump
          ``next_run_at`` to the next future occurrence.
        * ``catch_up_one`` (default) — enqueue one pickup job, record
          extra misses, jump to the next future occurrence.
        * ``catch_up`` — enqueue one job for the oldest missed tick and
          leave ``next_run_at`` on the next missed slot so later ticks
          drain one-at-a-time up to ``catch_up_max``. Remaining ticks
          are recorded as missed, not enqueued.

        **Schedule fires bypass pending caps and enqueue rate limits** so
        timed work still runs under tenant throttles.

        **Paused tenants, open failure breakers, and covering blackout
        windows are skipped entirely** — ``next_run_at`` is left in the
        past so the policy can run on resume / breaker close / window end.
        No jobs pile up while blocked.

        **No overlapping runs:** if a previous job (or any node of a DAG
        tick) for the same schedule is still pending, leased, awaiting
        approval/children/skills, or frozen, this tick is skipped and
        ``next_run_at`` is left unchanged so the schedule remains due.

        **Mutex groups:** schedules that share ``mutex_group`` on a tenant
        are exclusive. Two due members racing take a *non-blocking*
        per-tenant advisory lock; the loser skips and leaves
        ``next_run_at`` overdue so skip / catch_up_one / catch_up run
        when the group is free — no backlog dump. The lock is
        ``pg_try_advisory_xact_lock`` so a busy group cannot stall
        other tenants in the same ``process_due`` batch. Pause and
        delete only lock their own row (``SKIP LOCKED`` here).

        **Waits-on:** a schedule that lists upstream ids will not fire
        while any of those has a live tick (on any tenant — delete or
        move cannot hide it), a cancelled / dead-lettered / failed tick
        with no later completed job, is due but has not started that
        tick, is paused, or has a paused / inflight mutex-group peer.
        ``next_run_at`` stays overdue so skip / catch_up_one / catch_up
        run when the upstream is clear.

        **Joins-on:** a schedule that lists waiter ids fires one tick
        per generation after every waiter has completed that same
        iteration. A paused waiter or one stuck behind a mutex peer
        keeps the join blocked. ``next_run_at`` stays overdue so
        skip / catch_up_one / catch_up run when the set is clear.
        ``catch_up`` drains at most ``catch_up_max`` generations;
        leftover ready generations are recorded as missed, not
        dumped one-at-a-time.

        Returns a list of ``ScheduleFire`` (updated schedule + enqueued job).
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        now = _utcnow()
        fires: list[ScheduleFire] = []

        # Shared filter: never tick schedules for paused / breaker-open tenants.
        # Draining queues skip fires too (other queues on the tenant still tick).
        paused_filter = f"""
            AND NOT EXISTS (
                SELECT 1
                FROM tenant_settings ts
                WHERE ts.tenant_id = schedules.tenant_id
                  AND {_TENANT_CLAIMS_BLOCKED_SQL}
            )
            AND NOT EXISTS (
                SELECT 1
                FROM tenant_settings ts
                WHERE ts.tenant_id = schedules.tenant_id
                  AND ts.draining_queues IS NOT NULL
                  AND schedules.queue_name = ANY(ts.draining_queues)
            )
            AND NOT {_BLACKOUT_EXISTS_SQL}
        """

        with transaction(self._pool) as conn:
            if hasattr(self, "_fence_control_leader") and not self._fence_control_leader(
                conn, lease_token=leader_token, epoch=leader_epoch
            ):
                return []
            if tenant_id is not None:
                if self._tenant_claims_blocked(conn, tenant_id, now=now):
                    return []
                due = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE enabled = TRUE
                      AND next_run_at <= %s
                      AND tenant_id = %s
                      {paused_filter}
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, tenant_id, now, limit),
                ).fetchall()
            else:
                due = conn.execute(
                    f"""
                    SELECT {_SCHEDULE_COLUMNS}
                    FROM schedules
                    WHERE enabled = TRUE
                      AND next_run_at <= %s
                      {paused_filter}
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, now, limit),
                ).fetchall()

            for sched_row in due:
                # Race: tenant paused / breaker opened / blackout set after SELECT.
                if self._tenant_claims_blocked(
                    conn, sched_row["tenant_id"], now=now
                ) or (
                    hasattr(self, "_is_queue_draining")
                    and self._is_queue_draining(
                        conn,
                        str(sched_row["tenant_id"]),
                        str(sched_row["queue_name"]),
                    )
                ):
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_skip",
                        count=1,
                        now=now,
                    )
                    continue
                if self._is_schedule_blacked_out(
                    conn,
                    str(sched_row["tenant_id"]),
                    sched_row["id"],
                    now,
                ):
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_blackout",
                        count=1,
                        now=now,
                    )
                    continue

                # Skip if a prior run is still in flight (incl. park waits),
                # or a mutex-group peer still holds the exclusive tick.
                group = sched_row.get("mutex_group")
                if group:
                    locked = self._try_lock_schedule_mutex_group(
                        conn,
                        str(sched_row["tenant_id"]),
                        str(group),
                    )
                    blocked = (not locked) or self._schedule_mutex_blocked(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        mutex_group=str(group),
                    )
                    skip_metric = "schedule_mutex" if blocked else None
                else:
                    active = conn.execute(
                        """
                        SELECT id
                        FROM jobs
                        WHERE schedule_id = %s
                          AND status = ANY(%s)
                        LIMIT 1
                        """,
                        (
                            sched_row["id"],
                            list(_SCHEDULE_INFLIGHT_STATUSES),
                        ),
                    ).fetchone()
                    blocked = active is not None or self._schedule_workflow_tick_open(
                        conn, [sched_row["id"]]
                    )
                    skip_metric = "schedule_skip" if blocked else None
                if not blocked and self._schedule_waits_blocked(
                    conn,
                    tenant_id=str(sched_row["tenant_id"]),
                    waits_on=list(sched_row.get("waits_on") or []),
                    now=now,
                ):
                    blocked = True
                    skip_metric = "schedule_wait"
                if not blocked and self._schedule_join_blocked(
                    conn,
                    tenant_id=str(sched_row["tenant_id"]),
                    joins_on=list(sched_row.get("joins_on") or []),
                    tick_iteration=sched_row.get("tick_iteration"),
                    now=now,
                ):
                    blocked = True
                    skip_metric = "schedule_join"
                iteration = self._resolve_tick_iteration(conn, sched_row)
                if not blocked and self._schedule_generation_blocked(
                    conn, sched_row, iteration=iteration
                ):
                    blocked = True
                    skip_metric = "schedule_generation"
                if blocked:
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric=skip_metric or "schedule_skip",
                        count=1,
                        now=now,
                    )
                    continue

                try:
                    policy = normalize_misfire_policy(
                        sched_row.get("misfire_policy")
                    )
                    remaining = sched_row.get("catch_up_remaining")
                    if policy == MISFIRE_SKIP:
                        probe, extra, future = self._misfire_walk(
                            sched_row, now=now, fire_cap=2
                        )
                        if extra or len(probe) > 1:
                            slots, skipped, future = self._misfire_walk(
                                sched_row, now=now, fire_cap=0
                            )
                        else:
                            slots, skipped, future = probe, 0, future
                    elif policy == MISFIRE_CATCH_UP:
                        if remaining is None:
                            fire_cap = int(sched_row.get("catch_up_max") or 8)
                            slots, skipped, future = self._misfire_walk(
                                sched_row, now=now, fire_cap=fire_cap
                            )
                            remaining = len(slots)
                        else:
                            fire_cap = max(0, int(remaining))
                            slots, _ignored, future = self._misfire_walk(
                                sched_row, now=now, fire_cap=fire_cap
                            )
                            skipped = 0
                    else:
                        slots, skipped, future = self._misfire_walk(
                            sched_row, now=now, fire_cap=1
                        )
                        remaining = None
                except ValueError:
                    # Bad stored cron — leave row for ops; skip this tick.
                    import logging as _logging

                    _logging.getLogger(__name__).exception(
                        "schedule %s invalid cron; skipping fire",
                        sched_row["id"],
                    )
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_skip",
                        count=1,
                        now=now,
                    )
                    continue

                join_ids = list(sched_row.get("joins_on") or [])
                watermark = (
                    self._join_ready_watermark(conn, join_ids)
                    if join_ids
                    else 0
                )
                current_iter = int(sched_row.get("tick_iteration") or 0)
                extra_join = max(0, watermark - current_iter - 1)
                stored_remaining = sched_row.get("catch_up_remaining")

                if not slots:
                    consume = watermark if join_ids else current_iter
                    missed = skipped + extra_join
                    conn.execute(
                        f"""
                        UPDATE schedules
                        SET next_run_at = %s,
                            tick_iteration = %s,
                            missed_ticks = missed_ticks + %s,
                            last_misfire_at = CASE
                                WHEN %s > 0 THEN %s
                                ELSE last_misfire_at
                            END,
                            catch_up_remaining = NULL,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            future,
                            consume,
                            missed,
                            missed,
                            now,
                            now,
                            sched_row["id"],
                        ),
                    )
                    if missed:
                        self._record_metric(
                            conn,
                            tenant_id=str(sched_row["tenant_id"]),
                            metric="schedule_miss",
                            count=missed,
                            now=now,
                        )
                    continue

                new_remaining = None
                join_budget = 0
                extra_join_skip = 0
                if join_ids and policy == MISFIRE_CATCH_UP:
                    ready = max(0, watermark - current_iter)
                    if stored_remaining is None:
                        cap = int(sched_row.get("catch_up_max") or 8)
                        join_budget = min(cap, ready)
                        extra_join_skip = max(0, ready - join_budget)
                    else:
                        join_budget = max(0, int(stored_remaining))
                if policy == MISFIRE_CATCH_UP and remaining is not None:
                    left = int(remaining) - 1
                    if left > 0:
                        nxt = self._schedule_tick_after(sched_row, slots[0])
                        if nxt <= now:
                            next_run = nxt
                            new_remaining = left
                        else:
                            next_run = future
                    else:
                        next_run = future
                else:
                    next_run = future
                if join_ids and policy == MISFIRE_CATCH_UP and join_budget > 0:
                    left = join_budget - 1
                    if left > 0:
                        # Drain at most catch_up_max generations, one
                        # tick at a time. Do not snap next_run to now
                        # for leftover generations past the cap.
                        next_run = now
                        new_remaining = left
                    else:
                        next_run = future
                        new_remaining = None

                try:
                    # Intentionally no pending-cap / enqueue-rate checks:
                    # schedule materialization is system-timed work.
                    # Spend caps still apply — scheduled jobs are billed
                    # enqueues and must not bypass a tenant quota.
                    stamp_iter = iteration
                    extra_skip = skipped + extra_join_skip
                    if join_ids and policy == MISFIRE_CATCH_UP:
                        if join_budget <= 1:
                            # Last budgeted tick: skip leftover generations.
                            stamp_iter = max(iteration, watermark)
                    elif join_ids and extra_join:
                        # skip / catch_up_one: one pickup, consume the rest.
                        stamp_iter = watermark
                        extra_skip += extra_join
                    first_job, tick_jobs, tick_refs, batch_id, batch_key, wf_run_id = (
                        self._materialize_schedule_tick(
                            conn,
                            sched_row,
                            now=now,
                            scheduled_for=slots[0],
                            policy=policy,
                            skipped=extra_skip,
                            iteration=iteration,
                        )
                    )
                except UniqueViolation:
                    # Race: another ticker inserted an active job for this schedule.
                    continue
                except ScheduleError:
                    # Waiter vanished between the generation check and
                    # attach — fail closed, do not enqueue an empty join.
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_generation",
                        count=1,
                        now=now,
                    )
                    continue
                except SpendCapError:
                    # Tenant quota exhausted; leave next_run so it retries
                    # after the window resets.
                    continue

                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET next_run_at = %s,
                        last_enqueued_at = %s,
                        tick_iteration = %s,
                        missed_ticks = missed_ticks + %s,
                        last_misfire_at = CASE
                            WHEN %s > 0 THEN %s
                            ELSE last_misfire_at
                        END,
                        catch_up_remaining = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (
                        next_run,
                        now,
                        stamp_iter,
                        extra_skip,
                        extra_skip,
                        now,
                        new_remaining,
                        now,
                        sched_row["id"],
                    ),
                ).fetchone()
                assert updated is not None
                fires.append(
                    ScheduleFire(
                        schedule=self._schedule(updated),
                        job=first_job,
                        jobs=tick_jobs,
                        refs=tick_refs,
                        batch_id=batch_id,
                        batch_key=batch_key,
                        workflow_run_id=wf_run_id,
                    )
                )
                self._record_metric(
                    conn,
                    tenant_id=str(sched_row["tenant_id"]),
                    metric="schedule_fire",
                    count=1,
                    now=now,
                )
                if skipped:
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_miss",
                        count=skipped,
                        now=now,
                    )

        if fires and hasattr(self, "_rescan_awaiting_skills_after_commit"):
            if any(
                f.job.required_skills
                or any(j.required_skills for j in (f.jobs or ()))
                for f in fires
            ):
                self._rescan_awaiting_skills_after_commit()
        return fires
