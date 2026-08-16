"""JobQueue process_due_schedules."""

from __future__ import annotations

from uuid import uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import transaction
from job_queue.models import (
    JobStatus,
    ScheduleFire,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    SpendCapError,
    _as_utc,
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
    ) -> list[ScheduleFire]:
        """Enqueue jobs for schedules that are due and advance their next run.

        Safe for concurrent callers: due rows are claimed with
        ``FOR UPDATE SKIP LOCKED``. Each claimed schedule enqueues at most one
        job in the same transaction.

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

        **No overlapping runs:** if a previous job for the same schedule is
        still ``pending`` or ``leased``, this tick is skipped and
        ``next_run_at`` is left unchanged so the schedule remains due and can
        fire as soon as that in-flight job completes.

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

                # Skip if a prior run is still in flight (incl. park waits).
                active = conn.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE schedule_id = %s
                      AND status IN (%s, %s, %s, %s)
                    LIMIT 1
                    """,
                    (
                        sched_row["id"],
                        JobStatus.PENDING.value,
                        JobStatus.LEASED.value,
                        JobStatus.AWAITING_APPROVAL.value,
                        JobStatus.AWAITING_CHILDREN.value,
                    ),
                ).fetchone()
                if active is not None:
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_skip",
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

                if not slots:
                    conn.execute(
                        f"""
                        UPDATE schedules
                        SET next_run_at = %s,
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
                            skipped,
                            skipped,
                            now,
                            now,
                            sched_row["id"],
                        ),
                    )
                    if skipped:
                        self._record_metric(
                            conn,
                            tenant_id=str(sched_row["tenant_id"]),
                            metric="schedule_miss",
                            count=skipped,
                            now=now,
                        )
                    continue

                new_remaining = None
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
                job_id = uuid4()
                payload = self._open_schedule_payload(
                    sched_row["payload"],
                    tenant_id=str(sched_row["tenant_id"]),
                    schedule_id=sched_row["id"],
                    conn=conn,
                )

                try:
                    with conn.transaction():
                        # Intentionally no pending-cap / enqueue-rate checks:
                        # schedule materialization is system-timed work.
                        # Spend caps still apply — scheduled jobs are billed
                        # enqueues and must not bypass a tenant quota.
                        if hasattr(self, "_consume_spend_enqueues"):
                            self._consume_spend_enqueues(
                                conn,
                                str(sched_row["tenant_id"]),
                                extra=1,
                                now=now,
                            )
                        sched_labels = list(sched_row.get("labels") or [])
                        sched_skills = list(sched_row.get("required_skills") or [])
                        job_status = (
                            self._enqueue_status_for_skills(conn, sched_skills)
                            if hasattr(self, "_enqueue_status_for_skills")
                            else JobStatus.PENDING.value
                        )
                        part_id = (
                            self._compute_partition_id(
                                conn,
                                tenant_id=str(sched_row["tenant_id"]),
                                job_id=job_id,
                            )
                            if hasattr(self, "_compute_partition_id")
                            else 0
                        )
                        sealed, payload_key_id = self._seal_job_payload(
                            conn,
                            tenant_id=str(sched_row["tenant_id"]),
                            job_id=job_id,
                            payload=payload,
                        )
                        expires, exp_action = (
                            self._resolve_job_expiry(
                                conn,
                                tenant_id=str(sched_row["tenant_id"]),
                                now=now,
                                ttl_seconds=None,
                                expires_at=None,
                                expiry_action=None,
                            )
                            if hasattr(self, "_resolve_job_expiry")
                            else (None, "dead")
                        )
                        job_row = conn.execute(
                            f"""
                            INSERT INTO jobs (
                                id, tenant_id, queue_name, payload, status, priority,
                                attempts, max_attempts, visibility_timeout_seconds,
                                max_runtime_seconds,
                                retry_backoff_base_seconds, retry_backoff_max_seconds,
                                available_at, dedupe_key, schedule_id, depends_on,
                                labels, required_skills,
                                created_at, updated_at, partition_id, payload_key_id,
                                expires_at, expiry_action
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                0, %s, %s,
                                3600,
                                1, 300,
                                %s, NULL, %s, %s,
                                %s, %s,
                                %s, %s, %s, %s,
                                %s, %s
                            )
                            RETURNING {_JOB_COLUMNS}
                            """,
                            (
                                job_id,
                                sched_row["tenant_id"],
                                sched_row["queue_name"],
                                Jsonb(sealed),
                                job_status,
                                sched_row["priority"],
                                sched_row["max_attempts"],
                                sched_row["visibility_timeout_seconds"],
                                now,
                                sched_row["id"],
                                [],
                                sched_labels,
                                sched_skills,
                                now,
                                now,
                                part_id,
                                payload_key_id,
                                expires,
                                exp_action,
                            ),
                        ).fetchone()
                        if job_row is not None:
                            if hasattr(self, "_recheck_awaiting_skills_row_locked"):
                                job_row = self._recheck_awaiting_skills_row_locked(
                                    conn, job_row, now=now
                                )
                            self._append_job_audit(
                                conn,
                                job_row=job_row,
                                event_type="enqueued",
                                from_status=None,
                                actor="schedule",
                                detail={
                                    "schedule_id": str(sched_row["id"]),
                                    "cron_expr": sched_row.get("cron_expr"),
                                    "misfire_policy": policy,
                                    "scheduled_for": slots[0].isoformat(),
                                    "missed_skipped": skipped,
                                },
                                now=now,
                            )
                            if hasattr(self, "_record_usage"):
                                self._record_usage(
                                    conn,
                                    tenant_id=str(sched_row["tenant_id"]),
                                    now=now,
                                    enqueues=1,
                                )
                            if hasattr(self, "_stamp_job_quarantine"):
                                self._stamp_job_quarantine(
                                    conn,
                                    job_id=job_row["id"],
                                    tenant_id=str(sched_row["tenant_id"]),
                                )
                except UniqueViolation:
                    # Race: another ticker inserted an active job for this schedule.
                    continue
                except SpendCapError:
                    # Tenant quota exhausted; leave next_run so it retries
                    # after the window resets.
                    continue

                assert job_row is not None

                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET next_run_at = %s,
                        last_enqueued_at = %s,
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
                        skipped,
                        skipped,
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
                        job=self._job(job_row),
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
            if any(f.job.required_skills for f in fires):
                self._rescan_awaiting_skills_after_commit()
        return fires
