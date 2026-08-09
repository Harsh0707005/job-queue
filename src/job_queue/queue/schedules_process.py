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
    _utcnow,
)


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
        job in the same transaction, then advances ``next_run_at`` via cron
        (next occurrence after *now*) or ``now + interval_seconds`` — no
        catch-up burst of missed windows.

        **Schedule fires bypass pending caps and enqueue rate limits** so
        timed work still runs under tenant throttles.

        **Paused tenants are skipped entirely** — no jobs are enqueued for their
        schedules while paused, so ticks do not pile up. After ``resume_tenant``,
        overdue schedules fire once (clean pickup).

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

        # Shared filter: never tick schedules for paused tenants.
        paused_filter = """
            AND NOT EXISTS (
                SELECT 1
                FROM tenant_settings ts
                WHERE ts.tenant_id = schedules.tenant_id
                  AND ts.paused = TRUE
            )
        """

        with transaction(self._pool) as conn:
            if tenant_id is not None:
                if self._is_tenant_paused(conn, tenant_id):
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
                    (now, tenant_id, limit),
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
                    (now, limit),
                ).fetchall()

            for sched_row in due:
                # Race: tenant paused after SELECT.
                if self._is_tenant_paused(conn, sched_row["tenant_id"]):
                    self._record_metric(
                        conn,
                        tenant_id=str(sched_row["tenant_id"]),
                        metric="schedule_skip",
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
                    next_run = self._schedule_next_run(sched_row, now=now)
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
                        job_row = conn.execute(
                            f"""
                            INSERT INTO jobs (
                                id, tenant_id, queue_name, payload, status, priority,
                                attempts, max_attempts, visibility_timeout_seconds,
                                max_runtime_seconds,
                                retry_backoff_base_seconds, retry_backoff_max_seconds,
                                available_at, dedupe_key, schedule_id, depends_on,
                                labels, required_skills,
                                created_at, updated_at, partition_id, payload_key_id
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                0, %s, %s,
                                3600,
                                1, 300,
                                %s, NULL, %s, %s,
                                %s, %s,
                                %s, %s, %s, %s
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
                except UniqueViolation:
                    # Race: another ticker inserted an active job for this schedule.
                    continue

                assert job_row is not None

                updated = conn.execute(
                    f"""
                    UPDATE schedules
                    SET next_run_at = %s,
                        last_enqueued_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING {_SCHEDULE_COLUMNS}
                    """,
                    (next_run, now, now, sched_row["id"]),
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

        if fires and hasattr(self, "_rescan_awaiting_skills_after_commit"):
            if any(f.job.required_skills for f in fires):
                self._rescan_awaiting_skills_after_commit()
        return fires
