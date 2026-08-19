"""Manual one-shot schedule fire (do not wait for the next cron tick)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from job_queue.db import transaction

from job_queue.models import Job, JobStatus, ScheduleFire
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _SCHEDULE_COLUMNS,
    _SCHEDULE_INFLIGHT_STATUSES,
    ScheduleError,
    SpendCapError,
    _as_utc,
    _dag_jobs_from_row,
    _utcnow,
)


class SchedulesFireMixin:
    def fire_schedule(
        self,
        schedule_id: UUID,
        *,
        tenant_id: str,
    ) -> ScheduleFire:
        """Enqueue one job from *schedule_id* now, without waiting for cron.

        Does **not** advance ``next_run_at`` or apply misfire policy — the
        next regular tick still runs. Same rules as ``process_due_schedules``
        otherwise: tenant pause / breaker, covering blackout, queue drain,
        disabled schedule, offboarding, in-flight unique job, and spend caps
        all refuse. Pending caps and enqueue rate limits are bypassed.

        Concurrent callers serialize on the schedule row
        (``FOR UPDATE``). A ``mutex_group`` peer that is already in
        flight refuses this fire the same way an in-flight self tick does.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        now = _utcnow()
        with transaction(self._pool) as conn:
            sched_row = conn.execute(
                f"""
                SELECT {_SCHEDULE_COLUMNS}
                FROM schedules
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (schedule_id, tenant_id),
            ).fetchone()
            if sched_row is None:
                raise ScheduleError(
                    f"schedule {schedule_id} not found for tenant {tenant_id!r}"
                )
            if not bool(sched_row["enabled"]):
                raise ScheduleError(
                    f"schedule {schedule_id} is disabled"
                )
            if self._is_tenant_offboarding(conn, tenant_id):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is offboarding; fire is refused"
                )
            if self._tenant_claims_blocked(conn, tenant_id, now=now):
                raise ScheduleError(
                    f"tenant {tenant_id!r} is paused or the breaker is open"
                )
            if hasattr(self, "_is_queue_draining") and self._is_queue_draining(
                conn, tenant_id, str(sched_row["queue_name"])
            ):
                raise ScheduleError(
                    f"queue {sched_row['queue_name']!r} is draining"
                )
            if self._is_schedule_blacked_out(conn, tenant_id, schedule_id, now):
                raise ScheduleError(
                    f"schedule {schedule_id} is inside a blackout window"
                )
            group = sched_row.get("mutex_group")
            if group:
                if not self._try_lock_schedule_mutex_group(
                    conn, tenant_id, str(group)
                ) or self._schedule_mutex_blocked(
                    conn, tenant_id=tenant_id, mutex_group=str(group)
                ):
                    raise ScheduleError(
                        f"schedule {schedule_id} is blocked by mutex "
                        f"group {group!r}"
                    )
            else:
                active = conn.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE schedule_id = %s
                      AND status = ANY(%s)
                    LIMIT 1
                    """,
                    (schedule_id, list(_SCHEDULE_INFLIGHT_STATUSES)),
                ).fetchone()
                if active is not None:
                    raise ScheduleError(
                        f"schedule {schedule_id} already has an in-flight job"
                    )

            try:
                first, jobs, refs, batch_id, batch_key = (
                    self._materialize_schedule_tick(
                        conn,
                        sched_row,
                        now=now,
                        scheduled_for=now,
                        policy=str(
                            sched_row.get("misfire_policy") or "catch_up_one"
                        ),
                        skipped=0,
                        extra_detail={"fire": "manual"},
                        tick_kind="manual",
                    )
                )
            except UniqueViolation as exc:
                raise ScheduleError(
                    f"schedule {schedule_id} already has an in-flight job"
                ) from exc
            except SpendCapError:
                raise

            updated = conn.execute(
                f"""
                UPDATE schedules
                SET last_enqueued_at = %s,
                    updated_at = %s
                WHERE id = %s AND tenant_id = %s
                RETURNING {_SCHEDULE_COLUMNS}
                """,
                (now, now, schedule_id, tenant_id),
            ).fetchone()
            assert updated is not None
            self._record_metric(
                conn,
                tenant_id=tenant_id,
                metric="schedule_fire",
                count=1,
                now=now,
            )
            self._record_metric(
                conn,
                tenant_id=tenant_id,
                metric="schedule_manual_fire",
                count=1,
                now=now,
            )
            fire = ScheduleFire(
                schedule=self._schedule(updated),
                job=first,
                jobs=jobs,
                refs=refs,
                batch_id=batch_id,
                batch_key=batch_key,
            )

        if any(j.required_skills for j in fire.jobs or (fire.job,)) and hasattr(
            self, "_rescan_awaiting_skills_after_commit"
        ):
            self._rescan_awaiting_skills_after_commit()
        return fire

    def _materialize_schedule_tick(
        self,
        conn: Any,
        sched_row: Any,
        *,
        now: datetime,
        scheduled_for: datetime,
        policy: str,
        skipped: int,
        extra_detail: dict[str, Any] | None = None,
        tick_kind: str = "tick",
    ) -> tuple[Job, tuple[Job, ...], tuple[str, ...], UUID | None, str | None]:
        """Enqueue one job or one whole DAG. Caller holds the schedule lock."""
        dag = _dag_jobs_from_row(sched_row.get("dag_jobs"))
        if not dag:
            row = self._insert_schedule_job(
                conn,
                sched_row,
                now=now,
                scheduled_for=scheduled_for,
                policy=policy,
                skipped=skipped,
                extra_detail=extra_detail,
            )
            job = self._job(row)
            return job, (job,), (), None, None

        default_payload = self._open_schedule_payload(
            sched_row["payload"],
            tenant_id=str(sched_row["tenant_id"]),
            schedule_id=sched_row["id"],
            conn=conn,
        )
        jobs_spec: list[dict[str, Any]] = []
        for node in dag:
            payload = node.get("payload")
            if payload is None:
                payload = dict(default_payload)
            jobs_spec.append(
                {
                    "ref": node["ref"],
                    "payload": dict(payload),
                    "depends_on": list(node.get("depends_on") or []),
                    "priority": (
                        sched_row["priority"]
                        if node.get("priority") is None
                        else int(node["priority"])
                    ),
                    "max_attempts": (
                        sched_row["max_attempts"]
                        if node.get("max_attempts") is None
                        else int(node["max_attempts"])
                    ),
                    "visibility_timeout_seconds": (
                        sched_row["visibility_timeout_seconds"]
                        if node.get("visibility_timeout_seconds") is None
                        else int(node["visibility_timeout_seconds"])
                    ),
                    "labels": (
                        list(sched_row.get("labels") or [])
                        if node.get("labels") is None
                        else node.get("labels")
                    ),
                    "required_skills": (
                        list(sched_row.get("required_skills") or [])
                        if node.get("required_skills") is None
                        else node.get("required_skills")
                    ),
                }
            )
        specs = self._normalize_dag_specs(jobs_spec)
        slot = _as_utc(scheduled_for).isoformat()
        batch_key = f"sched:{sched_row['id']}:{tick_kind}:{slot}"
        detail = {
            "schedule_id": str(sched_row["id"]),
            "cron_expr": sched_row.get("cron_expr"),
            "misfire_policy": policy,
            "scheduled_for": slot,
            "missed_skipped": skipped,
        }
        if extra_detail:
            detail.update(extra_detail)
        with conn.transaction():
            result = self._enqueue_dag_on_conn(
                conn,
                tenant_id=str(sched_row["tenant_id"]),
                queue_name=str(sched_row["queue_name"]),
                batch_key=batch_key,
                specs=specs,
                now=now,
                schedule_id=sched_row["id"],
                bypass_pending_and_rate=True,
                extra_audit=detail,
            )
        first = result.jobs[0]
        return first, result.jobs, result.refs, result.batch_id, result.batch_key

    def _insert_schedule_job(
        self,
        conn: Any,
        sched_row: Any,
        *,
        now: datetime,
        scheduled_for: datetime,
        policy: str,
        skipped: int,
        extra_detail: dict[str, Any] | None = None,
    ) -> Any:
        """Insert one schedule-materialized job. Caller holds the schedule lock.

        Spend caps apply. Pending caps and enqueue rate limits do not.
        """
        job_id = uuid4()
        payload = self._open_schedule_payload(
            sched_row["payload"],
            tenant_id=str(sched_row["tenant_id"]),
            schedule_id=sched_row["id"],
            conn=conn,
        )
        with conn.transaction():
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
            assert job_row is not None
            if hasattr(self, "_recheck_awaiting_skills_row_locked"):
                job_row = self._recheck_awaiting_skills_row_locked(
                    conn, job_row, now=now
                )
            detail = {
                "schedule_id": str(sched_row["id"]),
                "cron_expr": sched_row.get("cron_expr"),
                "misfire_policy": policy,
                "scheduled_for": scheduled_for.isoformat(),
                "missed_skipped": skipped,
            }
            if extra_detail:
                detail.update(extra_detail)
            self._append_job_audit(
                conn,
                job_row=job_row,
                event_type="enqueued",
                from_status=None,
                actor="schedule",
                detail=detail,
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
            return job_row
