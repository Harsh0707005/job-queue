"""Clone / re-run an existing job as a new pending job (tenant-scoped)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from job_queue.db import transaction
from job_queue.models import Job, JobStatus
from job_queue.queue._common import (
    CloneError,
    _JOB_COLUMNS,
    _as_utc,
    _utcnow,
)


class JobsCloneMixin:
    def clone_job(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        payload: dict[str, Any] | None = None,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        dedupe_key: str | None = None,
    ) -> Job:
        """Re-run *job_id* as a new pending job that points at the source.

        Copies queue, payload, priority, skills, labels, retry/timeout policy,
        deadline offset, TTL duration, hops, and last-worker preference.
        Runtime state (lease, attempts, checkpoint, result, errors, failed
        workers, graph links) is not copied. Pass *payload* to replace the
        body; *dedupe_key* is never inherited so an active source cannot
        block the re-run.

        The clone insert and the source ``cloned`` audit row commit in one
        transaction. The source job is locked ``FOR UPDATE`` so concurrent
        clones cannot race ``job_audit_events.seq``. A failure after insert
        rolls the clone back, so a retry cannot create a second copy.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload must be a dict when provided")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if dedupe_key is not None and dedupe_key == "":
            raise ValueError("dedupe_key must be non-empty when provided")

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
                raise ValueError(
                    f"tenant {tenant_id!r} is offboarding; enqueue is rejected"
                )
            if self._is_tenant_restoring(conn, tenant_id):
                raise ValueError(
                    f"tenant {tenant_id!r} is restoring; enqueue is rejected"
                )
            self._assert_tenant_mutable(conn, tenant_id, action="clone")
            source_row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                FOR UPDATE
                """,
                (job_id, tenant_id),
            ).fetchone()
            if source_row is None:
                raise CloneError(
                    f"job {job_id} not found for tenant {tenant_id!r}"
                )
            source = self._job(source_row, conn=conn)
            body = dict(payload) if payload is not None else dict(source.payload)
            deadline_at = None
            if source.deadline_at is not None:
                created = _as_utc(source.created_at)
                deadline_at = now + (_as_utc(source.deadline_at) - created)
            ttl_seconds = None
            if source.expires_at is not None:
                created = _as_utc(source.created_at)
                ttl_seconds = (_as_utc(source.expires_at) - created).total_seconds()
                if ttl_seconds < 1:
                    ttl_seconds = 1.0
            expires, exp_action = self._resolve_job_expiry(
                conn,
                tenant_id=tenant_id,
                now=now,
                ttl_seconds=ttl_seconds,
                expires_at=None,
                expiry_action=source.expiry_action,
            )
            clone = self._enqueue_pending_on_conn(
                conn,
                tenant_id=tenant_id,
                payload=body,
                queue_name=source.queue_name,
                priority=source.priority,
                max_attempts=source.max_attempts,
                visibility_timeout_seconds=source.visibility_timeout_seconds,
                max_runtime_seconds=source.max_runtime_seconds,
                retry_backoff_base_seconds=source.retry_backoff_base_seconds,
                retry_backoff_max_seconds=source.retry_backoff_max_seconds,
                run_at=run_at,
                dedupe_key=dedupe_key,
                dep_ids=[],
                label_list=list(source.labels),
                skill_list=list(source.required_skills),
                resource_cost={k: int(v) for k, v in (source.resource_cost or ())},
                deadline=deadline_at,
                estimated_duration_seconds=source.estimated_duration_seconds,
                preemptible=source.preemptible,
                now=now,
                jid=uuid4(),
                expires=expires,
                exp_action=exp_action,
                prefer_lw=source.prefer_last_worker,
                payload_kind=self._payload_kind(body),
                cloned_from_job_id=source.id,
                max_worker_hops=source.max_worker_hops,
                quarantine_action=source.quarantine_action,
            )
            if clone.cloned_from_job_id != source.id:
                raise CloneError(
                    f"job {job_id} cannot be cloned: dedupe_key "
                    f"{dedupe_key!r} is already active on job {clone.id}",
                    job=clone,
                )
            self._append_job_audit(
                conn,
                job_row=source_row,
                event_type="cloned",
                from_status=source_row["status"],
                actor=None,
                detail={"clone_job_id": str(clone.id)},
                now=now,
            )
            out = clone

        if (
            out.status
            in (JobStatus.AWAITING_SKILLS, JobStatus.AWAITING_CAPACITY)
            and hasattr(self, "_rescan_awaiting_skills_after_commit")
        ):
            self._rescan_awaiting_skills_after_commit()
            refreshed = self.get(out.id, tenant_id=out.tenant_id)
            if refreshed is not None:
                out = refreshed
        return out
