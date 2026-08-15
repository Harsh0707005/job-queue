"""Clone / re-run an existing job as a new pending job (tenant-scoped)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import Job
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

        source = self.get(job_id, tenant_id=tenant_id)
        if source is None:
            raise CloneError(f"job {job_id} not found for tenant {tenant_id!r}")

        body = dict(payload) if payload is not None else dict(source.payload)
        now = _utcnow()
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

        clone = self.enqueue(
            tenant_id=tenant_id,
            payload=body,
            queue_name=source.queue_name,
            priority=source.priority,
            max_attempts=source.max_attempts,
            visibility_timeout_seconds=source.visibility_timeout_seconds,
            max_runtime_seconds=source.max_runtime_seconds,
            retry_backoff_base_seconds=source.retry_backoff_base_seconds,
            retry_backoff_max_seconds=source.retry_backoff_max_seconds,
            available_at=available_at,
            delay_seconds=delay_seconds,
            dedupe_key=dedupe_key,
            deadline_at=deadline_at,
            estimated_duration_seconds=source.estimated_duration_seconds,
            preemptible=source.preemptible,
            labels=source.labels,
            required_skills=source.required_skills,
            ttl_seconds=ttl_seconds,
            expiry_action=source.expiry_action,
            max_worker_hops=source.max_worker_hops,
            quarantine_action=source.quarantine_action,
            prefer_last_worker=source.prefer_last_worker,
            cloned_from_job_id=source.id,
        )
        if clone.cloned_from_job_id != source.id:
            raise CloneError(
                f"job {job_id} cannot be cloned: dedupe_key "
                f"{dedupe_key!r} is already active on job {clone.id}",
                job=clone,
            )
        self._audit_clone_source(source.id, tenant_id=tenant_id, clone_id=clone.id)
        return clone

    def _audit_clone_source(
        self,
        source_id: UUID,
        *,
        tenant_id: str,
        clone_id: UUID,
    ) -> None:
        if not hasattr(self, "_append_job_audit"):
            return
        now = _utcnow()
        with transaction(self._pool) as conn:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (source_id, tenant_id),
            ).fetchone()
            if row is None:
                return
            self._append_job_audit(
                conn,
                job_row=row,
                event_type="cloned",
                from_status=row["status"],
                actor=None,
                detail={"clone_job_id": str(clone_id)},
                now=now,
            )
