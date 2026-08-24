"""JobQueue enqueue."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from job_queue.db import transaction
from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    DependencyError,
    _as_utc,
    _normalize_labels,
    _normalize_resource_map,
    _normalize_skills,
    _utcnow,
)


class JobsEnqueueCoreMixin:
    def enqueue(
        self,
        *,
        tenant_id: str,
        payload: dict[str, Any],
        queue_name: str = "default",
        priority: int = 0,
        max_attempts: int = 3,
        visibility_timeout_seconds: int = 30,
        max_runtime_seconds: int = 3600,
        retry_backoff_base_seconds: int = 1,
        retry_backoff_max_seconds: int = 300,
        available_at: datetime | None = None,
        delay_seconds: float | int | None = None,
        dedupe_key: str | None = None,
        depends_on: UUID | Sequence[UUID] | None = None,
        job_id: UUID | None = None,
        deadline_at: datetime | None = None,
        estimated_duration_seconds: int | None = None,
        preemptible: bool = True,
        labels: Sequence[str] | str | None = None,
        required_skills: Sequence[str] | str | None = None,
        resource_cost: dict[str, int] | None = None,
        ttl_seconds: float | int | None = None,
        expires_at: datetime | None = None,
        expiry_action: str | None = None,
        max_worker_hops: int | None = None,
        quarantine_action: str | None = None,
        prefer_last_worker: bool = False,
        cloned_from_job_id: UUID | None = None,
        coalesce_key: str | None = None,
    ) -> Job:
        """Add a job to the queue (or return an existing active dedupe match).

        Parameters
        ----------
        available_at:
            Earliest time the job may be claimed. Defaults to now (immediate).
        delay_seconds:
            Convenience alternative to ``available_at``: run after this many
            seconds from now. Mutually exclusive with ``available_at``.
        dedupe_key:
            If set, at most one *active* (pending or leased) job with this key
            may exist for the tenant/queue. A second enqueue returns the
            existing job instead of creating a duplicate. Completed / dead /
            failed jobs do not block a later enqueue with the same key.
        coalesce_key:
            Optional fold group. Unlike ``dedupe_key`` this is not unique,
            so the same logical work can pile up. ``coalesce_jobs`` later
            absorbs extras into one survivor.
        depends_on:
            Parent job id or list of parent ids (same tenant). The job is not
            claimable until every parent is ``completed``. If a parent is
            already failed/dead/cancelled at enqueue time, raises
            ``DependencyError``. If a parent later fails, pending children are
            cascade-cancelled.
        max_runtime_seconds:
            Hard wall-clock limit for one lease attempt (from
            ``lease_started_at``). Heartbeats cannot extend past this; on
            exceed the job is force-released (requeued or dead-lettered).
        retry_backoff_base_seconds / retry_backoff_max_seconds:
            On nack requeue, delay is ``base * 2^(attempt-1)`` seconds, capped
            by max. After ``max_attempts`` failures the job is dead-lettered.
        deadline_at:
            Absolute deadline. When set, the job may preempt less-urgent
            work if concurrency is full and waiting would miss the deadline.
        estimated_duration_seconds:
            Hint for remaining-work / slack calculations. When omitted the
            queue uses learned EWMAs (by payload kind) or a default.
        preemptible:
            If False this job will not be chosen as a preemption victim.
        required_skills:
            Worker capabilities this job needs (AND). Empty (default) means
            any worker may claim it. A worker leases the job only when it
            advertised every listed skill; partitions, fairness, and
            concurrency still apply.
        resource_cost:
            How much of each worker resource this job occupies while
            leased (``{"cpu": 2, "memory": 1024}``). Empty means free.
            A worker claims it only when remaining capacity covers every
            listed amount.
        ttl_seconds / expires_at:
            Hard finish-by. If the job is still unfinished after this,
            ``process_expired_jobs`` (also run on lease/cleaner) terminates
            it. Pass only one. When both omitted, the tenant default TTL
            applies if configured.
        expiry_action:
            ``dead`` (default / dead-letter) or ``cancel`` when the TTL
            fires. Falls back to the tenant default, then ``dead``.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if visibility_timeout_seconds < 1:
            raise ValueError("visibility_timeout_seconds must be >= 1")
        if max_runtime_seconds < 1:
            raise ValueError("max_runtime_seconds must be >= 1")
        if retry_backoff_base_seconds < 0:
            raise ValueError("retry_backoff_base_seconds must be >= 0")
        if retry_backoff_max_seconds < 0:
            raise ValueError("retry_backoff_max_seconds must be >= 0")
        if available_at is not None and delay_seconds is not None:
            raise ValueError("pass only one of available_at or delay_seconds")
        if delay_seconds is not None and delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if dedupe_key is not None and dedupe_key == "":
            raise ValueError("dedupe_key must be non-empty when provided")
        if coalesce_key is not None and coalesce_key == "":
            raise ValueError("coalesce_key must be non-empty when provided")
        coal_key = coalesce_key.strip() if coalesce_key is not None else None
        if estimated_duration_seconds is not None and estimated_duration_seconds < 1:
            raise ValueError("estimated_duration_seconds must be >= 1 when set")

        dep_ids = self._normalize_depends_on(depends_on)
        label_list = _normalize_labels(labels)
        skill_list = _normalize_skills(required_skills)
        cost_map = _normalize_resource_map(
            resource_cost, allow_zero=False, noun="resource_cost"
        )

        now = _utcnow()
        payload_kind = self._payload_kind(payload)
        prefer_lw = bool(prefer_last_worker)
        if available_at is not None:
            run_at = _as_utc(available_at)
        elif delay_seconds is not None:
            run_at = now + timedelta(seconds=float(delay_seconds))
        else:
            run_at = now
        deadline = _as_utc(deadline_at) if deadline_at is not None else None

        jid = job_id or uuid4()
        if jid in dep_ids:
            raise DependencyError("a job cannot depend on itself")
        if cloned_from_job_id is not None and cloned_from_job_id == jid:
            raise ValueError("a job cannot be cloned from itself")

        with transaction(self._pool) as conn:
            # Serialize with merge_tenant / pending-cap for this tenant.
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
            if dep_ids:
                self._validate_dependencies(
                    conn, tenant_id=tenant_id, depends_on=dep_ids
                )
            expires, exp_action = self._resolve_job_expiry(
                conn,
                tenant_id=tenant_id,
                now=now,
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                expiry_action=expiry_action,
            )
            out = self._enqueue_pending_on_conn(
                conn,
                tenant_id=tenant_id,
                payload=payload,
                queue_name=queue_name,
                priority=priority,
                max_attempts=max_attempts,
                visibility_timeout_seconds=visibility_timeout_seconds,
                max_runtime_seconds=max_runtime_seconds,
                retry_backoff_base_seconds=retry_backoff_base_seconds,
                retry_backoff_max_seconds=retry_backoff_max_seconds,
                run_at=run_at,
                dedupe_key=dedupe_key,
                dep_ids=dep_ids,
                label_list=label_list,
                skill_list=skill_list,
                resource_cost=cost_map,
                deadline=deadline,
                estimated_duration_seconds=estimated_duration_seconds,
                preemptible=bool(preemptible),
                now=now,
                jid=jid,
                expires=expires,
                exp_action=exp_action,
                prefer_lw=prefer_lw,
                payload_kind=payload_kind,
                cloned_from_job_id=cloned_from_job_id,
                max_worker_hops=max_worker_hops,
                quarantine_action=quarantine_action,
                coalesce_key=coal_key,
            )

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
