"""JSON codecs for jobs and related models (HTTP coordinator boundary)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.models import Job, JobStatus, Schedule, TenantStats, WebhookEndpoint


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _uuid(value: UUID | None) -> str | None:
    return str(value) if value is not None else None


def job_to_dict(job: Job) -> dict[str, Any]:
    return {
        "id": str(job.id),
        "tenant_id": job.tenant_id,
        "queue_name": job.queue_name,
        "payload": job.payload,
        "status": job.status.value,
        "priority": job.priority,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "visibility_timeout_seconds": job.visibility_timeout_seconds,
        "retry_backoff_base_seconds": job.retry_backoff_base_seconds,
        "retry_backoff_max_seconds": job.retry_backoff_max_seconds,
        "available_at": _dt(job.available_at),
        "dedupe_key": job.dedupe_key,
        "schedule_id": _uuid(job.schedule_id),
        "depends_on": [str(x) for x in job.depends_on],
        "leased_by": job.leased_by,
        "lease_token": _uuid(job.lease_token),
        "lease_expires_at": _dt(job.lease_expires_at),
        "created_at": _dt(job.created_at),
        "updated_at": _dt(job.updated_at),
        "completed_at": _dt(job.completed_at),
        "result": job.result,
        "last_error": job.last_error,
    }


def job_from_dict(data: dict[str, Any]) -> Job:
    def parse_dt(key: str) -> datetime | None:
        raw = data.get(key)
        if raw is None:
            return None
        return datetime.fromisoformat(raw)

    def parse_uuid(key: str) -> UUID | None:
        raw = data.get(key)
        if raw is None:
            return None
        return UUID(str(raw))

    depends = data.get("depends_on") or []
    return Job(
        id=UUID(str(data["id"])),
        tenant_id=str(data["tenant_id"]),
        queue_name=str(data.get("queue_name") or "default"),
        payload=dict(data.get("payload") or {}),
        status=JobStatus(data["status"]),
        priority=int(data.get("priority") or 0),
        attempts=int(data.get("attempts") or 0),
        max_attempts=int(data.get("max_attempts") or 3),
        visibility_timeout_seconds=int(data.get("visibility_timeout_seconds") or 30),
        retry_backoff_base_seconds=int(data.get("retry_backoff_base_seconds") or 1),
        retry_backoff_max_seconds=int(data.get("retry_backoff_max_seconds") or 300),
        available_at=parse_dt("available_at") or datetime.fromisoformat(data["created_at"]),
        dedupe_key=data.get("dedupe_key"),
        schedule_id=parse_uuid("schedule_id"),
        depends_on=tuple(UUID(str(x)) for x in depends),
        leased_by=data.get("leased_by"),
        lease_token=parse_uuid("lease_token"),
        lease_expires_at=parse_dt("lease_expires_at"),
        created_at=parse_dt("created_at") or datetime.now().astimezone(),
        updated_at=parse_dt("updated_at") or datetime.now().astimezone(),
        completed_at=parse_dt("completed_at"),
        result=data.get("result"),
        last_error=data.get("last_error"),
    )


def schedule_to_dict(sched: Schedule) -> dict[str, Any]:
    return {
        "id": str(sched.id),
        "tenant_id": sched.tenant_id,
        "name": sched.name,
        "queue_name": sched.queue_name,
        "payload": sched.payload,
        "interval_seconds": sched.interval_seconds,
        "priority": sched.priority,
        "max_attempts": sched.max_attempts,
        "visibility_timeout_seconds": sched.visibility_timeout_seconds,
        "enabled": sched.enabled,
        "next_run_at": _dt(sched.next_run_at),
        "last_enqueued_at": _dt(sched.last_enqueued_at),
        "created_at": _dt(sched.created_at),
        "updated_at": _dt(sched.updated_at),
    }


def webhook_endpoint_to_dict(ep: WebhookEndpoint) -> dict[str, Any]:
    return {
        "id": str(ep.id),
        "tenant_id": ep.tenant_id,
        "url": ep.url,
        "secret": ep.secret,
        "events": list(ep.events),
        "enabled": ep.enabled,
        "max_attempts": ep.max_attempts,
        "backoff_base_seconds": ep.backoff_base_seconds,
        "backoff_max_seconds": ep.backoff_max_seconds,
        "created_at": _dt(ep.created_at),
        "updated_at": _dt(ep.updated_at),
    }


def tenant_stats_to_dict(stats: TenantStats) -> dict[str, Any]:
    return {
        "tenant_id": stats.tenant_id,
        "paused": stats.paused,
        "max_concurrent_leases": stats.max_concurrent_leases,
        "pending": stats.pending,
        "leased": stats.leased,
        "completed": stats.completed,
        "failed": stats.failed,
        "dead": stats.dead,
        "cancelled": stats.cancelled,
        "total": stats.total,
        "claimable": stats.claimable,
    }
