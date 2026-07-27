"""JSON codecs for jobs and related models (HTTP coordinator boundary)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.models import (
    AuditConsistencyReport,
    ClaimedWebhook,
    Job,
    JobAuditEvent,
    JobSideEffect,
    JobStatus,
    PreemptResult,
    Schedule,
    TenantMergeResult,
    TenantOffboardResult,
    TenantStats,
    WebhookDelivery,
    WebhookEndpoint,
)


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
        "max_runtime_seconds": job.max_runtime_seconds,
        "retry_backoff_base_seconds": job.retry_backoff_base_seconds,
        "retry_backoff_max_seconds": job.retry_backoff_max_seconds,
        "available_at": _dt(job.available_at),
        "dedupe_key": job.dedupe_key,
        "schedule_id": _uuid(job.schedule_id),
        "depends_on": [str(x) for x in job.depends_on],
        "leased_by": job.leased_by,
        "lease_token": _uuid(job.lease_token),
        "lease_tenant_id": job.lease_tenant_id,
        "lease_expires_at": _dt(job.lease_expires_at),
        "lease_started_at": _dt(job.lease_started_at),
        "created_at": _dt(job.created_at),
        "updated_at": _dt(job.updated_at),
        "completed_at": _dt(job.completed_at),
        "result": job.result,
        "checkpoint": job.checkpoint,
        "checkpointed_at": _dt(job.checkpointed_at),
        "deadline_at": _dt(job.deadline_at),
        "estimated_duration_seconds": job.estimated_duration_seconds,
        "preemptible": job.preemptible,
        "preempt_count": job.preempt_count,
        "last_preempted_at": _dt(job.last_preempted_at),
        "approval_requested_at": _dt(job.approval_requested_at),
        "approval_deadline_at": _dt(job.approval_deadline_at),
        "approval_message": job.approval_message,
        "approval_outcome": job.approval_outcome,
        "approval_resolved_at": _dt(job.approval_resolved_at),
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
        max_runtime_seconds=int(data.get("max_runtime_seconds") or 3600),
        retry_backoff_base_seconds=int(data.get("retry_backoff_base_seconds") or 1),
        retry_backoff_max_seconds=int(data.get("retry_backoff_max_seconds") or 300),
        available_at=parse_dt("available_at") or datetime.fromisoformat(data["created_at"]),
        dedupe_key=data.get("dedupe_key"),
        schedule_id=parse_uuid("schedule_id"),
        depends_on=tuple(UUID(str(x)) for x in depends),
        leased_by=data.get("leased_by"),
        lease_token=parse_uuid("lease_token"),
        lease_tenant_id=data.get("lease_tenant_id"),
        lease_expires_at=parse_dt("lease_expires_at"),
        lease_started_at=parse_dt("lease_started_at"),
        created_at=parse_dt("created_at") or datetime.now().astimezone(),
        updated_at=parse_dt("updated_at") or datetime.now().astimezone(),
        completed_at=parse_dt("completed_at"),
        result=data.get("result"),
        checkpoint=data.get("checkpoint"),
        checkpointed_at=parse_dt("checkpointed_at"),
        deadline_at=parse_dt("deadline_at"),
        estimated_duration_seconds=(
            None
            if data.get("estimated_duration_seconds") is None
            else int(data["estimated_duration_seconds"])
        ),
        preemptible=bool(data["preemptible"]) if data.get("preemptible") is not None else True,
        preempt_count=int(data.get("preempt_count") or 0),
        last_preempted_at=parse_dt("last_preempted_at"),
        approval_requested_at=parse_dt("approval_requested_at"),
        approval_deadline_at=parse_dt("approval_deadline_at"),
        approval_message=data.get("approval_message"),
        approval_outcome=data.get("approval_outcome"),
        approval_resolved_at=parse_dt("approval_resolved_at"),
        last_error=data.get("last_error"),
    )


def preempt_result_to_dict(result: PreemptResult) -> dict[str, Any]:
    return {
        "tenant_id": result.tenant_id,
        "queue_name": result.queue_name,
        "urgent_job_id": str(result.urgent_job_id),
        "victim_job_id": str(result.victim_job_id),
        "urgent_slack_seconds": result.urgent_slack_seconds,
        "victim_slack_seconds": (
            result.victim_slack_seconds
            if result.victim_slack_seconds != float("inf")
            else None
        ),
    }


def job_audit_event_to_dict(ev: JobAuditEvent) -> dict[str, Any]:
    return {
        "event_id": str(ev.event_id),
        "tenant_id": ev.tenant_id,
        "job_id": str(ev.job_id),
        "seq": ev.seq,
        "event_type": ev.event_type,
        "from_status": ev.from_status,
        "to_status": ev.to_status,
        "actor": ev.actor,
        "detail": ev.detail,
        "created_at": _dt(ev.created_at),
    }


def audit_consistency_report_to_dict(
    report: AuditConsistencyReport,
) -> dict[str, Any]:
    return {
        "job_id": str(report.job_id),
        "tenant_id": report.tenant_id,
        "ok": report.ok,
        "job_status": report.job_status,
        "audit_final_status": report.audit_final_status,
        "event_count": report.event_count,
        "gaps": list(report.gaps),
        "error": report.error,
    }


def job_side_effect_to_dict(eff: JobSideEffect) -> dict[str, Any]:
    return {
        "job_id": str(eff.job_id),
        "effect_key": eff.effect_key,
        "tenant_id": eff.tenant_id,
        "status": eff.status,
        "idempotency_key": eff.idempotency_key,
        "result": eff.result,
        "started_by": eff.started_by,
        "started_at": _dt(eff.started_at),
        "completed_by": eff.completed_by,
        "lease_token": _uuid(eff.lease_token),
        "created_at": _dt(eff.created_at),
        "completed_at": _dt(eff.completed_at),
    }


def schedule_to_dict(sched: Schedule) -> dict[str, Any]:
    return {
        "id": str(sched.id),
        "tenant_id": sched.tenant_id,
        "name": sched.name,
        "queue_name": sched.queue_name,
        "payload": sched.payload,
        "interval_seconds": sched.interval_seconds,
        "cron_expr": sched.cron_expr,
        "timezone": sched.timezone,
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
        "max_pending_jobs": stats.max_pending_jobs,
        "enqueue_rate_per_sec": stats.enqueue_rate_per_sec,
        "enqueue_burst": stats.enqueue_burst,
        "lease_rate_per_sec": stats.lease_rate_per_sec,
        "lease_burst": stats.lease_burst,
        "pending": stats.pending,
        "leased": stats.leased,
        "completed": stats.completed,
        "failed": stats.failed,
        "dead": stats.dead,
        "cancelled": stats.cancelled,
        "total": stats.total,
        "claimable": stats.claimable,
    }


def tenant_offboard_result_to_dict(result: TenantOffboardResult) -> dict[str, Any]:
    """Serialize :class:`~job_queue.models.TenantOffboardResult`."""
    return {
        "tenant_id": result.tenant_id,
        "pending_cancelled": result.pending_cancelled,
        "schedules_deleted": result.schedules_deleted,
        "webhooks_deleted": result.webhooks_deleted,
        "jobs_purged": result.jobs_purged,
        "leased_remaining": result.leased_remaining,
        "webhooks_delivering_remaining": result.webhooks_delivering_remaining,
        "offboarding": result.offboarding,
        "empty": result.empty,
    }


def tenant_merge_result_to_dict(result: TenantMergeResult) -> dict[str, Any]:
    """Serialize :class:`~job_queue.models.TenantMergeResult`."""
    return {
        "source_tenant_id": result.source_tenant_id,
        "target_tenant_id": result.target_tenant_id,
        "jobs_moved": result.jobs_moved,
        "jobs_leased_moved": result.jobs_leased_moved,
        "schedules_moved": result.schedules_moved,
        "webhook_endpoints_moved": result.webhook_endpoints_moved,
        "webhook_deliveries_moved": result.webhook_deliveries_moved,
        "dedupe_keys_cleared": result.dedupe_keys_cleared,
        "schedule_names_renamed": result.schedule_names_renamed,
        "source_empty": result.source_empty,
    }


def webhook_delivery_to_dict(d: WebhookDelivery) -> dict[str, Any]:
    return {
        "id": str(d.id),
        "endpoint_id": str(d.endpoint_id),
        "tenant_id": d.tenant_id,
        "job_id": str(d.job_id),
        "event": d.event,
        "payload": d.payload,
        "status": d.status,
        "attempts": d.attempts,
        "max_attempts": d.max_attempts,
        "backoff_base_seconds": d.backoff_base_seconds,
        "backoff_max_seconds": d.backoff_max_seconds,
        "next_attempt_at": _dt(d.next_attempt_at),
        "delivering_until": _dt(d.delivering_until),
        "last_error": d.last_error,
        "last_status_code": d.last_status_code,
        "created_at": _dt(d.created_at),
        "updated_at": _dt(d.updated_at),
        "completed_at": _dt(d.completed_at),
    }


def claimed_webhook_to_dict(c: ClaimedWebhook) -> dict[str, Any]:
    return {
        "delivery": webhook_delivery_to_dict(c.delivery),
        "url": c.url,
        "secret": c.secret,
        "endpoint_enabled": c.endpoint_enabled,
    }
