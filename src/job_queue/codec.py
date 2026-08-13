"""JSON codecs for jobs and related models (HTTP coordinator boundary)."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.crypto import redact_payload
from job_queue.models import (
    AuditConsistencyReport,
    BulkCancelResult,
    ClaimedWebhook,
    DagEnqueueResult,
    Job,
    JobAuditEvent,
    JobProgressEvent,
    JobSideEffect,
    JobStatus,
    PayloadKeyRotation,
    PreemptResult,
    Schedule,
    SpawnChildrenResult,
    TenantMergeResult,
    TenantOffboardResult,
    TenantPayloadKey,
    TenantRestore,
    TenantSnapshot,
    TenantBreaker,
    TenantSpendCap,
    TenantStats,
    TenantUsage,
    WebhookDelivery,
    WebhookEndpoint,
)


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _uuid(value: UUID | None) -> str | None:
    return str(value) if value is not None else None


def job_to_dict(
    job: Job,
    *,
    include_lease_token: bool = False,
    include_payload: bool = False,
) -> dict[str, Any]:
    """Serialize a job for HTTP.

    ``lease_token`` is a fencing secret: anyone who holds it can ack/nack/
    heartbeat as the worker. Default is **redacted** (``null``) so list/get
    cannot hijack in-flight leases. Pass ``include_lease_token=True`` only on
    lease (and similar) responses that mint a token for the caller.

    ``payload`` is ciphertext at rest. Default is a redacted placeholder so
    inspect/list cannot read tenant secrets. Pass ``include_payload=True``
    only on lease (workers) and enqueue (caller already knows the body).
    """
    return {
        "id": str(job.id),
        "tenant_id": job.tenant_id,
        "queue_name": job.queue_name,
        "payload": job.payload if include_payload else redact_payload(job.payload),
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
        "parent_job_id": _uuid(job.parent_job_id),
        "waiting_on": [str(x) for x in job.waiting_on],
        "labels": list(job.labels),
        "required_skills": list(job.required_skills),
        "leased_by": job.leased_by,
        "lease_token": (_uuid(job.lease_token) if include_lease_token else None),
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
        "await_children_started_at": _dt(job.await_children_started_at),
        "await_children_deadline_at": _dt(job.await_children_deadline_at),
        "await_children_outcome": job.await_children_outcome,
        "await_children_resolved_at": _dt(job.await_children_resolved_at),
        "last_error": job.last_error,
        "partition_id": int(job.partition_id),
        "expires_at": _dt(job.expires_at),
        "expiry_action": job.expiry_action,
        "failed_workers": list(job.failed_workers),
        "max_worker_hops": job.max_worker_hops,
        "quarantine_action": job.quarantine_action,
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
    waiting = data.get("waiting_on") or []
    labels = data.get("labels") or []
    required_skills = data.get("required_skills") or []
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
        available_at=parse_dt("available_at")
        or datetime.fromisoformat(data["created_at"]),
        dedupe_key=data.get("dedupe_key"),
        schedule_id=parse_uuid("schedule_id"),
        depends_on=tuple(UUID(str(x)) for x in depends),
        parent_job_id=parse_uuid("parent_job_id"),
        waiting_on=tuple(UUID(str(x)) for x in waiting),
        labels=tuple(str(x) for x in labels),
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
        preemptible=bool(data["preemptible"])
        if data.get("preemptible") is not None
        else True,
        preempt_count=int(data.get("preempt_count") or 0),
        last_preempted_at=parse_dt("last_preempted_at"),
        approval_requested_at=parse_dt("approval_requested_at"),
        approval_deadline_at=parse_dt("approval_deadline_at"),
        approval_message=data.get("approval_message"),
        approval_outcome=data.get("approval_outcome"),
        approval_resolved_at=parse_dt("approval_resolved_at"),
        await_children_started_at=parse_dt("await_children_started_at"),
        await_children_deadline_at=parse_dt("await_children_deadline_at"),
        await_children_outcome=data.get("await_children_outcome"),
        await_children_resolved_at=parse_dt("await_children_resolved_at"),
        last_error=data.get("last_error"),
        partition_id=int(data.get("partition_id") or 0),
        required_skills=tuple(str(x) for x in required_skills),
        expires_at=parse_dt("expires_at"),
        expiry_action=str(data.get("expiry_action") or "dead"),
        failed_workers=tuple(str(x) for x in (data.get("failed_workers") or [])),
        max_worker_hops=(
            None
            if data.get("max_worker_hops") is None
            else int(data["max_worker_hops"])
        ),
        quarantine_action=str(data.get("quarantine_action") or "dead"),
    )


def dag_enqueue_result_to_dict(result: DagEnqueueResult) -> dict[str, Any]:
    return {
        "batch_key": result.batch_key,
        "batch_id": str(result.batch_id),
        "created": result.created,
        "refs": list(result.refs),
        "jobs": [job_to_dict(j, include_payload=True) for j in result.jobs],
    }


def spawn_children_result_to_dict(result: SpawnChildrenResult) -> dict[str, Any]:
    return {
        "parent": job_to_dict(result.parent),
        "children": [job_to_dict(c) for c in result.children],
    }


def bulk_cancel_result_to_dict(result: BulkCancelResult) -> dict[str, Any]:
    return {
        "tenant_id": result.tenant_id,
        "cancelled": result.cancelled,
        "jobs": [job_to_dict(j) for j in result.jobs],
        "count": result.cancelled,
        "complete": result.complete,
        "remaining_estimate": result.remaining_estimate,
        "batches": result.batches,
    }


def job_progress_event_to_dict(ev: JobProgressEvent) -> dict[str, Any]:
    return {
        "event": ev.event,
        "tenant_id": ev.tenant_id,
        "job_id": str(ev.job_id),
        "status": ev.status,
        "from_status": ev.from_status,
        "attempts": ev.attempts,
        "labels": list(ev.labels),
        "checkpoint": ev.checkpoint,
        "result": ev.result,
        "last_error": ev.last_error,
        "actor": ev.actor,
        "ts": _dt(ev.ts),
        "snapshot": ev.snapshot,
    }


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
    """Serialize a side-effect row for HTTP.

    ``lease_token`` is always redacted: it is a fencing secret stored for
    audit/idempotency correlation server-side, never needed by readers of
    get/list/begin/complete responses (callers already hold the live lease).
    """
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
        "lease_token": None,
        "created_at": _dt(eff.created_at),
        "completed_at": _dt(eff.completed_at),
    }


def schedule_to_dict(
    sched: Schedule,
    *,
    include_payload: bool = False,
) -> dict[str, Any]:
    return {
        "id": str(sched.id),
        "tenant_id": sched.tenant_id,
        "name": sched.name,
        "queue_name": sched.queue_name,
        "payload": (
            sched.payload if include_payload else redact_payload(sched.payload)
        ),
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
        "labels": list(sched.labels),
        "required_skills": list(sched.required_skills),
    }


def webhook_endpoint_to_dict(
    ep: WebhookEndpoint,
    *,
    include_secret: bool = False,
) -> dict[str, Any]:
    """Serialize a webhook endpoint for HTTP.

    Plaintext ``secret`` is **never** included by default (list / status
    responses). Pass ``include_secret=True`` only for create/rotate responses
    where the caller just supplied the secret and needs the echo.
    """
    out: dict[str, Any] = {
        "id": str(ep.id),
        "tenant_id": ep.tenant_id,
        "url": ep.url,
        "events": list(ep.events),
        "enabled": ep.enabled,
        "max_attempts": ep.max_attempts,
        "backoff_base_seconds": ep.backoff_base_seconds,
        "backoff_max_seconds": ep.backoff_max_seconds,
        "created_at": _dt(ep.created_at),
        "updated_at": _dt(ep.updated_at),
        "has_secret": bool(ep.secret),
    }
    if include_secret:
        out["secret"] = ep.secret
    else:
        # Explicit null so clients never treat a missing key as "unknown".
        out["secret"] = None
    return out


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
        "awaiting_skills": int(getattr(stats, "awaiting_skills", 0) or 0),
        "breaker_open": bool(getattr(stats, "breaker_open", False)),
    }


def tenant_breaker_to_dict(breaker: TenantBreaker) -> dict[str, Any]:
    return {
        "tenant_id": breaker.tenant_id,
        "failure_threshold": breaker.failure_threshold,
        "window_seconds": breaker.window_seconds,
        "cooldown_seconds": breaker.cooldown_seconds,
        "failures": breaker.failures,
        "window_start": _dt(breaker.window_start),
        "opened_at": _dt(breaker.opened_at),
        "open": breaker.open,
        "reset_at": _dt(breaker.reset_at),
    }


def tenant_spend_cap_to_dict(cap: TenantSpendCap) -> dict[str, Any]:
    return {
        "tenant_id": cap.tenant_id,
        "window_seconds": cap.window_seconds,
        "window_start": _dt(cap.window_start),
        "window_end": _dt(cap.window_end),
        "max_enqueues": cap.max_enqueues,
        "enqueues": cap.enqueues,
        "max_runtime_ms": cap.max_runtime_ms,
        "runtime_ms": cap.runtime_ms,
        "enqueue_remaining": cap.enqueue_remaining,
        "runtime_remaining_ms": cap.runtime_remaining_ms,
        "exhausted": cap.exhausted,
        "exhausted_kinds": list(cap.exhausted_kinds),
    }


def tenant_usage_to_dict(usage: TenantUsage) -> dict[str, Any]:
    return {
        "tenant_id": usage.tenant_id,
        "period_start": _dt(usage.period_start),
        "period_end": _dt(usage.period_end),
        "enqueues": usage.enqueues,
        "leases": usage.leases,
        "acks": usage.acks,
        "nacks": usage.nacks,
        "fails": usage.fails,
        "dead_letters": usage.dead_letters,
        "cancels": usage.cancels,
        "releases": usage.releases,
        "preemptions": usage.preemptions,
        "schedule_fires": usage.schedule_fires,
        "webhook_successes": usage.webhook_successes,
        "webhook_failures": usage.webhook_failures,
        "webhook_deliveries": usage.webhook_deliveries,
        "spawn_children": usage.spawn_children,
        "runtime_ms": usage.runtime_ms,
        "runtime_seconds": usage.runtime_seconds,
        "wait_ms": usage.wait_ms,
    }


def job_retention_to_dict(settings: Any) -> dict[str, Any]:
    return {
        "completed_retention_seconds": settings.completed_retention_seconds,
        "dead_retention_seconds": settings.dead_retention_seconds,
        "completed_inherits": bool(getattr(settings, "completed_inherits", False)),
        "dead_inherits": bool(getattr(settings, "dead_inherits", False)),
        "metrics_retention_seconds": getattr(
            settings, "metrics_retention_seconds", None
        ),
    }


def retention_purge_to_dict(result: Any) -> dict[str, Any]:
    return {
        "completed_deleted": result.completed_deleted,
        "dead_deleted": result.dead_deleted,
        "side_effects_deleted": result.side_effects_deleted,
        "metric_buckets_deleted": int(
            getattr(result, "metric_buckets_deleted", 0) or 0
        ),
        "metric_snapshots_deleted": int(
            getattr(result, "metric_snapshots_deleted", 0) or 0
        ),
        "metrics_deleted": int(getattr(result, "metrics_deleted", 0) or 0),
        "total_deleted": result.total_deleted,
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


def tenant_snapshot_to_dict(snap: TenantSnapshot) -> dict[str, Any]:
    return {
        "id": str(snap.id),
        "source_tenant_id": snap.source_tenant_id,
        "status": snap.status,
        "counts": dict(snap.counts),
        "created_at": _dt(snap.created_at),
        "completed_at": _dt(snap.completed_at),
        "error": snap.error,
        "format_version": snap.format_version,
    }


def tenant_restore_to_dict(rst: TenantRestore) -> dict[str, Any]:
    return {
        "id": str(rst.id),
        "snapshot_id": str(rst.snapshot_id),
        "target_tenant_id": rst.target_tenant_id,
        "source_tenant_id": rst.source_tenant_id,
        "status": rst.status,
        "kind_index": rst.kind_index,
        "applied": dict(rst.applied),
        "created_at": _dt(rst.created_at),
        "updated_at": _dt(rst.updated_at),
        "completed_at": _dt(rst.completed_at),
        "error": rst.error,
    }


def tenant_payload_key_to_dict(key: TenantPayloadKey) -> dict[str, Any]:
    return {
        "tenant_id": key.tenant_id,
        "key_id": str(key.key_id),
        "kek_id": key.kek_id,
        "status": key.status,
        "created_at": _dt(key.created_at),
        "retired_at": _dt(key.retired_at),
    }


def payload_key_rotation_to_dict(rot: PayloadKeyRotation) -> dict[str, Any]:
    return {
        "id": str(rot.id),
        "tenant_id": rot.tenant_id,
        "from_key_id": _uuid(rot.from_key_id),
        "to_key_id": str(rot.to_key_id),
        "status": rot.status,
        "jobs_rewrapped": rot.jobs_rewrapped,
        "schedules_rewrapped": rot.schedules_rewrapped,
        "created_at": _dt(rot.created_at),
        "updated_at": _dt(rot.updated_at),
        "completed_at": _dt(rot.completed_at),
    }


def webhook_delivery_to_dict(
    d: WebhookDelivery,
    *,
    include_signing_secret: bool = False,
    include_payload: bool = False,
) -> dict[str, Any]:
    """Serialize a webhook delivery.

    ``signing_secret`` is redacted by default. Claim responses expose the
    secret once at the top level of :func:`claimed_webhook_to_dict` for the
    agent that must sign the POST — not nested on every delivery echo.

    Delivery ``payload`` (the POST body) is redacted by default so list/get
    cannot dump tenant job secrets. Pass ``include_payload=True`` only on
    claim (the agent that will POST).
    """
    out: dict[str, Any] = {
        "id": str(d.id),
        "endpoint_id": str(d.endpoint_id),
        "tenant_id": d.tenant_id,
        "job_id": str(d.job_id) if d.job_id is not None else None,
        "event": d.event,
        "payload": d.payload if include_payload else redact_payload(d.payload),
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
        "has_signing_secret": bool(d.signing_secret),
        "signing_secret": (d.signing_secret if include_signing_secret else None),
    }
    return out


def claimed_webhook_to_dict(c: ClaimedWebhook) -> dict[str, Any]:
    # Nested delivery is redacted; top-level secret is the claim handoff
    # for the agent that will POST and sign.
    return {
        "delivery": webhook_delivery_to_dict(
            c.delivery, include_signing_secret=False, include_payload=True
        ),
        "url": c.url,
        "secret": c.secret,
        "endpoint_enabled": c.endpoint_enabled,
    }
