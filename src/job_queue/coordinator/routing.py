"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _configured_api_token,
)

import os
from typing import Any

from job_queue.queue import (
    JobQueue,
)


class RoutingMixin:
    def __init__(
        self,
        queue: JobQueue,
        *,
        api_token: str | None = None,
    ) -> None:
        self.queue = queue
        # Explicit token wins; else env; None means auth disabled.
        if api_token is not None:
            self.api_token = api_token or None
        else:
            self.api_token = _configured_api_token()

    def handle(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        query: dict[str, list[str]],
    ) -> tuple[int, dict[str, Any]]:
        body = body or {}
        if method == "GET" and path == "/health":
            return 200, {
                "ok": True,
                "role": "coordinator",
                "node": os.environ.get("COORDINATOR_NODE", "default"),
            }

        if method == "POST" and path == "/v1/jobs":
            return self._enqueue(body)
        if method == "POST" and path == "/v1/jobs/dag":
            return self._enqueue_dag(body)
        if method == "GET" and path == "/v1/jobs":
            return self._list_jobs(query)
        if method == "POST" and path == "/v1/jobs/cancel-matching":
            return self._cancel_matching(body)
        if method == "POST" and path == "/v1/jobs/freeze-matching":
            return self._freeze_matching(body)
        if method == "POST" and path == "/v1/jobs/unfreeze-matching":
            return self._unfreeze_matching(body)
        if method == "POST" and path == "/v1/lease":
            return self._lease(body)
        if method == "POST" and path == "/v1/deadlines/process":
            return self._process_deadlines(body)
        if method == "POST" and path == "/v1/jobs/process-expired":
            return self._process_expired_jobs(body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/heartbeat")
        ):
            return self._job_action(path, body, "heartbeat")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/ack"):
            return self._job_action(path, body, "ack")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/nack"):
            return self._job_action(path, body, "nack")
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/release")
        ):
            return self._job_action(path, body, "release")
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/checkpoint")
        ):
            return self._job_action(path, body, "checkpoint")
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/side-effects/begin")
        ):
            return self._begin_side_effect(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/side-effects")
        ):
            return self._complete_side_effect(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/jobs/")
            and "/side-effects/" in path
        ):
            return self._get_side_effect(path, query)
        if (
            method == "GET"
            and path.startswith("/v1/jobs/")
            and path.endswith("/side-effects")
        ):
            return self._list_side_effects(path, query)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/request-approval")
        ):
            return self._job_action(path, body, "request_approval")
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/spawn-children")
        ):
            return self._spawn_children(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/jobs/")
            and path.endswith("/children")
        ):
            return self._list_children(path, query)
        if method == "POST" and path == "/v1/await-children/process-timeouts":
            return self._process_await_children_timeouts(body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/approve")
        ):
            return self._approve_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/reject")
        ):
            return self._reject_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/freeze/dead")
        ):
            return self._dead_letter_frozen(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/unfreeze")
        ):
            return self._unfreeze_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/freeze")
        ):
            return self._freeze_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/cancel")
        ):
            return self._cancel_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/priority")
        ):
            return self._set_job_priority(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/worker-affinity")
        ):
            return self._set_job_worker_affinity(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/clone")
        ):
            return self._clone_job(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/queue")
        ):
            return self._move_job_queue(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/tenant")
        ):
            return self._move_job_tenant(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/postpone")
        ):
            return self._postpone_job(path, body)
        if method == "POST" and path == "/v1/approvals/process-timeouts":
            return self._process_approval_timeouts(body)
        if method == "GET" and path == "/v1/approvals":
            return self._list_approvals(query)
        if (
            method == "GET"
            and path.startswith("/v1/jobs/")
            and path.endswith("/audit/verify")
        ):
            return self._job_audit_verify(path, query)
        if method == "GET" and path.startswith("/v1/jobs/") and path.endswith("/audit"):
            return self._job_audit(path, query)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/audit/verify")
        ):
            return self._tenant_audit_verify(path, query)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/audit")
        ):
            return self._tenant_audit(path, query)
        if (
            method == "POST"
            and path.startswith("/v1/jobs/")
            and path.endswith("/rebuild")
        ):
            return self._rebuild_job(path, body)
        if method == "POST" and path == "/v1/audit/rebuild":
            return self._rebuild_jobs_batch(body)
        if method == "GET" and path == "/v1/audit/orphans":
            return self._list_audit_orphans(query)
        if method == "GET" and path.startswith("/v1/jobs/"):
            return self._get_job(path, query)

        if method == "POST" and path == "/v1/schedules/process-due":
            return self._process_due(body)
        if method == "POST" and path == "/v1/webhooks/claim":
            return self._claim_webhooks(body)
        if (
            method == "POST"
            and path.startswith("/v1/webhooks/")
            and path.endswith("/finalize")
        ):
            return self._finalize_webhook(path, body)
        # Legacy in-process process path intentionally removed from coordinator
        # so agents always claim+POST+finalize themselves.

        if method == "POST" and path == "/v1/schedules":
            return self._create_schedule(body)
        if method == "GET" and path == "/v1/schedules":
            return self._list_schedules(query)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and "/blackouts/" in path
            and path.endswith("/delete")
        ):
            return self._delete_schedule_blackout(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/blackouts")
        ):
            return self._create_schedule_blackout(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/schedules/")
            and path.endswith("/blackouts")
        ):
            return self._list_schedule_blackouts(path, query)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and "/blackouts/" in path
        ):
            return self._update_schedule_blackout(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/update")
        ):
            return self._update_schedule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/fire")
        ):
            return self._fire_schedule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/misfire")
        ):
            return self._set_schedule_misfire(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/pause")
        ):
            return self._pause_schedule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/resume")
        ):
            return self._resume_schedule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/move")
        ):
            return self._move_schedule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/schedules/")
            and path.endswith("/delete")
        ):
            return self._delete_schedule(path, body)
        if method == "GET" and path.startswith("/v1/schedules/"):
            return self._get_schedule(path, query)

        if method == "POST" and path == "/v1/webhooks":
            return self._register_webhook(body)
        if method == "GET" and path == "/v1/webhooks":
            return self._list_webhooks(query)
        if (
            method == "POST"
            and path.startswith("/v1/webhooks/")
            and path.endswith("/rotate-secret")
        ):
            return self._rotate_webhook_secret(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/webhooks/")
            and path.endswith("/set-enabled")
        ):
            return self._set_webhook_enabled(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/webhooks/")
            and path.endswith("/delete")
        ):
            return self._delete_webhook(path, body)

        if method == "GET" and path == "/v1/dead-letters":
            return self._list_dead_letters(query)
        if method == "POST" and path == "/v1/dead-letters/replay-graph/plan":
            return self._plan_replay_dead_letter_graph(body)
        if method == "POST" and path == "/v1/dead-letters/replay-graph":
            return self._replay_dead_letter_graph(body)
        if method == "POST" and path == "/v1/dead-letters/replay":
            return self._replay_dead_letters(body)
        if (
            method == "POST"
            and path.startswith("/v1/dead-letters/")
            and path.endswith("/replay")
        ):
            return self._replay_dead_letter(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and "/blackouts/" in path
            and path.endswith("/delete")
        ):
            return self._delete_tenant_blackout(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/blackouts")
        ):
            return self._create_tenant_blackout(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/blackouts")
        ):
            return self._list_tenant_blackouts(path, query)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and "/blackouts/" in path
        ):
            return self._update_tenant_blackout(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/pause")
        ):
            return self._pause_tenant(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/resume")
        ):
            return self._resume_tenant(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/drain-queue")
        ):
            return self._drain_tenant_queue(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/undrain-queue")
        ):
            return self._undrain_tenant_queue(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/drain-queues")
        ):
            return self._list_draining_queues(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/concurrency")
        ):
            return self._set_concurrency(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/pending-limit")
        ):
            return self._set_pending_limit(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/rate-limits")
        ):
            return self._set_rate_limits(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/rate-limits")
        ):
            return self._get_rate_limits(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/quarantine")
        ):
            return self._set_quarantine(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/quarantine")
        ):
            return self._get_quarantine(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/spend-cap")
        ):
            return self._set_spend_cap(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/spend-cap")
        ):
            return self._get_spend_cap(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/breaker/reset")
        ):
            return self._reset_breaker(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/breaker")
        ):
            return self._set_breaker(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/breaker")
        ):
            return self._get_breaker(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/lease-weight")
        ):
            return self._set_lease_weight(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/lease-weight")
        ):
            return self._get_lease_weight(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/worker-affinity")
        ):
            return self._set_worker_affinity(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/worker-affinity")
        ):
            return self._get_worker_affinity(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/retention")
        ):
            return self._set_tenant_retention(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/job-ttl")
        ):
            return self._set_tenant_job_ttl(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/job-ttl")
        ):
            return self._get_tenant_job_ttl(path)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/retention")
        ):
            return self._get_tenant_retention(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/offboard")
        ):
            return self._offboard_tenant(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/merge")
        ):
            return self._merge_tenant(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/split/plan")
        ):
            return self._plan_tenant_split(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/split")
        ):
            return self._split_tenant(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/coalesce")
        ):
            return self._coalesce_jobs(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/export")
        ):
            return self._export_tenant(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/restore/advance")
        ):
            return self._advance_tenant_restore(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/restore")
        ):
            return self._restore_tenant(path, body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/restore")
        ):
            return self._get_tenant_restore(path)
        if method == "GET" and path.startswith("/v1/snapshots/"):
            return self._get_snapshot(path)
        if method == "POST" and path == "/v1/admin/restores/advance":
            return self._advance_all_restores(body)
        if (
            method == "GET"
            and path.startswith("/v1/tenants/")
            and path.endswith("/payload-keys")
        ):
            return self._list_tenant_payload_keys(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/payload-keys/rotate")
        ):
            return self._rotate_tenant_payload_key(path)
        if (
            method == "POST"
            and path.startswith("/v1/tenants/")
            and path.endswith("/payload-keys/advance")
        ):
            return self._advance_tenant_payload_key(path, body)
        if method == "POST" and path == "/v1/admin/payload-keys/advance":
            return self._advance_all_payload_keys(body)

        if method == "POST" and path == "/v1/admin/retention":
            return self._set_retention(body)
        if method == "GET" and path == "/v1/admin/retention":
            return self._get_retention()
        if method == "POST" and path == "/v1/admin/purge-expired-jobs":
            return self._purge_expired_jobs(body)
        if method == "GET" and path == "/v1/admin/backlog":
            return self._backlog()
        if method == "GET" and path == "/v1/admin/completed-by-schedule":
            return self._completed_by_schedule()
        if method == "GET" and path == "/v1/admin/stats":
            return self._list_stats(query)
        if method == "GET" and path == "/v1/admin/webhook-stats":
            return self._webhook_stats()
        if method == "GET" and path == "/v1/admin/fairness-check":
            return self._fairness_check()
        if method == "GET" and path == "/v1/admin/completion-order":
            return self._completion_order(query)
        if method == "GET" and path == "/v1/admin/priority-stats":
            return self._priority_stats(query)
        if method == "GET" and path == "/v1/admin/tenant-residue":
            return self._tenant_residue(query)

        # Operational monitoring (per-tenant + overall, multi-format).
        if method == "GET" and path == "/v1/metrics/snapshot":
            return self._metrics_snapshot(query)
        if method == "GET" and path == "/v1/metrics/history":
            return self._metrics_history(query)
        if method == "GET" and path == "/v1/metrics/noisy":
            return self._metrics_noisy(query)
        if method == "POST" and path == "/v1/metrics/sample":
            return self._metrics_sample(body)
        if method == "POST" and path == "/v1/metrics/purge":
            return self._metrics_purge(body)
        if method == "GET" and path == "/v1/metrics/retention":
            return self._get_metrics_retention()
        if method == "POST" and path == "/v1/metrics/retention":
            return self._set_metrics_retention(body)

        # Per-tenant billing usage (durable, same-txn increments).
        if method == "GET" and path == "/v1/usage":
            return self._usage_get(query)
        if method == "GET" and path == "/v1/usage/buckets":
            return self._usage_buckets(query)

        # Worker partition ownership / rebalance.
        if method == "POST" and path == "/v1/workers/register":
            return self._register_worker(body)
        if method == "POST" and path == "/v1/workers/heartbeat":
            return self._worker_heartbeat(body)
        if method == "POST" and path == "/v1/workers/deregister":
            return self._deregister_worker(body)
        if method == "GET" and path == "/v1/workers/partitions":
            return self._list_worker_partitions(query)
        if method == "GET" and path == "/v1/partitions":
            return self._list_partition_assignments()
        if method == "POST" and path == "/v1/workers/process-timeouts":
            return self._process_worker_timeouts(body)
        if method == "POST" and path == "/v1/partitions/count":
            return self._set_partition_count(body)

        # Multi-step saga workflows.
        if method == "POST" and path == "/v1/workflows":
            return self._start_workflow(body)
        if method == "POST" and path == "/v1/workflows/process-compensation":
            return self._process_workflow_compensation(body)
        if (
            method == "POST"
            and path.startswith("/v1/workflows/")
            and path.rstrip("/").endswith("/cancel")
        ):
            return self._cancel_workflow(path, body)
        if method == "GET" and path.startswith("/v1/workflows/"):
            if path.rstrip("/").endswith("/steps"):
                return self._list_workflow_steps(path, query)
            return self._get_workflow(path, query)

        # Metric threshold alerts + history.
        if method == "POST" and path == "/v1/alerts/rules":
            return self._create_alert_rule(body)
        if method == "GET" and path == "/v1/alerts/rules":
            return self._list_alert_rules(query)
        if (
            method == "POST"
            and path.startswith("/v1/alerts/rules/")
            and path.endswith("/enabled")
        ):
            return self._set_alert_rule_enabled(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/alerts/rules/")
            and path.endswith("/mute")
        ):
            return self._mute_alert_rule(path, body)
        if (
            method == "POST"
            and path.startswith("/v1/alerts/rules/")
            and path.endswith("/unmute")
        ):
            return self._unmute_alert_rule(path)
        if method == "DELETE" and path.startswith("/v1/alerts/rules/"):
            return self._delete_alert_rule(path)
        if method == "GET" and path == "/v1/alerts/events":
            return self._list_alert_events(query)
        if (
            method == "POST"
            and path.startswith("/v1/alerts/events/")
            and path.endswith("/resolve")
        ):
            return self._resolve_alert_event(path, body)
        if method == "POST" and path == "/v1/alerts/evaluate":
            return self._evaluate_alerts(body)

        return 404, {"error": f"unknown route {method} {path}"}
