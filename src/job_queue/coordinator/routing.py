"""Coordinator API handlers."""
from __future__ import annotations

from job_queue.coordinator.auth import (
    _API_TOKEN_ENV_KEYS,
    _BodyTooLarge,
    _MAX_REQUEST_BODY_BYTES,
    _UUID_RE,
    _configured_api_token,
    _extract_request_token,
)




import json
import logging
import os
import re
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

from job_queue.codec import (
    bulk_cancel_result_to_dict,
    claimed_webhook_to_dict,
    job_to_dict,
    job_progress_event_to_dict,
    job_retention_to_dict,
    retention_purge_to_dict,
    schedule_to_dict,
    audit_consistency_report_to_dict,
    job_audit_event_to_dict,
    job_side_effect_to_dict,
    preempt_result_to_dict,
    tenant_merge_result_to_dict,
    tenant_offboard_result_to_dict,
    tenant_stats_to_dict,
    webhook_delivery_to_dict,
    webhook_endpoint_to_dict,
)
from job_queue.models import JobStatus
from job_queue.queue import (
    CancelError,
    JobQueue,
    LeaseError,
    RateLimitError,
    ReplayError,
    ScheduleError,
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
            return 200, {"ok": True, "role": "coordinator", "node": os.environ.get("COORDINATOR_NODE", "default")}

        if method == "POST" and path == "/v1/jobs":
            return self._enqueue(body)
        if method == "GET" and path == "/v1/jobs":
            return self._list_jobs(query)
        if method == "POST" and path == "/v1/jobs/cancel-matching":
            return self._cancel_matching(body)
        if method == "POST" and path == "/v1/lease":
            return self._lease(body)
        if method == "POST" and path == "/v1/deadlines/process":
            return self._process_deadlines(body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/heartbeat"):
            return self._job_action(path, body, "heartbeat")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/ack"):
            return self._job_action(path, body, "ack")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/nack"):
            return self._job_action(path, body, "nack")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/release"):
            return self._job_action(path, body, "release")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/checkpoint"):
            return self._job_action(path, body, "checkpoint")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith(
            "/side-effects/begin"
        ):
            return self._begin_side_effect(path, body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith(
            "/side-effects"
        ):
            return self._complete_side_effect(path, body)
        if method == "GET" and path.startswith("/v1/jobs/") and "/side-effects/" in path:
            return self._get_side_effect(path, query)
        if method == "GET" and path.startswith("/v1/jobs/") and path.endswith("/side-effects"):
            return self._list_side_effects(path, query)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/request-approval"):
            return self._job_action(path, body, "request_approval")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith(
            "/spawn-children"
        ):
            return self._spawn_children(path, body)
        if method == "GET" and path.startswith("/v1/jobs/") and path.endswith(
            "/children"
        ):
            return self._list_children(path, query)
        if method == "POST" and path == "/v1/await-children/process-timeouts":
            return self._process_await_children_timeouts(body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/approve"):
            return self._approve_job(path, body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/reject"):
            return self._reject_job(path, body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/cancel"):
            return self._cancel_job(path, body)
        if method == "POST" and path == "/v1/approvals/process-timeouts":
            return self._process_approval_timeouts(body)
        if method == "GET" and path == "/v1/approvals":
            return self._list_approvals(query)
        if method == "GET" and path.startswith("/v1/jobs/") and path.endswith("/audit/verify"):
            return self._job_audit_verify(path, query)
        if method == "GET" and path.startswith("/v1/jobs/") and path.endswith("/audit"):
            return self._job_audit(path, query)
        if method == "GET" and path.startswith("/v1/tenants/") and path.endswith("/audit/verify"):
            return self._tenant_audit_verify(path, query)
        if method == "GET" and path.startswith("/v1/tenants/") and path.endswith("/audit"):
            return self._tenant_audit(path, query)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/rebuild"):
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
        if method == "POST" and path.startswith("/v1/webhooks/") and path.endswith("/finalize"):
            return self._finalize_webhook(path, body)
        # Legacy in-process process path intentionally removed from coordinator
        # so agents always claim+POST+finalize themselves.

        if method == "POST" and path == "/v1/schedules":
            return self._create_schedule(body)
        if method == "GET" and path == "/v1/schedules":
            return self._list_schedules(query)
        if method == "POST" and path.startswith("/v1/schedules/") and path.endswith("/pause"):
            return self._pause_schedule(path, body)
        if method == "POST" and path.startswith("/v1/schedules/") and path.endswith("/resume"):
            return self._resume_schedule(path, body)
        if method == "POST" and path.startswith("/v1/schedules/") and path.endswith("/delete"):
            return self._delete_schedule(path, body)
        if method == "GET" and path.startswith("/v1/schedules/"):
            return self._get_schedule(path, query)

        if method == "POST" and path == "/v1/webhooks":
            return self._register_webhook(body)
        if method == "GET" and path == "/v1/webhooks":
            return self._list_webhooks(query)
        if method == "POST" and path.startswith("/v1/webhooks/") and path.endswith(
            "/rotate-secret"
        ):
            return self._rotate_webhook_secret(path, body)
        if method == "POST" and path.startswith("/v1/webhooks/") and path.endswith(
            "/set-enabled"
        ):
            return self._set_webhook_enabled(path, body)
        if method == "POST" and path.startswith("/v1/webhooks/") and path.endswith("/delete"):
            return self._delete_webhook(path, body)

        if method == "GET" and path == "/v1/dead-letters":
            return self._list_dead_letters(query)
        if method == "POST" and path == "/v1/dead-letters/replay":
            return self._replay_dead_letters(body)
        if method == "POST" and path.startswith("/v1/dead-letters/") and path.endswith(
            "/replay"
        ):
            return self._replay_dead_letter(path, body)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/pause"):
            return self._pause_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/resume"):
            return self._resume_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/concurrency"):
            return self._set_concurrency(path, body)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith(
            "/pending-limit"
        ):
            return self._set_pending_limit(path, body)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith(
            "/rate-limits"
        ):
            return self._set_rate_limits(path, body)
        if method == "GET" and path.startswith("/v1/tenants/") and path.endswith(
            "/rate-limits"
        ):
            return self._get_rate_limits(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith(
            "/retention"
        ):
            return self._set_tenant_retention(path, body)
        if method == "GET" and path.startswith("/v1/tenants/") and path.endswith(
            "/retention"
        ):
            return self._get_tenant_retention(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/offboard"):
            return self._offboard_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/merge"):
            return self._merge_tenant(path, body)

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

        return 404, {"error": f"unknown route {method} {path}"}


