"""Coordinator tenant limit/rate/retention API handlers."""
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


class TenantsApiLimitsMixin:
    def _set_concurrency(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / concurrency
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "max_concurrent_leases" not in body and "limit" not in body:
            return 400, {"error": "max_concurrent_leases required (or null to clear)"}
        raw = body.get("max_concurrent_leases", body.get("limit"))
        try:
            limit = None if raw is None else int(raw)
            self.queue.set_concurrency_limit(tenant_id, limit)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "max_concurrent_leases": limit,
        }

    def _set_pending_limit(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / pending-limit
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if "max_pending_jobs" not in body and "limit" not in body:
            return 400, {"error": "max_pending_jobs required (or null to clear)"}
        raw = body.get("max_pending_jobs", body.get("limit"))
        try:
            limit = None if raw is None else int(raw)
            self.queue.set_pending_limit(tenant_id, limit)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "tenant_id": tenant_id,
            "max_pending_jobs": limit,
        }

    def _set_rate_limits(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / rate-limits
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if (
            "enqueue_per_second" not in body
            and "lease_per_second" not in body
            and "enqueue_rate_per_sec" not in body
            and "lease_rate_per_sec" not in body
        ):
            return 400, {
                "error": "enqueue_per_second and/or lease_per_second required "
                "(null clears that limit)"
            }
        try:
            if "enqueue_per_second" in body or "enqueue_rate_per_sec" in body:
                raw = body.get(
                    "enqueue_per_second", body.get("enqueue_rate_per_sec")
                )
                burst = body.get("enqueue_burst")
                rate = None if raw is None else float(raw)
                self.queue.set_enqueue_rate_limit(
                    tenant_id,
                    rate,
                    burst=None if burst is None else int(burst),
                )
            if "lease_per_second" in body or "lease_rate_per_sec" in body:
                raw = body.get(
                    "lease_per_second", body.get("lease_rate_per_sec")
                )
                burst = body.get("lease_burst")
                rate = None if raw is None else float(raw)
                self.queue.set_lease_rate_limit(
                    tenant_id,
                    rate,
                    burst=None if burst is None else int(burst),
                )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        enq = self.queue.get_enqueue_rate_limit(tenant_id)
        lease = self.queue.get_lease_rate_limit(tenant_id)
        return 200, {
            "tenant_id": tenant_id,
            "enqueue_rate_per_sec": enq[0],
            "enqueue_burst": enq[1],
            "lease_rate_per_sec": lease[0],
            "lease_burst": lease[1],
        }

    def _get_rate_limits(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        enq = self.queue.get_enqueue_rate_limit(tenant_id)
        lease = self.queue.get_lease_rate_limit(tenant_id)
        return 200, {
            "tenant_id": tenant_id,
            "enqueue_rate_per_sec": enq[0],
            "enqueue_burst": enq[1],
            "lease_rate_per_sec": lease[0],
            "lease_burst": lease[1],
        }

    def _set_tenant_retention(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / retention
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        if body.get("clear"):
            try:
                settings = self.queue.set_tenant_job_retention(
                    tenant_id, clear=True
                )
            except ValueError as exc:
                return 400, {"error": str(exc)}
            out = job_retention_to_dict(settings)
            out["tenant_id"] = tenant_id
            return 200, out
        kwargs: dict[str, Any] = {}
        if (
            "completed_retention_seconds" in body
            or "completed_seconds" in body
        ):
            c_raw = body.get(
                "completed_retention_seconds", body.get("completed_seconds")
            )
            kwargs["completed_seconds"] = (
                None if c_raw is None else int(c_raw)
            )
        if "dead_retention_seconds" in body or "dead_seconds" in body:
            d_raw = body.get(
                "dead_retention_seconds", body.get("dead_seconds")
            )
            kwargs["dead_seconds"] = None if d_raw is None else int(d_raw)
        if not kwargs:
            return 400, {
                "error": "completed_retention_seconds and/or "
                "dead_retention_seconds required (null = keep forever "
                "for this tenant); or clear=true to inherit global"
            }
        try:
            settings = self.queue.set_tenant_job_retention(
                tenant_id, **kwargs
            )
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        out = job_retention_to_dict(settings)
        out["tenant_id"] = tenant_id
        return 200, out

    def _get_tenant_retention(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        settings = self.queue.get_tenant_job_retention(tenant_id)
        out = job_retention_to_dict(settings)
        out["tenant_id"] = tenant_id
        return 200, out

    def _set_retention(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        if (
            "completed_retention_seconds" not in body
            and "completed_seconds" not in body
            and "dead_retention_seconds" not in body
            and "dead_seconds" not in body
        ):
            return 400, {
                "error": "completed_retention_seconds and/or "
                "dead_retention_seconds required (null = keep forever)"
            }
        try:
            c_raw = body.get(
                "completed_retention_seconds", body.get("completed_seconds")
            )
            d_raw = body.get(
                "dead_retention_seconds", body.get("dead_seconds")
            )
            # Allow partial update: only provided keys change.
            current = self.queue.get_job_retention()
            completed = (
                current.completed_retention_seconds
                if "completed_retention_seconds" not in body
                and "completed_seconds" not in body
                else (None if c_raw is None else int(c_raw))
            )
            dead = (
                current.dead_retention_seconds
                if "dead_retention_seconds" not in body
                and "dead_seconds" not in body
                else (None if d_raw is None else int(d_raw))
            )
            settings = self.queue.set_job_retention(
                completed_seconds=completed,
                dead_seconds=dead,
            )
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 200, job_retention_to_dict(settings)

    def _get_retention(self) -> tuple[int, dict[str, Any]]:
        return 200, job_retention_to_dict(self.queue.get_job_retention())
