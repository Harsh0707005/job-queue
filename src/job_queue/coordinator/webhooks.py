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


class WebhooksApiMixin:
    def _claim_webhooks(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        """Claim deliveries for remote agents — coordinator does NOT POST."""
        limit = int(body.get("limit") or 50)
        timeout = float(body.get("http_timeout_seconds") or 10.0)
        claim_seconds = body.get("claim_seconds")
        claimed, auto = self.queue.claim_webhook_deliveries(
            limit=limit,
            http_timeout_seconds=timeout,
            claim_seconds=float(claim_seconds) if claim_seconds is not None else None,
        )
        auto_out = []
        for r in auto:
            auto_out.append(
                {
                    "delivery_id": str(r.delivery.id),
                    "tenant_id": r.delivery.tenant_id,
                    "job_id": str(r.delivery.job_id),
                    "status": r.delivery.status,
                    "success": r.success,
                    "status_code": r.status_code,
                    "error": r.error,
                }
            )
        return 200, {
            "count": len(claimed),
            "claimed": [claimed_webhook_to_dict(c) for c in claimed],
            "auto_finalized": auto_out,
        }


    def _finalize_webhook(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / webhooks / {id} / finalize
        if len(parts) != 4:
            return 400, {"error": "invalid finalize path"}
        did = parts[2]
        if not _UUID_RE.match(did):
            return 400, {"error": "invalid delivery id"}
        result = self.queue.finalize_webhook_delivery(
            UUID(did),
            status_code=body.get("status_code"),
            error=body.get("error"),
            endpoint_missing=bool(body.get("endpoint_missing", False)),
        )
        if result is None:
            return 409, {
                "error": "delivery not in delivering state",
                "code": "lease_error",
            }
        return 200, {
            "delivery": webhook_delivery_to_dict(result.delivery),
            "success": result.success,
            "status_code": result.status_code,
            "error": result.error,
        }


    def _register_webhook(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            ep = self.queue.register_webhook(
                tenant_id=str(body["tenant_id"]),
                url=str(body["url"]),
                events=body.get("events"),
                secret=body.get("secret"),
                max_attempts=int(body.get("max_attempts") or 5),
                backoff_base_seconds=int(body.get("backoff_base_seconds") or 1),
                backoff_max_seconds=int(body.get("backoff_max_seconds") or 300),
                enabled=bool(body.get("enabled", True)),
            )
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        # Echo secret only on create (caller just set it).
        return 201, {
            "endpoint": webhook_endpoint_to_dict(ep, include_secret=True)
        }


    def _rotate_webhook_secret(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        # /v1/webhooks/{id}/rotate-secret
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid webhook path"}
        eid = parts[2]
        if not _UUID_RE.match(eid):
            return 400, {"error": "invalid webhook id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        # secret may be explicitly null to clear
        if "secret" not in body:
            return 400, {"error": "secret is required (use null to clear)"}
        try:
            ep = self.queue.rotate_webhook_secret(
                UUID(eid),
                tenant_id=str(tenant_id),
                secret=body.get("secret"),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        # Echo secret only on rotate (caller just set it).
        return 200, {
            "endpoint": webhook_endpoint_to_dict(ep, include_secret=True)
        }


    def _list_webhooks(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant_id = (query.get("tenant_id") or [None])[0]
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            rows = self.queue.list_webhooks(tenant_id=str(tenant_id))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        # Never return plaintext secrets on list.
        return 200, {
            "endpoints": [
                webhook_endpoint_to_dict(e, include_secret=False) for e in rows
            ]
        }


    def _set_webhook_enabled(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid webhook path"}
        eid = parts[2]
        if not _UUID_RE.match(eid):
            return 400, {"error": "invalid webhook id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        if "enabled" not in body:
            return 400, {"error": "enabled is required"}
        try:
            ep = self.queue.set_webhook_enabled(
                UUID(eid),
                tenant_id=str(tenant_id),
                enabled=bool(body.get("enabled")),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        # Status-only mutation — do not leak secret.
        return 200, {
            "endpoint": webhook_endpoint_to_dict(ep, include_secret=False)
        }


    def _delete_webhook(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid webhook path"}
        eid = parts[2]
        if not _UUID_RE.match(eid):
            return 400, {"error": "invalid webhook id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        deleted = self.queue.delete_webhook(UUID(eid), tenant_id=str(tenant_id))
        if not deleted:
            return 404, {"error": f"webhook {eid} not found for tenant"}
        return 200, {"deleted": True, "endpoint_id": eid}


    def _list_dead_letters(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant_id = (query.get("tenant_id") or [None])[0]
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        queue_name = (query.get("queue_name") or [None])[0]
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
        except ValueError:
            return 400, {"error": "limit/offset must be integers"}
        try:
            jobs = self.queue.list_dead_letters(
                tenant_id=str(tenant_id),
                queue_name=str(queue_name) if queue_name else None,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"jobs": [job_to_dict(j) for j in jobs]}


    def _replay_dead_letter(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / dead-letters / {id} / replay
        if len(parts) != 4:
            return 400, {"error": "invalid dead-letter path"}
        jid = parts[2]
        if not _UUID_RE.match(jid):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            job = self.queue.replay_dead_letter(
                UUID(jid),
                tenant_id=str(tenant_id),
                reset_attempts=bool(body.get("reset_attempts", True)),
                max_attempts=(
                    None
                    if body.get("max_attempts") is None
                    else int(body["max_attempts"])
                ),
                delay_seconds=(
                    None
                    if body.get("delay_seconds") is None
                    else float(body["delay_seconds"])
                ),
                clear_error=bool(body.get("clear_error", False)),
            )
        except (ValueError, TypeError) as exc:
            return 400, {"error": str(exc)}
        except ReplayError as exc:
            return 409, {"error": str(exc), "code": "replay_error"}
        return 200, {"job": job_to_dict(job)}


    def _replay_dead_letters(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        raw_ids = body.get("job_ids")
        job_ids: list[UUID] | None = None
        if raw_ids is not None:
            try:
                job_ids = [UUID(str(x)) for x in raw_ids]
            except (TypeError, ValueError) as exc:
                return 400, {"error": f"invalid job_ids: {exc}"}
        try:
            jobs = self.queue.replay_dead_letters(
                tenant_id=str(tenant_id),
                job_ids=job_ids,
                queue_name=(
                    None
                    if body.get("queue_name") is None
                    else str(body["queue_name"])
                ),
                limit=(
                    None if body.get("limit") is None else int(body["limit"])
                ),
                reset_attempts=bool(body.get("reset_attempts", True)),
                max_attempts=(
                    None
                    if body.get("max_attempts") is None
                    else int(body["max_attempts"])
                ),
                delay_seconds=(
                    None
                    if body.get("delay_seconds") is None
                    else float(body["delay_seconds"])
                ),
                clear_error=bool(body.get("clear_error", False)),
            )
        except (ValueError, TypeError) as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "jobs": [job_to_dict(j) for j in jobs],
            "replayed": len(jobs),
        }


    def _webhook_stats(self) -> tuple[int, dict[str, Any]]:
        with self.queue._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*)::int AS n
                FROM webhook_deliveries
                GROUP BY status
                """
            ).fetchall()
            # Detect multiple succeeded deliveries for same job+event (should be 1).
            dups = conn.execute(
                """
                SELECT job_id::text AS job_id, event, COUNT(*)::int AS n
                FROM webhook_deliveries
                WHERE status = 'succeeded'
                GROUP BY job_id, event
                HAVING COUNT(*) > 1
                """
            ).fetchall()
        return 200, {
            "by_status": {r["status"]: int(r["n"]) for r in rows},
            "duplicate_succeeded_job_events": [dict(r) for r in dups],
        }


