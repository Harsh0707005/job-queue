"""HTTP coordinator: multi-node fleet control plane over Postgres JobQueue.

Remote worker agents, schedule nodes, and webhook nodes call this service.
The coordinator is a thin Postgres façade. Worker agents lease over HTTP;
webhook agents claim over HTTP and POST themselves (coordinator never POSTs).
"""

from __future__ import annotations

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
    claimed_webhook_to_dict,
    job_to_dict,
    schedule_to_dict,
    tenant_merge_result_to_dict,
    tenant_offboard_result_to_dict,
    tenant_stats_to_dict,
    webhook_delivery_to_dict,
    webhook_endpoint_to_dict,
)
from job_queue.models import JobStatus
from job_queue.queue import CancelError, JobQueue, LeaseError, ScheduleError

logger = logging.getLogger(__name__)

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class Coordinator:
    """Thin HTTP façade over :class:`JobQueue`."""

    def __init__(self, queue: JobQueue) -> None:
        self.queue = queue

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

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
        if method == "POST" and path == "/v1/lease":
            return self._lease(body)
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/heartbeat"):
            return self._job_action(path, body, "heartbeat")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/ack"):
            return self._job_action(path, body, "ack")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/nack"):
            return self._job_action(path, body, "nack")
        if method == "POST" and path.startswith("/v1/jobs/") and path.endswith("/cancel"):
            return self._cancel_job(path, body)

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
        if method == "POST" and path == "/v1/webhooks":
            return self._register_webhook(body)
        if method == "POST" and path.startswith("/v1/schedules/") and path.endswith("/pause"):
            return self._pause_schedule(path, body)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/pause"):
            return self._pause_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/resume"):
            return self._resume_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/offboard"):
            return self._offboard_tenant(path)
        if method == "POST" and path.startswith("/v1/tenants/") and path.endswith("/merge"):
            return self._merge_tenant(path, body)

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

    # ------------------------------------------------------------------
    # Worker protocol
    # ------------------------------------------------------------------

    def _enqueue(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            deps = body.get("depends_on")
            dep_ids = None
            if deps is not None:
                from uuid import UUID as _UUID

                if isinstance(deps, (str,)):
                    dep_ids = _UUID(str(deps))
                else:
                    dep_ids = [_UUID(str(x)) for x in deps]
            job = self.queue.enqueue(
                tenant_id=str(body["tenant_id"]),
                payload=dict(body.get("payload") or {}),
                queue_name=str(body.get("queue_name") or "default"),
                priority=int(body.get("priority") or 0),
                max_attempts=int(body.get("max_attempts") or 3),
                visibility_timeout_seconds=int(
                    body.get("visibility_timeout_seconds") or 30
                ),
                max_runtime_seconds=int(body.get("max_runtime_seconds") or 3600),
                depends_on=dep_ids,
            )
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"job": job_to_dict(job)}

    def _lease(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        worker_id = body.get("worker_id")
        if not worker_id:
            return 400, {"error": "worker_id is required"}
        tenant_id = body.get("tenant_id")  # None => fair
        queue_name = body.get("queue_name") or "default"
        vt = body.get("visibility_timeout_seconds")
        limit = int(body.get("limit") or 1)
        if limit < 1:
            return 400, {"error": "limit must be >= 1"}
        try:
            jobs = self.queue.lease_batch(
                worker_id=str(worker_id),
                limit=limit,
                tenant_id=str(tenant_id) if tenant_id else None,
                queue_name=str(queue_name),
                visibility_timeout_seconds=int(vt) if vt is not None else None,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        if not jobs:
            return 204, {}
        payload: dict[str, Any] = {
            "jobs": [job_to_dict(j) for j in jobs],
            "count": len(jobs),
        }
        # Backward-compatible single-job field when limit==1 / one result.
        if len(jobs) == 1:
            payload["job"] = payload["jobs"][0]
        return 200, payload

    def _cancel_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / cancel
        if len(parts) != 4:
            return 400, {"error": "invalid cancel path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            job = self.queue.cancel(UUID(job_id_s), tenant_id=str(tenant_id))
        except CancelError as exc:
            return 409, {
                "error": str(exc),
                "code": "cancel_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _job_action(
        self, path: str, body: dict[str, Any], action: str
    ) -> tuple[int, dict[str, Any]]:
        # /v1/jobs/{uuid}/{action}
        parts = path.strip("/").split("/")
        if len(parts) != 4 or parts[0] != "v1" or parts[1] != "jobs":
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        job_id = UUID(job_id_s)

        tenant_id = body.get("tenant_id")
        worker_id = body.get("worker_id")
        lease_token = body.get("lease_token")
        if not tenant_id or not worker_id or not lease_token:
            return 400, {"error": "tenant_id, worker_id, lease_token required"}
        try:
            token = UUID(str(lease_token))
        except ValueError:
            return 400, {"error": "invalid lease_token"}

        try:
            if action == "heartbeat":
                job = self.queue.heartbeat(
                    job_id,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    extend_by_seconds=body.get("extend_by_seconds"),
                )
            elif action == "ack":
                job = self.queue.ack(
                    job_id,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    result=body.get("result"),
                )
            elif action == "nack":
                job = self.queue.nack(
                    job_id,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    error=body.get("error"),
                    requeue=bool(body.get("requeue", True)),
                )
            else:
                return 400, {"error": f"unknown action {action}"}
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}

        return 200, {"job": job_to_dict(job)}

    # ------------------------------------------------------------------
    # Scheduler / webhook nodes
    # ------------------------------------------------------------------

    def _process_due(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        limit = int(body.get("limit") or 100)
        tenant_id = body.get("tenant_id")
        fires = self.queue.process_due_schedules(
            tenant_id=str(tenant_id) if tenant_id else None,
            limit=limit,
        )
        return 200, {
            "count": len(fires),
            "fires": [
                {
                    "schedule": schedule_to_dict(f.schedule),
                    "job": job_to_dict(f.job),
                }
                for f in fires
            ],
        }

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

    def _pause_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / pause
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        self.queue.pause_tenant(tenant_id)
        return 200, {"tenant_id": tenant_id, "paused": True}

    def _resume_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            self.queue.resume_tenant(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"tenant_id": tenant_id, "paused": False}

    def _offboard_tenant(self, path: str) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {id} / offboard
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        tenant_id = parts[2]
        try:
            result = self.queue.offboard_tenant(tenant_id)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "result": tenant_offboard_result_to_dict(result),
            "tenant_id": tenant_id,
            "empty": result.empty,
            "offboarding": result.offboarding,
            "leased_remaining": result.leased_remaining,
            "webhooks_delivering_remaining": result.webhooks_delivering_remaining,
        }

    def _merge_tenant(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / tenants / {source_id} / merge
        if len(parts) != 4:
            return 400, {"error": "invalid tenant path"}
        source_tenant_id = parts[2]
        target_tenant_id = body.get("target_tenant_id") or body.get("into")
        if not target_tenant_id:
            return 400, {"error": "target_tenant_id required"}
        try:
            result = self.queue.merge_tenant(
                source_tenant_id, str(target_tenant_id)
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except Exception as exc:
            # MergeError (and unexpected) surface as 409 conflict / 400.
            from job_queue.queue import MergeError

            if isinstance(exc, MergeError):
                return 409, {"error": str(exc), "code": "merge_error"}
            raise
        return 200, {
            "result": tenant_merge_result_to_dict(result),
            "source_tenant_id": result.source_tenant_id,
            "target_tenant_id": result.target_tenant_id,
            "source_empty": result.source_empty,
            "jobs_moved": result.jobs_moved,
            "jobs_leased_moved": result.jobs_leased_moved,
        }

    # ------------------------------------------------------------------
    # Setup / admin
    # ------------------------------------------------------------------

    def _create_schedule(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            sched = self.queue.create_schedule(
                tenant_id=str(body["tenant_id"]),
                interval_seconds=int(body["interval_seconds"]),
                payload=dict(body.get("payload") or {}),
                queue_name=str(body.get("queue_name") or "default"),
                name=body.get("name"),
                priority=int(body.get("priority") or 0),
                max_attempts=int(body.get("max_attempts") or 3),
                visibility_timeout_seconds=int(
                    body.get("visibility_timeout_seconds") or 30
                ),
                enabled=bool(body.get("enabled", True)),
            )
        except (KeyError, ValueError, ScheduleError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"schedule": schedule_to_dict(sched)}

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
        return 201, {"endpoint": webhook_endpoint_to_dict(ep)}

    def _pause_schedule(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / schedules / {id} / pause
        if len(parts) != 4:
            return 400, {"error": "invalid schedule path"}
        sid = parts[2]
        if not _UUID_RE.match(sid):
            return 400, {"error": "invalid schedule id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            sched = self.queue.pause_schedule(UUID(sid), tenant_id=str(tenant_id))
        except ScheduleError as exc:
            return 404, {"error": str(exc)}
        return 200, {"schedule": schedule_to_dict(sched)}

    def _backlog(self) -> tuple[int, dict[str, Any]]:
        with self.queue._pool.connection() as conn:
            job_row = conn.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled
                FROM jobs
                """
            ).fetchone()
            wh_row = conn.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded
                FROM webhook_deliveries
                """
            ).fetchone()
        assert job_row is not None and wh_row is not None
        return 200, {
            "jobs": dict(job_row),
            "webhooks": dict(wh_row),
        }

    def _completed_by_schedule(self) -> tuple[int, dict[str, Any]]:
        with self.queue._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT schedule_id::text AS schedule_id, COUNT(*)::int AS completed
                FROM jobs
                WHERE schedule_id IS NOT NULL
                  AND status = %s
                GROUP BY schedule_id
                ORDER BY schedule_id
                """,
                (JobStatus.COMPLETED.value,),
            ).fetchall()
            scheds = conn.execute(
                """
                SELECT id::text AS id, tenant_id, name, enabled, interval_seconds
                FROM schedules
                ORDER BY tenant_id
                """
            ).fetchall()
        by = {r["schedule_id"]: int(r["completed"]) for r in rows}
        return 200, {
            "completed_by_schedule": by,
            "schedules": [dict(s) for s in scheds],
        }

    def _list_stats(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant") or [None])[0]
        if tenant:
            stats = self.queue.get_tenant_stats(tenant)
            return 200, {"stats": [tenant_stats_to_dict(stats)]}
        all_stats = self.queue.list_tenant_stats()
        return 200, {"stats": [tenant_stats_to_dict(s) for s in all_stats]}

    def _tenant_residue(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Per-tenant residual state for offboard/empty soak verification.

        Pass ``?tenant=id`` for one tenant, or omit for every tenant that still
        has any jobs / schedules / webhooks / settings / fairness row.
        """
        single = (query.get("tenant") or [None])[0]

        def residue_for(conn: Any, tenant_id: str) -> dict[str, Any]:
            jobs = conn.execute(
                """
                SELECT
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'leased')::int AS leased,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed
                FROM jobs
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            schedules = conn.execute(
                "SELECT COUNT(*)::int AS n FROM schedules WHERE tenant_id = %s",
                (tenant_id,),
            ).fetchone()
            endpoints = conn.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM webhook_endpoints
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            deliveries = conn.execute(
                """
                SELECT
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'delivering')::int AS delivering,
                  COUNT(*) FILTER (WHERE status = 'succeeded')::int AS succeeded,
                  COUNT(*) FILTER (WHERE status = 'dead')::int AS dead
                FROM webhook_deliveries
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            settings = conn.execute(
                """
                SELECT paused, offboarding
                FROM tenant_settings
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            ).fetchone()
            fairness = conn.execute(
                """
                SELECT 1 FROM tenant_fairness WHERE tenant_id = %s LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            j_total = int(jobs["total"]) if jobs else 0
            s_n = int(schedules["n"]) if schedules else 0
            e_n = int(endpoints["n"]) if endpoints else 0
            d_total = int(deliveries["total"]) if deliveries else 0
            has_settings = settings is not None
            has_fairness = fairness is not None
            empty = (
                j_total == 0
                and s_n == 0
                and e_n == 0
                and d_total == 0
                and not has_settings
                and not has_fairness
            )
            return {
                "tenant_id": tenant_id,
                "empty": empty,
                "offboarding": bool(settings["offboarding"]) if settings else False,
                "paused": bool(settings["paused"]) if settings else False,
                "jobs": dict(jobs) if jobs else {},
                "schedules": s_n,
                "webhook_endpoints": e_n,
                "webhook_deliveries": dict(deliveries) if deliveries else {},
                "has_settings": has_settings,
                "has_fairness": has_fairness,
            }

        with self.queue._pool.connection() as conn:
            if single:
                return 200, {"tenants": [residue_for(conn, str(single))]}
            rows = conn.execute(
                """
                SELECT tenant_id FROM jobs
                UNION
                SELECT tenant_id FROM schedules
                UNION
                SELECT tenant_id FROM webhook_endpoints
                UNION
                SELECT tenant_id FROM webhook_deliveries
                UNION
                SELECT tenant_id FROM tenant_settings
                UNION
                SELECT tenant_id FROM tenant_fairness
                ORDER BY tenant_id
                """
            ).fetchall()
            tenants = [residue_for(conn, r["tenant_id"]) for r in rows]
            return 200, {"tenants": tenants}

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

    def _fairness_check(self) -> tuple[int, dict[str, Any]]:
        """Schedule overlap + paused claimable + completed-by-tenant for soak."""
        with self.queue._pool.connection() as conn:
            active = conn.execute(
                """
                SELECT schedule_id::text AS schedule_id, COUNT(*)::int AS n
                FROM jobs
                WHERE schedule_id IS NOT NULL
                  AND status IN ('pending', 'leased')
                GROUP BY schedule_id
                """
            ).fetchall()
            # New pending work should not appear for paused tenants (schedules
            # skip pause; reclaim also skips). In-flight leased jobs may finish.
            paused_claimable = conn.execute(
                """
                SELECT j.tenant_id, COUNT(*)::int AS n
                FROM jobs j
                JOIN tenant_settings ts ON ts.tenant_id = j.tenant_id
                WHERE ts.paused = TRUE
                  AND j.status = 'pending'
                GROUP BY j.tenant_id
                """
            ).fetchall()
            by_tenant = conn.execute(
                """
                SELECT tenant_id, COUNT(*)::int AS completed
                FROM jobs
                WHERE status = 'completed'
                GROUP BY tenant_id
                ORDER BY tenant_id
                """
            ).fetchall()
        return 200, {
            "active_jobs_by_schedule": {
                r["schedule_id"]: int(r["n"]) for r in active
            },
            "paused_tenant_claimable": {
                r["tenant_id"]: int(r["n"]) for r in paused_claimable
            },
            "completed_by_tenant": {
                r["tenant_id"]: int(r["completed"]) for r in by_tenant
            },
            "max_active_per_schedule": max(
                (int(r["n"]) for r in active), default=0
            ),
        }

    def _completion_order(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Completed jobs in completion time order (for priority-lane soaks)."""
        limit = int((query.get("limit") or ["500"])[0])
        limit = max(1, min(limit, 5000))
        with self.queue._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id::text AS id, tenant_id, priority, completed_at
                FROM jobs
                WHERE status = 'completed'
                  AND completed_at IS NOT NULL
                ORDER BY completed_at ASC, created_at ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return 200, {
            "completed": [
                {
                    "id": r["id"],
                    "tenant_id": r["tenant_id"],
                    "priority": int(r["priority"]),
                    "completed_at": r["completed_at"].isoformat()
                    if r["completed_at"]
                    else None,
                }
                for r in rows
            ]
        }

    def _priority_stats(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        """Aggregate high/low lane progress (defaults: high>=50, low<50)."""
        high_min = int((query.get("high_min") or ["50"])[0])
        with self.queue._pool.connection() as conn:
            high_c = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'completed' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            low_c = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'completed' AND priority < %s
                """,
                (high_min,),
            ).fetchone()
            high_p = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'pending' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            high_l = conn.execute(
                """
                SELECT COUNT(*)::int AS n FROM jobs
                WHERE status = 'leased' AND priority >= %s
                """,
                (high_min,),
            ).fetchone()
            by_tenant = conn.execute(
                """
                SELECT tenant_id, COUNT(*)::int AS n
                FROM jobs
                WHERE status = 'completed' AND priority >= %s
                GROUP BY tenant_id
                ORDER BY tenant_id
                """,
                (high_min,),
            ).fetchall()
        assert high_c is not None and low_c is not None
        assert high_p is not None and high_l is not None
        return 200, {
            "high_min": high_min,
            "high_completed": int(high_c["n"]),
            "low_completed": int(low_c["n"]),
            "high_pending": int(high_p["n"]),
            "high_leased": int(high_l["n"]),
            "high_completed_by_tenant": {
                r["tenant_id"]: int(r["n"]) for r in by_tenant
            },
        }


def _make_handler(coordinator: Coordinator) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("http: " + format, *args)

        def _read_json(self) -> dict[str, Any] | None:
            length = int(self.headers.get("Content-Length") or "0")
            if length <= 0:
                return None
            raw = self.rfile.read(length)
            if not raw:
                return None
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid json: {exc}") from exc
            if data is None:
                return {}
            if not isinstance(data, dict):
                raise ValueError("json body must be an object")
            return data

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            if status == 204:
                self.send_response(204)
                self.end_headers()
                return
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            # keep trailing structure for /v1/jobs/.../ack
            if self.path.split("?", 1)[0].endswith("/") and path != "/":
                path = self.path.split("?", 1)[0].rstrip("/")
            query = parse_qs(parsed.query)
            try:
                body = self._read_json() if method in ("POST", "PUT", "PATCH") else None
                status, payload = coordinator.handle(method, path, body, query)
                self._send(status, payload)
            except ValueError as exc:
                self._send(400, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                logger.exception("coordinator error: %s", exc)
                self._send(
                    500,
                    {
                        "error": str(exc),
                        "trace": traceback.format_exc().splitlines()[-5:],
                    },
                )

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT")

    return Handler


def serve_coordinator(
    dsn: str,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    min_size: int = 2,
    max_size: int = 20,
) -> None:
    """Bind ThreadingHTTPServer and serve forever."""
    queue = JobQueue.connect(dsn, min_size=min_size, max_size=max_size)
    coordinator = Coordinator(queue)
    handler = _make_handler(coordinator)
    server = ThreadingHTTPServer((host, port), handler)
    logger.info("coordinator listening on http://%s:%s", host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        queue.close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    p = argparse.ArgumentParser(prog="job-queue-coordinator")
    p.add_argument(
        "--dsn",
        default=os.environ.get("JOB_QUEUE_DSN") or os.environ.get("DATABASE_URL"),
        help="Postgres DSN",
    )
    p.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    args = p.parse_args(argv)
    if not args.dsn:
        print("JOB_QUEUE_DSN / --dsn required", file=__import__("sys").stderr)
        return 2
    dsn = (
        args.dsn.replace("postgresql+psycopg://", "postgresql://", 1)
        .replace("postgresql+psycopg2://", "postgresql://", 1)
    )
    serve_coordinator(dsn, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
