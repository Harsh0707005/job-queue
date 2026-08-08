"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _UUID_RE,
)

from typing import Any
from uuid import UUID

from job_queue.codec import (
    bulk_cancel_result_to_dict,
    job_to_dict,
    schedule_to_dict,
    preempt_result_to_dict,
)
from job_queue.queue import (
    CancelError,
    LeaseError,
    RateLimitError,
)


class JobsApiMixin:
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
            deadline_raw = body.get("deadline_at")
            deadline_at = None
            if deadline_raw is not None:
                from datetime import datetime

                if isinstance(deadline_raw, str):
                    deadline_at = datetime.fromisoformat(
                        deadline_raw.replace("Z", "+00:00")
                    )
                else:
                    return 400, {"error": "deadline_at must be an ISO-8601 string"}
            est = body.get("estimated_duration_seconds")
            labels = body.get("labels")
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
                deadline_at=deadline_at,
                estimated_duration_seconds=(None if est is None else int(est)),
                preemptible=bool(body.get("preemptible", True)),
                labels=labels,
            )
        except RateLimitError as exc:
            payload: dict[str, Any] = {
                "error": str(exc),
                "code": "rate_limited",
                "kind": exc.kind,
                "tenant_id": exc.tenant_id,
            }
            if exc.retry_after_seconds is not None:
                payload["retry_after_seconds"] = exc.retry_after_seconds
            return 429, payload
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"job": job_to_dict(job, include_payload=True)}

    def _process_deadlines(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            results = self.queue.process_deadlines(
                tenant_id=(str(body["tenant_id"]) if body.get("tenant_id") else None),
                queue_name=str(body.get("queue_name") or "default"),
                limit=int(body.get("limit") or 20),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "count": len(results),
            "preemptions": [preempt_result_to_dict(r) for r in results],
        }

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
        # Lease responses are the only place that must return fencing tokens.
        jobs_out = [
            job_to_dict(j, include_lease_token=True, include_payload=True)
            for j in jobs
        ]
        payload: dict[str, Any] = {
            "jobs": jobs_out,
            "count": len(jobs_out),
        }
        # Backward-compatible single-job field when limit==1 / one result.
        if len(jobs_out) == 1:
            payload["job"] = jobs_out[0]
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
            elif action == "release":
                job = self.queue.release(
                    job_id,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    reason=body.get("reason"),
                )
            elif action == "checkpoint":
                if "progress" not in body and "checkpoint" not in body:
                    return 400, {"error": "progress required"}
                progress = body.get("progress", body.get("checkpoint"))
                job = self.queue.checkpoint(
                    job_id,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    progress=progress,
                )
            elif action == "request_approval":
                from job_queue.queue import ApprovalError

                try:
                    job = self.queue.request_approval(
                        job_id,
                        tenant_id=str(tenant_id),
                        worker_id=str(worker_id),
                        lease_token=token,
                        message=body.get("message"),
                        timeout_seconds=float(body.get("timeout_seconds") or 3600),
                        progress=body.get("progress"),
                    )
                except ApprovalError as exc:
                    return 409, {"error": str(exc), "code": "approval_error"}
            else:
                return 400, {"error": f"unknown action {action}"}
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}

        return 200, {"job": job_to_dict(job)}

    def _list_jobs(self, query: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        labels = query.get("label") or query.get("labels") or []
        statuses = query.get("status") or []
        queue_names = query.get("queue_name") or []
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
            jobs = self.queue.list_jobs(
                tenant_id=str(tenants[0]),
                labels=labels or None,
                status=statuses or None,
                queue_name=(
                    str(queue_names[0]) if queue_names and queue_names[0] else None
                ),
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "jobs": [job_to_dict(j) for j in jobs],
            "count": len(jobs),
        }

    def _cancel_matching(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id required"}
        try:
            max_jobs = body.get("max_jobs", body.get("limit"))
            result = self.queue.cancel_matching(
                tenant_id=str(tenant_id),
                labels=body.get("labels"),
                status=body.get("status"),
                queue_name=(
                    str(body["queue_name"])
                    if body.get("queue_name") is not None
                    else None
                ),
                batch_size=int(body.get("batch_size") or 500),
                max_jobs=(None if max_jobs is None else int(max_jobs)),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, bulk_cancel_result_to_dict(result)

    def _get_job(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        # /v1/jobs/{uuid}?tenant_id=
        parts = path.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "v1" or parts[1] != "jobs":
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        job = self.queue.get(UUID(job_id_s), tenant_id=str(tenants[0]))
        if job is None:
            return 404, {"error": "job not found"}
        return 200, {"job": job_to_dict(job)}

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
