"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _UUID_RE,
)

from typing import Any
from uuid import UUID

from job_queue.codec import (
    bulk_cancel_result_to_dict,
    bulk_freeze_result_to_dict,
    dag_enqueue_result_to_dict,
    job_to_dict,
    schedule_to_dict,
    preempt_result_to_dict,
)
from job_queue.queue import (
    CancelError,
    DependencyError,
    FreezeError,
    PriorityError,
    CloneError,
    QueueMoveError,
    TenantMoveError,
    PostponeError,
    QueueDrainError,
    LeaseError,
    RateLimitError,
    SpendCapError,
)


def _limit_error_response(exc: RateLimitError) -> tuple[int, dict[str, Any]]:
    payload: dict[str, Any] = {
        "error": str(exc),
        "code": (
            "spend_capped" if isinstance(exc, SpendCapError) else "rate_limited"
        ),
        "kind": exc.kind,
        "tenant_id": exc.tenant_id,
    }
    if exc.retry_after_seconds is not None:
        payload["retry_after_seconds"] = exc.retry_after_seconds
    return 429, payload


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
            from datetime import datetime

            deadline_raw = body.get("deadline_at")
            deadline_at = None
            if deadline_raw is not None:
                if isinstance(deadline_raw, str):
                    deadline_at = datetime.fromisoformat(
                        deadline_raw.replace("Z", "+00:00")
                    )
                else:
                    return 400, {"error": "deadline_at must be an ISO-8601 string"}
            expires_raw = body.get("expires_at")
            expires_at = None
            if expires_raw is not None:
                if isinstance(expires_raw, str):
                    expires_at = datetime.fromisoformat(
                        expires_raw.replace("Z", "+00:00")
                    )
                else:
                    return 400, {"error": "expires_at must be an ISO-8601 string"}
            ttl_raw = body.get("ttl_seconds")
            ttl_seconds = None if ttl_raw is None else float(ttl_raw)
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
                retry_backoff_base_seconds=int(
                    body.get("retry_backoff_base_seconds") or 1
                ),
                retry_backoff_max_seconds=int(
                    body.get("retry_backoff_max_seconds") or 300
                ),
                depends_on=dep_ids,
                deadline_at=deadline_at,
                estimated_duration_seconds=(None if est is None else int(est)),
                preemptible=bool(body.get("preemptible", True)),
                labels=labels,
                required_skills=body.get("required_skills"),
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                expiry_action=body.get("expiry_action"),
                max_worker_hops=(
                    None
                    if body.get("max_worker_hops") is None
                    else int(body["max_worker_hops"])
                ),
                quarantine_action=body.get("quarantine_action"),
                prefer_last_worker=bool(body.get("prefer_last_worker") or False),
                coalesce_key=body.get("coalesce_key"),
            )
        except RateLimitError as exc:
            return _limit_error_response(exc)
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}
        return 201, {"job": job_to_dict(job, include_payload=True)}

    def _enqueue_dag(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            result = self.queue.enqueue_dag(
                tenant_id=str(body["tenant_id"]),
                jobs=list(body.get("jobs") or []),
                batch_key=str(body["batch_key"]),
                queue_name=str(body.get("queue_name") or "default"),
            )
        except RateLimitError as exc:
            return _limit_error_response(exc)
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except (KeyError, ValueError, TypeError, DependencyError) as exc:
            return 400, {"error": str(exc)}
        status = 201 if result.created else 200
        return status, dag_enqueue_result_to_dict(result)

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

    def _process_expired_jobs(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            out = self.queue.process_expired_jobs(
                tenant_id=(str(body["tenant_id"]) if body.get("tenant_id") else None),
                limit=int(body.get("limit") or 200),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, out

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

    def _set_job_priority(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        has_pri = "priority" in body
        has_delta = "delta" in body
        if has_pri == has_delta:
            return 400, {"error": "pass exactly one of priority or delta"}
        try:
            if has_pri:
                if body["priority"] is None:
                    return 400, {"error": "priority is required"}
                job = self.queue.set_job_priority(
                    UUID(job_id_s),
                    tenant_id=str(tenant_id),
                    priority=int(body["priority"]),
                )
            else:
                if body["delta"] is None:
                    return 400, {"error": "delta is required"}
                job = self.queue.set_job_priority(
                    UUID(job_id_s),
                    tenant_id=str(tenant_id),
                    delta=int(body["delta"]),
                )
        except PriorityError as exc:
            return 409, {
                "error": str(exc),
                "code": "priority_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _set_job_worker_affinity(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        if "enabled" not in body and "prefer_last_worker" not in body:
            return 400, {"error": "enabled is required"}
        raw = body.get("enabled", body.get("prefer_last_worker"))
        try:
            job = self.queue.set_job_prefer_last_worker(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                enabled=bool(raw),
            )
        except PriorityError as exc:
            return 409, {
                "error": str(exc),
                "code": "priority_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _clone_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        payload = body.get("payload")
        if payload is not None and not isinstance(payload, dict):
            return 400, {"error": "payload must be an object"}
        from datetime import datetime

        available_at = None
        raw_at = body.get("available_at")
        if raw_at is not None:
            if not isinstance(raw_at, str):
                return 400, {"error": "available_at must be an ISO-8601 string"}
            available_at = datetime.fromisoformat(raw_at.replace("Z", "+00:00"))
        delay = body.get("delay_seconds")
        try:
            clone = self.queue.clone_job(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                payload=payload,
                available_at=available_at,
                delay_seconds=(None if delay is None else float(delay)),
                dedupe_key=body.get("dedupe_key"),
            )
        except CloneError as exc:
            code = 404 if getattr(exc, "job", None) is None else 409
            return code, {
                "error": str(exc),
                "code": "clone_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except RateLimitError as exc:
            return _limit_error_response(exc)
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 201, {"job": job_to_dict(clone, include_payload=True)}

    def _move_job_queue(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        raw = body.get("queue_name", body.get("queue"))
        if raw is None or str(raw).strip() == "":
            return 400, {"error": "queue_name is required"}
        try:
            job = self.queue.move_job_queue(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                queue_name=str(raw),
            )
        except QueueMoveError as exc:
            missing = getattr(exc, "job", None) is None and "not found" in str(exc)
            code = 404 if missing else 409
            return code, {
                "error": str(exc),
                "code": "queue_move_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _move_job_tenant(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        dest = body.get("to_tenant_id", body.get("dest_tenant_id"))
        if dest is None or str(dest).strip() == "":
            return 400, {"error": "to_tenant_id is required"}
        try:
            job = self.queue.move_job_tenant(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                to_tenant_id=str(dest),
            )
        except TenantMoveError as exc:
            missing = getattr(exc, "job", None) is None and "not found" in str(exc)
            code = 404 if missing else 409
            return code, {
                "error": str(exc),
                "code": "tenant_move_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job, include_payload=True)}

    def _postpone_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        has_at = "available_at" in body and body.get("available_at") is not None
        has_delay = "delay_seconds" in body and body.get("delay_seconds") is not None
        if has_at == has_delay:
            return 400, {"error": "pass exactly one of available_at or delay_seconds"}
        from datetime import datetime

        available_at = None
        if has_at:
            raw_at = body.get("available_at")
            if not isinstance(raw_at, str):
                return 400, {"error": "available_at must be an ISO-8601 string"}
            available_at = datetime.fromisoformat(raw_at.replace("Z", "+00:00"))
        try:
            job = self.queue.postpone_job(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                available_at=available_at,
                delay_seconds=(
                    None if not has_delay else float(body["delay_seconds"])
                ),
            )
        except PostponeError as exc:
            missing = getattr(exc, "job", None) is None and "not found" in str(exc)
            code = 404 if missing else 409
            return code, {
                "error": str(exc),
                "code": "postpone_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

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

    def _freeze_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            job = self.queue.freeze(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                reason=body.get("reason"),
            )
        except FreezeError as exc:
            return 409, {
                "error": str(exc),
                "code": "freeze_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _unfreeze_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            job = self.queue.unfreeze(UUID(job_id_s), tenant_id=str(tenant_id))
        except FreezeError as exc:
            return 409, {
                "error": str(exc),
                "code": "freeze_error",
                "job": job_to_dict(exc.job) if getattr(exc, "job", None) else None,
            }
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _dead_letter_frozen(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 5:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        try:
            job = self.queue.dead_letter_frozen(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                error=body.get("error") or body.get("reason"),
            )
        except FreezeError as exc:
            return 409, {
                "error": str(exc),
                "code": "freeze_error",
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
        cloned_from = query.get("cloned_from_job_id") or []
        try:
            limit = int((query.get("limit") or ["100"])[0])
            offset = int((query.get("offset") or ["0"])[0])
            cloned_from_id = None
            if cloned_from and cloned_from[0]:
                cloned_from_id = UUID(str(cloned_from[0]))
            jobs = self.queue.list_jobs(
                tenant_id=str(tenants[0]),
                labels=labels or None,
                status=statuses or None,
                queue_name=(
                    str(queue_names[0]) if queue_names and queue_names[0] else None
                ),
                cloned_from_job_id=cloned_from_id,
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

    def _freeze_matching(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id required"}
        try:
            max_jobs = body.get("max_jobs", body.get("limit"))
            result = self.queue.freeze_matching(
                tenant_id=str(tenant_id),
                labels=body.get("labels"),
                status=body.get("status"),
                queue_name=(
                    str(body["queue_name"])
                    if body.get("queue_name") is not None
                    else None
                ),
                reason=body.get("reason"),
                batch_size=int(body.get("batch_size") or 500),
                max_jobs=(None if max_jobs is None else int(max_jobs)),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, bulk_freeze_result_to_dict(result)

    def _unfreeze_matching(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id required"}
        try:
            max_jobs = body.get("max_jobs", body.get("limit"))
            result = self.queue.unfreeze_matching(
                tenant_id=str(tenant_id),
                labels=body.get("labels"),
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
        return 200, bulk_freeze_result_to_dict(result)

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
