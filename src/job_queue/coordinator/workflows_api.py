"""Coordinator multi-step workflow / saga API."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.coordinator.auth import _UUID_RE
from job_queue.crypto import redact_payload
from job_queue.models import (
    WorkflowActivity,
    WorkflowRun,
    WorkflowSignal,
    WorkflowStepRun,
)
from job_queue.queue._common import (
    CancelError,
    LeaseError,
    QueueDrainError,
    RateLimitError,
)


def _redact_steps_def(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in steps:
        step = dict(raw)
        if "payload" in step:
            step["payload"] = redact_payload(step.get("payload"))
        comp = step.get("compensate")
        if isinstance(comp, dict):
            c = dict(comp)
            if "payload" in c:
                c["payload"] = redact_payload(c.get("payload"))
            step["compensate"] = c
        out.append(step)
    return out


def _run_to_dict(r: WorkflowRun) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "tenant_id": r.tenant_id,
        "name": r.name,
        "status": r.status,
        "steps_def": _redact_steps_def(list(r.steps_def or [])),
        "current_index": r.current_index,
        "fail_step_index": r.fail_step_index,
        "fail_reason": r.fail_reason,
        "max_compensate_retries": r.max_compensate_retries,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
    }


def _step_to_dict(s: WorkflowStepRun) -> dict[str, Any]:
    return {
        "id": str(s.id),
        "run_id": str(s.run_id),
        "tenant_id": s.tenant_id,
        "step_index": s.step_index,
        "name": s.name,
        "direction": s.direction,
        "status": s.status,
        "job_id": str(s.job_id) if s.job_id else None,
        "result": s.result,
        "error": s.error,
        "idempotency_key": s.idempotency_key,
        "retry_count": s.retry_count,
        "started_at": s.started_at.isoformat() if s.started_at else None,
        "completed_at": s.completed_at.isoformat() if s.completed_at else None,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        "deadline_at": s.deadline_at.isoformat() if s.deadline_at else None,
    }


def _activity_to_dict(a: WorkflowActivity) -> dict[str, Any]:
    return {
        "id": str(a.id),
        "step_run_id": str(a.step_run_id),
        "job_id": str(a.job_id) if a.job_id else None,
        "run_id": str(a.run_id),
        "tenant_id": a.tenant_id,
        "name": a.name,
        "url": a.url,
        "status": a.status,
        "timeout_seconds": a.timeout_seconds,
        "heartbeat_timeout_seconds": a.heartbeat_timeout_seconds,
        "started_at": a.started_at.isoformat() if a.started_at else None,
        "last_heartbeat_at": (
            a.last_heartbeat_at.isoformat() if a.last_heartbeat_at else None
        ),
        "heartbeat_due_at": (
            a.heartbeat_due_at.isoformat() if a.heartbeat_due_at else None
        ),
        "timeout_at": a.timeout_at.isoformat() if a.timeout_at else None,
        "completed_at": a.completed_at.isoformat() if a.completed_at else None,
        "result": a.result,
        "error": a.error,
    }


def _signal_to_dict(s: WorkflowSignal) -> dict[str, Any]:
    return {
        "id": str(s.id),
        "run_id": str(s.run_id),
        "tenant_id": s.tenant_id,
        "name": s.name,
        "payload": redact_payload(s.payload),
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }


class WorkflowsApiMixin:
    def _start_workflow(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        tenant_id = body.get("tenant_id")
        steps = body.get("steps")
        if not tenant_id:
            return 400, {"error": "tenant_id is required"}
        if not isinstance(steps, list) or not steps:
            return 400, {"error": "steps must be a non-empty list"}
        try:
            from datetime import datetime

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
            run = self.queue.start_workflow(
                tenant_id=str(tenant_id),
                steps=steps,
                name=body.get("name"),
                queue_name=str(body.get("queue_name") or "default"),
                max_attempts=int(body.get("max_attempts") or 3),
                visibility_timeout_seconds=int(
                    body.get("visibility_timeout_seconds") or 30
                ),
                max_compensate_retries=int(
                    body["max_compensate_retries"]
                    if body.get("max_compensate_retries") is not None
                    else 5
                ),
                ttl_seconds=None if ttl_raw is None else float(ttl_raw),
                expires_at=expires_at,
                expiry_action=body.get("expiry_action"),
            )
        except RateLimitError as exc:
            from job_queue.coordinator.jobs import _limit_error_response

            return _limit_error_response(exc)
        except QueueDrainError as exc:
            return 409, {
                "error": str(exc),
                "code": "queue_draining",
                "tenant_id": exc.tenant_id,
                "queue_name": exc.queue_name,
            }
        except (TypeError, ValueError) as exc:
            return 400, {"error": str(exc)}
        steps_out = self.queue.list_workflow_steps(run.id, tenant_id=run.tenant_id)
        return 201, {
            "run": _run_to_dict(run),
            "steps": [_step_to_dict(s) for s in steps_out],
        }

    def _get_workflow(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) < 3:
            return 400, {"error": "invalid workflow path"}
        try:
            rid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid workflow id"}
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            run = self.queue.get_workflow(rid, tenant_id=str(tenant))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        if run is None:
            return 404, {"error": "not found"}
        steps = self.queue.list_workflow_steps(rid, tenant_id=str(tenant))
        return 200, {
            "run": _run_to_dict(run),
            "steps": [_step_to_dict(s) for s in steps],
        }

    def _list_workflow_steps(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # /v1/workflows/{id}/steps
        if len(parts) < 4:
            return 400, {"error": "invalid workflow steps path"}
        try:
            rid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid workflow id"}
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            steps = self.queue.list_workflow_steps(rid, tenant_id=str(tenant))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "steps": [_step_to_dict(s) for s in steps],
            "count": len(steps),
            "tenant_id": str(tenant),
        }

    def _process_workflow_compensation(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            n = self.queue.process_workflow_compensation(
                tenant_id=(
                    str(body["tenant_id"]) if body.get("tenant_id") else None
                ),
                limit=int(body.get("limit") or 50),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"repaired": n}

    def _cancel_workflow(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # /v1/workflows/{id}/cancel
        if len(parts) < 4:
            return 400, {"error": "invalid workflow cancel path"}
        try:
            rid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid workflow id"}
        tenant = body.get("tenant_id")
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            out = self.queue.cancel_workflow(
                rid,
                tenant_id=str(tenant),
                reason=body.get("reason"),
                release_in_flight=bool(body.get("release_in_flight", False)),
                compensate=(
                    bool(body["compensate"])
                    if body.get("compensate") is not None
                    else True
                ),
            )
        except CancelError as exc:
            return 409, {"error": str(exc)}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        run = out["run"]
        return 200, {
            "run": _run_to_dict(run),
            "cancelled_pending": out["cancelled_pending"],
            "released_in_flight": out["released_in_flight"],
            "left_in_flight": out["left_in_flight"],
            "release_in_flight": out["release_in_flight"],
            "compensate": out.get("compensate", True),
            "reason": out["reason"],
        }

    def _send_workflow_signal(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # /v1/workflows/{id}/signals
        if len(parts) < 4:
            return 400, {"error": "invalid workflow signal path"}
        try:
            rid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid workflow id"}
        tenant = body.get("tenant_id")
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        name = body.get("name")
        if name is None or not str(name).strip():
            return 400, {"error": "name is required"}
        payload = body.get("payload")
        if payload is not None and not isinstance(payload, dict):
            return 400, {"error": "payload must be an object"}
        try:
            out = self.queue.send_workflow_signal(
                rid,
                tenant_id=str(tenant),
                name=str(name),
                payload=payload,
            )
        except ValueError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        code = 201 if out.get("created") else 200
        return code, {
            "signal": _signal_to_dict(out["signal"]),
            "created": bool(out.get("created")),
            "unblocked": int(out.get("unblocked") or 0),
        }

    def _list_workflow_signals(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) < 4:
            return 400, {"error": "invalid workflow signals path"}
        try:
            rid = UUID(parts[2])
        except ValueError:
            return 400, {"error": "invalid workflow id"}
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            signals = self.queue.list_workflow_signals(rid, tenant_id=str(tenant))
        except ValueError as exc:
            msg = str(exc)
            if "not found" in msg:
                return 404, {"error": msg}
            return 400, {"error": msg}
        return 200, {
            "signals": [_signal_to_dict(s) for s in signals],
            "count": len(signals),
            "tenant_id": str(tenant),
        }

    def _process_workflow_signal_timeouts(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            n = self.queue.process_workflow_signal_timeouts(
                tenant_id=(
                    str(body["tenant_id"]) if body.get("tenant_id") else None
                ),
                limit=int(body.get("limit") or 50),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"timed_out": n}

    def _process_workflow_timers(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            n = self.queue.process_workflow_timers(
                tenant_id=(
                    str(body["tenant_id"]) if body.get("tenant_id") else None
                ),
                limit=int(body.get("limit") or 50),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"fired": n}

    def _process_workflow_activities(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            n = self.queue.process_workflow_activities(
                tenant_id=(
                    str(body["tenant_id"]) if body.get("tenant_id") else None
                ),
                limit=int(body.get("limit") or 50),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"timed_out": n}

    def _start_workflow_activity(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._activity_lease_action(path, body, "start")

    def _heartbeat_workflow_activity(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._activity_lease_action(path, body, "heartbeat")

    def _complete_workflow_activity(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        return self._activity_lease_action(path, body, "complete")

    def _activity_lease_action(
        self, path: str, body: dict[str, Any], action: str
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / activity / start|heartbeat|complete
        if len(parts) != 5:
            return 400, {"error": "invalid activity path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        worker_id = body.get("worker_id")
        lease_token = body.get("lease_token")
        if not tenant_id or not worker_id or not lease_token:
            return 400, {
                "error": "tenant_id, worker_id, lease_token required"
            }
        try:
            token = UUID(str(lease_token))
            jid = UUID(parts[2])
            if action == "start":
                act = self.queue.start_workflow_activity(
                    jid,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                )
            elif action == "heartbeat":
                act = self.queue.heartbeat_workflow_activity(
                    jid,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                )
            else:
                result = body.get("result")
                if result is not None and not isinstance(result, dict):
                    return 400, {"error": "result must be an object"}
                act = self.queue.complete_workflow_activity(
                    jid,
                    tenant_id=str(tenant_id),
                    worker_id=str(worker_id),
                    lease_token=token,
                    result=result,
                )
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"activity": _activity_to_dict(act)}

    def _get_workflow_activity(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / activity
        if len(parts) != 4:
            return 400, {"error": "invalid activity path"}
        if not _UUID_RE.match(parts[2]):
            return 400, {"error": "invalid job id"}
        tenant = (query.get("tenant_id") or [None])[0]
        if not tenant:
            return 400, {"error": "tenant_id is required"}
        try:
            act = self.queue.get_workflow_activity(
                UUID(parts[2]), tenant_id=str(tenant)
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        if act is None:
            return 404, {"error": "activity not found"}
        return 200, {"activity": _activity_to_dict(act)}
