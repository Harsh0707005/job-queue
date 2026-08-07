"""Coordinator multi-step workflow / saga API."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.models import WorkflowRun, WorkflowStepRun
from job_queue.queue._common import CancelError


def _run_to_dict(r: WorkflowRun) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "tenant_id": r.tenant_id,
        "name": r.name,
        "status": r.status,
        "steps_def": r.steps_def,
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
            )
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
