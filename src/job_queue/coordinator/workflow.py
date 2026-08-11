"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _UUID_RE,
)

from typing import Any
from uuid import UUID

from job_queue.codec import (
    job_to_dict,
    job_side_effect_to_dict,
)
from job_queue.queue import (
    LeaseError,
)


class WorkflowApiMixin:
    def _approve_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from job_queue.queue import ApprovalError

        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id required"}
        try:
            job = self.queue.approve(UUID(job_id_s), tenant_id=str(tenant_id))
        except ApprovalError as exc:
            return 409, {"error": str(exc), "code": "approval_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _reject_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from job_queue.queue import ApprovalError

        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        if not tenant_id:
            return 400, {"error": "tenant_id required"}
        try:
            job = self.queue.reject(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                reason=body.get("reason"),
            )
        except ApprovalError as exc:
            return 409, {"error": str(exc), "code": "approval_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"job": job_to_dict(job)}

    def _process_approval_timeouts(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            jobs = self.queue.process_approval_timeouts(
                tenant_id=(str(body["tenant_id"]) if body.get("tenant_id") else None),
                limit=int(body.get("limit") or 100),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "count": len(jobs),
            "jobs": [job_to_dict(j) for j in jobs],
        }

    def _list_approvals(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        limit = int((query.get("limit") or ["100"])[0])
        offset = int((query.get("offset") or ["0"])[0])
        try:
            jobs = self.queue.list_awaiting_approvals(
                tenant_id=str(tenants[0]),
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"jobs": [job_to_dict(j) for j in jobs], "count": len(jobs)}

    def _spawn_children(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from job_queue.queue import LeaseError, RateLimitError, SpawnError

        parts = path.strip("/").split("/")
        # /v1/jobs/{id}/spawn-children
        if len(parts) != 4 or parts[0] != "v1" or parts[1] != "jobs":
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        worker_id = body.get("worker_id")
        lease_token = body.get("lease_token")
        children = body.get("children")
        if not tenant_id or not worker_id or not lease_token:
            return 400, {"error": "tenant_id, worker_id, lease_token required"}
        if not isinstance(children, list) or not children:
            return 400, {"error": "children must be a non-empty list"}
        try:
            token = UUID(str(lease_token))
        except ValueError:
            return 400, {"error": "invalid lease_token"}
        timeout = body.get("timeout_seconds")
        try:
            result = self.queue.spawn_children(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                worker_id=str(worker_id),
                lease_token=token,
                children=children,
                timeout_seconds=(float(timeout) if timeout is not None else None),
                progress=body.get("progress"),
                wait=bool(body.get("wait", True)),
            )
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except SpawnError as exc:
            return 409, {"error": str(exc), "code": "spawn_error"}
        except RateLimitError as exc:
            from job_queue.coordinator.jobs import _limit_error_response

            return _limit_error_response(exc)
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "parent": job_to_dict(result.parent),
            "children": [job_to_dict(c) for c in result.children],
        }

    def _list_children(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # /v1/jobs/{id}/children
        if len(parts) != 4:
            return 400, {"error": "invalid job path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        limit = int((query.get("limit") or ["500"])[0])
        try:
            kids = self.queue.list_children(
                UUID(job_id_s),
                tenant_id=str(tenants[0]),
                limit=limit,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "children": [job_to_dict(j) for j in kids],
            "count": len(kids),
        }

    def _process_await_children_timeouts(
        self, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        try:
            jobs = self.queue.process_await_children_timeouts(
                tenant_id=(str(body["tenant_id"]) if body.get("tenant_id") else None),
                limit=int(body.get("limit") or 100),
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "count": len(jobs),
            "jobs": [job_to_dict(j) for j in jobs],
        }

    def _begin_side_effect(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / side-effects / begin
        if len(parts) != 5 or parts[4] != "begin":
            return 400, {"error": "invalid side-effect begin path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        worker_id = body.get("worker_id")
        lease_token = body.get("lease_token")
        effect_key = body.get("effect_key") or body.get("key")
        if not tenant_id or not worker_id or not lease_token or not effect_key:
            return 400, {
                "error": "tenant_id, worker_id, lease_token, effect_key required"
            }
        try:
            token = UUID(str(lease_token))
            eff = self.queue.begin_side_effect(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                worker_id=str(worker_id),
                lease_token=token,
                effect_key=str(effect_key),
                idempotency_key=(
                    str(body["idempotency_key"])
                    if body.get("idempotency_key") is not None
                    else None
                ),
            )
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"effect": job_side_effect_to_dict(eff)}

    def _complete_side_effect(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / side-effects
        if len(parts) != 4:
            return 400, {"error": "invalid side-effect path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        worker_id = body.get("worker_id")
        lease_token = body.get("lease_token")
        effect_key = body.get("effect_key") or body.get("key")
        if not tenant_id or not worker_id or not lease_token or not effect_key:
            return 400, {
                "error": "tenant_id, worker_id, lease_token, effect_key required"
            }
        try:
            token = UUID(str(lease_token))
            eff = self.queue.complete_side_effect(
                UUID(job_id_s),
                tenant_id=str(tenant_id),
                worker_id=str(worker_id),
                lease_token=token,
                effect_key=str(effect_key),
                result=body.get("result"),
            )
        except LeaseError as exc:
            return 409, {"error": str(exc), "code": "lease_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {"effect": job_side_effect_to_dict(eff)}

    def _get_side_effect(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / side-effects / {key}
        if len(parts) != 5:
            return 400, {"error": "invalid side-effect path"}
        job_id_s = parts[2]
        effect_key = parts[4]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        try:
            from urllib.parse import unquote

            key = unquote(effect_key)
            done = self.queue.side_effect_done(
                UUID(job_id_s),
                tenant_id=str(tenants[0]),
                effect_key=key,
            )
            eff = self.queue.get_side_effect(
                UUID(job_id_s),
                tenant_id=str(tenants[0]),
                effect_key=key,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "done": done,
            "effect": job_side_effect_to_dict(eff) if eff else None,
        }

    def _list_side_effects(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid side-effect path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        try:
            effects = self.queue.list_side_effects(
                UUID(job_id_s), tenant_id=str(tenants[0])
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "effects": [job_side_effect_to_dict(e) for e in effects],
            "count": len(effects),
        }
