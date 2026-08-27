"""CoordinatorClient workflow: side effects, approvals, spawn/join, sagas."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import Job, SpawnChildrenResult


class ClientWorkflowMixin:
    def start_workflow(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/workflows", dict(kwargs))
        assert payload is not None
        return payload

    def get_workflow(self, run_id: str | UUID, *, tenant_id: str) -> dict[str, Any]:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/workflows/{run_id}?{q}")
        assert payload is not None
        return payload

    def list_workflow_steps(
        self, run_id: str | UUID, *, tenant_id: str
    ) -> dict[str, Any]:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/workflows/{run_id}/steps?{q}")
        assert payload is not None
        return payload

    def process_workflow_compensation(
        self, *, tenant_id: str | None = None, limit: int = 50
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request(
            "POST", "/v1/workflows/process-compensation", body
        )
        assert payload is not None
        return payload

    def send_workflow_signal(
        self,
        run_id: str | UUID,
        *,
        tenant_id: str,
        name: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"tenant_id": tenant_id, "name": name}
        if payload is not None:
            body["payload"] = payload
        payload_out = self._request(
            "POST", f"/v1/workflows/{run_id}/signals", body
        )
        assert payload_out is not None
        return payload_out

    def list_workflow_signals(
        self, run_id: str | UUID, *, tenant_id: str
    ) -> dict[str, Any]:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/workflows/{run_id}/signals?{q}")
        assert payload is not None
        return payload

    def process_workflow_signal_timeouts(
        self, *, tenant_id: str | None = None, limit: int = 50
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request(
            "POST", "/v1/workflows/process-signal-timeouts", body
        )
        assert payload is not None
        return payload

    def process_workflow_timers(
        self, *, tenant_id: str | None = None, limit: int = 50
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/workflows/process-timers", body)
        assert payload is not None
        return payload

    def cancel_workflow(
        self,
        run_id: str | UUID,
        *,
        tenant_id: str,
        reason: str | None = None,
        release_in_flight: bool = False,
        compensate: bool = True,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "release_in_flight": release_in_flight,
            "compensate": compensate,
        }
        if reason is not None:
            body["reason"] = reason
        payload = self._request(
            "POST", f"/v1/workflows/{run_id}/cancel", body
        )
        assert payload is not None
        return payload

    def begin_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        effect_key: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "effect_key": effect_key,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        payload = self._request("POST", f"/v1/jobs/{job_id}/side-effects/begin", body)
        assert payload is not None
        return payload.get("effect") or {}

    def complete_side_effect(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        effect_key: str,
        result: Any = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "effect_key": effect_key,
        }
        if result is not None:
            body["result"] = result
        payload = self._request("POST", f"/v1/jobs/{job_id}/side-effects", body)
        assert payload is not None
        return payload.get("effect") or {}

    def side_effect_done(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        effect_key: str,
    ) -> bool:
        from urllib.parse import quote

        payload = self._request(
            "GET",
            f"/v1/jobs/{job_id}/side-effects/{quote(effect_key, safe='')}?tenant_id={tenant_id}",
        )
        assert payload is not None
        return bool(payload.get("done"))

    def list_side_effects(
        self, job_id: UUID, *, tenant_id: str
    ) -> list[dict[str, Any]]:
        payload = self._request(
            "GET", f"/v1/jobs/{job_id}/side-effects?tenant_id={tenant_id}"
        )
        assert payload is not None
        return list(payload.get("effects") or [])

    def request_approval(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        message: str | None = None,
        timeout_seconds: float | int = 3600,
        progress: Any = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "timeout_seconds": timeout_seconds,
        }
        if message is not None:
            body["message"] = message
        if progress is not None:
            body["progress"] = progress
        payload = self._request("POST", f"/v1/jobs/{job_id}/request-approval", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def approve(self, job_id: UUID | str, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/approve",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def reject(
        self,
        job_id: UUID | str,
        *,
        tenant_id: str,
        reason: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if reason is not None:
            body["reason"] = reason
        payload = self._request("POST", f"/v1/jobs/{job_id}/reject", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def process_approval_timeouts(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> list[Job]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/approvals/process-timeouts", body)
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def spawn_children(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        children: list[dict[str, Any]],
        timeout_seconds: float | int | None = None,
        progress: Any = None,
        wait: bool = True,
    ) -> SpawnChildrenResult:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "children": children,
            "wait": wait,
        }
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if progress is not None:
            body["progress"] = progress
        payload = self._request("POST", f"/v1/jobs/{job_id}/spawn-children", body)
        assert payload is not None
        parent = job_from_dict(payload["parent"])
        kids = tuple(job_from_dict(c) for c in (payload.get("children") or []))
        return SpawnChildrenResult(parent=parent, children=kids)

    def list_children(self, job_id: UUID | str, *, tenant_id: str) -> list[Job]:
        payload = self._request(
            "GET", f"/v1/jobs/{job_id}/children?tenant_id={tenant_id}"
        )
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("children") or [])]

    def process_await_children_timeouts(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> list[Job]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/await-children/process-timeouts", body)
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]
