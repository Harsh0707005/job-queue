"""CoordinatorClient admin: tenants, schedules, webhooks, dead letters, retention, stats."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import Job


class ClientAdminMixin:
    def process_due_schedules(
        self, *, tenant_id: str | None = None, limit: int = 100
    ) -> int:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/schedules/process-due", body)
        assert payload is not None
        return int(payload.get("count") or 0)

    def claim_webhooks(
        self,
        *,
        limit: int = 50,
        http_timeout_seconds: float = 10.0,
        claim_seconds: float | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "limit": limit,
            "http_timeout_seconds": http_timeout_seconds,
        }
        if claim_seconds is not None:
            body["claim_seconds"] = claim_seconds
        payload = self._request("POST", "/v1/webhooks/claim", body)
        assert payload is not None
        return payload

    def finalize_webhook(
        self,
        delivery_id: str | UUID,
        *,
        status_code: int | None = None,
        error: str | None = None,
        endpoint_missing: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"endpoint_missing": endpoint_missing}
        if status_code is not None:
            body["status_code"] = status_code
        if error is not None:
            body["error"] = error
        payload = self._request("POST", f"/v1/webhooks/{delivery_id}/finalize", body)
        assert payload is not None
        return payload

    def pause_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/pause", {})
        assert payload is not None
        return payload

    def resume_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/resume", {})
        assert payload is not None
        return payload

    def offboard_tenant(self, tenant_id: str) -> dict[str, Any]:
        payload = self._request("POST", f"/v1/tenants/{tenant_id}/offboard", {})
        assert payload is not None
        return payload

    def merge_tenant(
        self, source_tenant_id: str, target_tenant_id: str
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/tenants/{source_tenant_id}/merge",
            {"target_tenant_id": target_tenant_id},
        )
        assert payload is not None
        return payload

    def create_schedule(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/schedules", kwargs)
        assert payload is not None
        return payload["schedule"]

    def rotate_webhook_secret(
        self,
        endpoint_id: str | UUID,
        *,
        tenant_id: str,
        secret: str | None,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/rotate-secret",
            {"tenant_id": tenant_id, "secret": secret},
        )
        assert payload is not None
        return payload

    def register_webhook(self, **kwargs: Any) -> dict[str, Any]:
        payload = self._request("POST", "/v1/webhooks", kwargs)
        assert payload is not None
        return payload["endpoint"]

    def pause_schedule(self, schedule_id: str, *, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/pause",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return payload["schedule"]

    def resume_schedule(self, schedule_id: str, *, tenant_id: str) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/resume",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return payload["schedule"]

    def delete_schedule(self, schedule_id: str, *, tenant_id: str) -> bool:
        payload = self._request(
            "POST",
            f"/v1/schedules/{schedule_id}/delete",
            {"tenant_id": tenant_id},
        )
        if payload is None:
            return False
        if payload.get("_http") == 404:
            return False
        return bool(payload.get("deleted"))

    def list_schedules(
        self,
        *,
        tenant_id: str,
        enabled_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        from urllib.parse import urlencode

        q = urlencode(
            {
                "tenant_id": tenant_id,
                "enabled_only": "1" if enabled_only else "0",
                "limit": str(limit),
                "offset": str(offset),
            }
        )
        payload = self._request("GET", f"/v1/schedules?{q}")
        assert payload is not None
        return list(payload.get("schedules") or [])

    def get_schedule(
        self, schedule_id: str | UUID, *, tenant_id: str
    ) -> dict[str, Any] | None:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/schedules/{schedule_id}?{q}")
        if payload is None or payload.get("_http") == 404:
            return None
        return payload.get("schedule")

    def list_webhooks(self, *, tenant_id: str) -> list[dict[str, Any]]:
        from urllib.parse import urlencode

        q = urlencode({"tenant_id": tenant_id})
        payload = self._request("GET", f"/v1/webhooks?{q}")
        assert payload is not None
        return list(payload.get("endpoints") or [])

    def set_webhook_enabled(
        self,
        endpoint_id: str | UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> dict[str, Any]:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/set-enabled",
            {"tenant_id": tenant_id, "enabled": enabled},
        )
        assert payload is not None
        return payload["endpoint"]

    def delete_webhook(self, endpoint_id: str | UUID, *, tenant_id: str) -> bool:
        payload = self._request(
            "POST",
            f"/v1/webhooks/{endpoint_id}/delete",
            {"tenant_id": tenant_id},
        )
        if payload is None or payload.get("_http") == 404:
            return False
        return bool(payload.get("deleted"))

    def list_dead_letters(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        from urllib.parse import urlencode

        q: list[tuple[str, str]] = [
            ("tenant_id", tenant_id),
            ("limit", str(limit)),
            ("offset", str(offset)),
        ]
        if queue_name is not None:
            q.append(("queue_name", queue_name))
        payload = self._request("GET", f"/v1/dead-letters?{urlencode(q)}")
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def replay_dead_letter(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "reset_attempts": reset_attempts,
            "clear_error": clear_error,
        }
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        payload = self._request("POST", f"/v1/dead-letters/{job_id}/replay", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def replay_dead_letters(
        self,
        *,
        tenant_id: str,
        job_ids: list[str | UUID] | None = None,
        queue_name: str | None = None,
        limit: int | None = None,
        reset_attempts: bool = True,
        max_attempts: int | None = None,
        delay_seconds: float | int | None = None,
        clear_error: bool = False,
    ) -> list[Job]:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "reset_attempts": reset_attempts,
            "clear_error": clear_error,
        }
        if job_ids is not None:
            body["job_ids"] = [str(j) for j in job_ids]
        if queue_name is not None:
            body["queue_name"] = queue_name
        if limit is not None:
            body["limit"] = limit
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        payload = self._request("POST", "/v1/dead-letters/replay", body)
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def set_retention(
        self,
        *,
        completed_seconds: int | None = None,
        dead_seconds: int | None = None,
        set_completed: bool = True,
        set_dead: bool = True,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if set_completed:
            body["completed_retention_seconds"] = completed_seconds
        if set_dead:
            body["dead_retention_seconds"] = dead_seconds
        payload = self._request("POST", "/v1/admin/retention", body)
        assert payload is not None
        return payload

    def get_retention(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/retention")
        assert payload is not None
        return payload

    def purge_expired_jobs(
        self,
        *,
        limit: int = 500,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"limit": limit}
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        payload = self._request("POST", "/v1/admin/purge-expired-jobs", body)
        assert payload is not None
        return payload

    def backlog(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/backlog")
        assert payload is not None
        return payload

    def completed_by_schedule(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/completed-by-schedule")
        assert payload is not None
        return payload

    def webhook_stats(self) -> dict[str, Any]:
        payload = self._request("GET", "/v1/admin/webhook-stats")
        assert payload is not None
        return payload
