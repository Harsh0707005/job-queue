"""client."""
from __future__ import annotations

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import BulkCancelResult, DrainResult, Job, SpawnChildrenResult
from job_queue.queue import ApprovalRequested, ChildrenAwaited, LeaseError


class _ActiveRemoteJob:
    __slots__ = ("job", "lease_lost", "stop_hb")

    def __init__(
        self,
        job: Job,
        lease_lost: threading.Event,
        stop_hb: threading.Event,
    ) -> None:
        self.job = job
        self.lease_lost = lease_lost
        self.stop_hb = stop_hb


class CoordinatorClient:
    """HTTP client for one or more fleet coordinators (failover / RR)."""

    def __init__(
        self,
        base_url: str | list[str],
        *,
        timeout: float = 30.0,
        api_token: str | None = None,
    ) -> None:
        if isinstance(base_url, str):
            urls = [u.strip() for u in base_url.split(",") if u.strip()]
        else:
            urls = list(base_url)
        if not urls:
            raise ValueError("at least one coordinator URL is required")
        self.base_urls = [u.rstrip("/") for u in urls]
        self.timeout = timeout
        # Prefer explicit token; else env (same keys as the coordinator).
        if api_token is not None:
            self.api_token = api_token or None
        else:
            import os

            self.api_token = (
                os.environ.get("JOB_QUEUE_API_TOKEN")
                or os.environ.get("COORDINATOR_TOKEN")
                or None
            )
        self._rr = 0
        self._lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return self.base_urls[0]

    def _next_order(self) -> list[str]:
        with self._lock:
            n = len(self.base_urls)
            start = self._rr % n
            self._rr += 1
        return self.base_urls[start:] + self.base_urls[:start]

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        allow_204: bool = False,
    ) -> dict[str, Any] | None:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, default=str).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        last_err: Exception | None = None
        for base in self._next_order():
            url = f"{base}{path}"
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    code = int(resp.getcode())
                    raw = resp.read()
                    if code == 204 or (allow_204 and not raw):
                        return None
                    if not raw:
                        return {}
                    return json.loads(raw.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read() if exc.fp else b""
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {"error": raw.decode("utf-8", errors="replace") or str(exc)}
                if exc.code == 401:
                    raise RuntimeError(
                        f"coordinator unauthorized for {method} {path}: {payload}"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "lease_error":
                    raise LeaseError(payload.get("error") or "lease error") from exc
                if exc.code == 409 and payload.get("code") == "approval_error":
                    from job_queue.queue import ApprovalError

                    raise ApprovalError(
                        payload.get("error") or "approval error"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "cancel_error":
                    from job_queue.queue import CancelError

                    raise CancelError(
                        payload.get("error") or "cancel error"
                    ) from exc
                if exc.code == 409 and payload.get("code") == "replay_error":
                    from job_queue.queue import ReplayError

                    raise ReplayError(
                        payload.get("error") or "replay error"
                    ) from exc
                if int(exc.code) == 404:
                    return {"error": payload.get("error") or "not found", "_http": 404}
                # 5xx → try next coordinator; 4xx is definitive.
                if 500 <= int(exc.code) < 600:
                    last_err = RuntimeError(
                        f"coordinator {method} {path} -> {exc.code}: {payload}"
                    )
                    continue
                raise RuntimeError(
                    f"coordinator {method} {path} -> {exc.code}: {payload}"
                ) from exc
            except urllib.error.URLError as exc:
                last_err = RuntimeError(f"coordinator {base} unreachable: {exc}")
                continue
        raise RuntimeError(f"all coordinators failed for {method} {path}: {last_err}")

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health") or {}

    def wait_healthy(self, *, timeout: float = 120.0, interval: float = 1.0) -> None:
        deadline = time.time() + timeout
        last: Exception | None = None
        while time.time() < deadline:
            try:
                h = self.health()
                if h.get("ok"):
                    return
            except Exception as exc:  # noqa: BLE001
                last = exc
            time.sleep(interval)
        raise RuntimeError(f"coordinator not healthy within {timeout}s: {last}")

    def lease(
        self,
        *,
        worker_id: str,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> Job | None:
        jobs = self.lease_batch(
            worker_id=worker_id,
            limit=1,
            tenant_id=tenant_id,
            queue_name=queue_name,
            visibility_timeout_seconds=visibility_timeout_seconds,
        )
        return jobs[0] if jobs else None

    def lease_batch(
        self,
        *,
        worker_id: str,
        limit: int = 1,
        tenant_id: str | None = None,
        queue_name: str = "default",
        visibility_timeout_seconds: int | None = None,
    ) -> list[Job]:
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "queue_name": queue_name,
            "limit": limit,
        }
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        if visibility_timeout_seconds is not None:
            body["visibility_timeout_seconds"] = visibility_timeout_seconds
        last_err: Exception | None = None
        for base in self._next_order():
            url = f"{base}/v1/lease"
            data = json.dumps(body).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            if self.api_token:
                headers["Authorization"] = f"Bearer {self.api_token}"
            req = urllib.request.Request(
                url,
                data=data,
                headers=headers,
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    if int(resp.getcode()) == 204:
                        return []
                    raw = resp.read()
                    if not raw:
                        return []
                    payload = json.loads(raw.decode("utf-8"))
                    jobs_raw = payload.get("jobs")
                    if jobs_raw is None and payload.get("job") is not None:
                        jobs_raw = [payload["job"]]
                    return [job_from_dict(j) for j in (jobs_raw or [])]
            except urllib.error.HTTPError as exc:
                if 500 <= int(exc.code) < 600:
                    last_err = exc
                    continue
                raw = exc.read() if exc.fp else b""
                raise RuntimeError(
                    f"lease failed {exc.code}: {raw.decode('utf-8', errors='replace')}"
                ) from exc
            except urllib.error.URLError as exc:
                last_err = exc
                continue
        raise RuntimeError(f"lease failed on all coordinators: {last_err}")

    def heartbeat(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        extend_by_seconds: int | None = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
        }
        if extend_by_seconds is not None:
            body["extend_by_seconds"] = extend_by_seconds
        payload = self._request("POST", f"/v1/jobs/{job_id}/heartbeat", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def ack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        result: Any = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
        }
        if result is not None:
            body["result"] = result
        payload = self._request("POST", f"/v1/jobs/{job_id}/ack", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def nack(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        error: str | None = None,
        requeue: bool = True,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "requeue": requeue,
        }
        if error is not None:
            body["error"] = error
        payload = self._request("POST", f"/v1/jobs/{job_id}/nack", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def release(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        reason: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
        }
        if reason is not None:
            body["reason"] = reason
        payload = self._request("POST", f"/v1/jobs/{job_id}/release", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def checkpoint(
        self,
        job_id: UUID,
        *,
        tenant_id: str,
        worker_id: str,
        lease_token: UUID,
        progress: Any,
    ) -> Job:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "worker_id": worker_id,
            "lease_token": str(lease_token),
            "progress": progress,
        }
        payload = self._request("POST", f"/v1/jobs/{job_id}/checkpoint", body)
        assert payload is not None
        return job_from_dict(payload["job"])

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
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/side-effects/begin", body
        )
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
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/side-effects", body
        )
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
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/request-approval", body
        )
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
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/spawn-children", body
        )
        assert payload is not None
        parent = job_from_dict(payload["parent"])
        kids = tuple(job_from_dict(c) for c in (payload.get("children") or []))
        return SpawnChildrenResult(parent=parent, children=kids)

    def list_children(
        self, job_id: UUID | str, *, tenant_id: str
    ) -> list[Job]:
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
        payload = self._request(
            "POST", "/v1/await-children/process-timeouts", body
        )
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def get_job(self, job_id: UUID | str, *, tenant_id: str) -> Job | None:
        payload = self._request(
            "GET",
            f"/v1/jobs/{job_id}?tenant_id={tenant_id}",
        )
        if payload is None:
            return None
        if payload.get("_http") == 404 or (
            payload.get("error") and "job" not in payload
        ):
            return None
        return job_from_dict(payload["job"])

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
        payload = self._request(
            "POST", f"/v1/webhooks/{delivery_id}/finalize", body
        )
        assert payload is not None
        return payload

    def list_jobs(
        self,
        *,
        tenant_id: str,
        labels: list[str] | None = None,
        status: str | list[str] | None = None,
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
        if labels:
            for lab in labels:
                q.append(("label", lab))
        if status is not None:
            if isinstance(status, str):
                q.append(("status", status))
            else:
                for s in status:
                    q.append(("status", s))
        payload = self._request("GET", f"/v1/jobs?{urlencode(q)}")
        assert payload is not None
        return [job_from_dict(j) for j in (payload.get("jobs") or [])]

    def cancel_matching(
        self,
        *,
        tenant_id: str,
        labels: list[str] | None = None,
        status: str | list[str] | None = None,
        queue_name: str | None = None,
        batch_size: int = 500,
        max_jobs: int | None = None,
        limit: int | None = None,
    ) -> BulkCancelResult:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "batch_size": batch_size,
        }
        if labels is not None:
            body["labels"] = labels
        if status is not None:
            body["status"] = status
        if queue_name is not None:
            body["queue_name"] = queue_name
        cap = max_jobs if max_jobs is not None else limit
        if cap is not None:
            body["max_jobs"] = cap
        payload = self._request("POST", "/v1/jobs/cancel-matching", body)
        assert payload is not None
        jobs = tuple(job_from_dict(j) for j in (payload.get("jobs") or []))
        return BulkCancelResult(
            tenant_id=str(payload.get("tenant_id") or tenant_id),
            cancelled=int(payload.get("cancelled") or len(jobs)),
            jobs=jobs,
            complete=bool(payload.get("complete", True)),
            remaining_estimate=int(payload.get("remaining_estimate") or 0),
            batches=int(payload.get("batches") or 1),
        )

    def cancel_job(self, job_id: str | UUID, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/cancel",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

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
        payload = self._request(
            "POST", f"/v1/dead-letters/{job_id}/replay", body
        )
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


