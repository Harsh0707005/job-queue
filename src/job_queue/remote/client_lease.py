"""CoordinatorClient lease ops: lease/batch, heartbeat, ack/nack/release, checkpoint."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import Job


class ClientLeaseMixin:
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
