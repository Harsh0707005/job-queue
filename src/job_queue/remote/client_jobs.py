"""CoordinatorClient jobs: get, list, cancel."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from job_queue.codec import job_from_dict
from job_queue.models import (
    BulkCancelResult,
    BulkFreezeResult,
    DagEnqueueResult,
    Job,
)


class ClientJobsMixin:
    def enqueue(self, **kwargs: Any) -> Job:
        payload = self._request("POST", "/v1/jobs", kwargs)
        assert payload is not None
        return job_from_dict(payload["job"])

    def enqueue_dag(
        self,
        *,
        tenant_id: str,
        jobs: list[dict[str, Any]],
        batch_key: str,
        queue_name: str = "default",
    ) -> DagEnqueueResult:
        payload = self._request(
            "POST",
            "/v1/jobs/dag",
            {
                "tenant_id": tenant_id,
                "jobs": jobs,
                "batch_key": batch_key,
                "queue_name": queue_name,
            },
        )
        assert payload is not None
        jobs_out = [job_from_dict(j) for j in (payload.get("jobs") or [])]
        return DagEnqueueResult(
            batch_key=str(payload.get("batch_key") or batch_key),
            batch_id=UUID(str(payload["batch_id"])),
            jobs=tuple(jobs_out),
            refs=tuple(str(r) for r in (payload.get("refs") or [])),
            created=bool(payload.get("created")),
        )

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

    def list_jobs(
        self,
        *,
        tenant_id: str,
        labels: list[str] | None = None,
        status: str | list[str] | None = None,
        queue_name: str | None = None,
        cloned_from_job_id: UUID | str | None = None,
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
        if cloned_from_job_id is not None:
            q.append(("cloned_from_job_id", str(cloned_from_job_id)))
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

    def freeze_matching(
        self,
        *,
        tenant_id: str,
        labels: list[str] | None = None,
        status: str | list[str] | None = None,
        queue_name: str | None = None,
        reason: str | None = None,
        batch_size: int = 500,
        max_jobs: int | None = None,
    ) -> BulkFreezeResult:
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
        if reason is not None:
            body["reason"] = reason
        if max_jobs is not None:
            body["max_jobs"] = max_jobs
        payload = self._request("POST", "/v1/jobs/freeze-matching", body)
        assert payload is not None
        return _bulk_freeze_from_payload(payload, tenant_id)

    def unfreeze_matching(
        self,
        *,
        tenant_id: str,
        labels: list[str] | None = None,
        queue_name: str | None = None,
        batch_size: int = 500,
        max_jobs: int | None = None,
    ) -> BulkFreezeResult:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "batch_size": batch_size,
        }
        if labels is not None:
            body["labels"] = labels
        if queue_name is not None:
            body["queue_name"] = queue_name
        if max_jobs is not None:
            body["max_jobs"] = max_jobs
        payload = self._request("POST", "/v1/jobs/unfreeze-matching", body)
        assert payload is not None
        return _bulk_freeze_from_payload(payload, tenant_id)

    def freeze_job(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        reason: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if reason is not None:
            body["reason"] = reason
        payload = self._request("POST", f"/v1/jobs/{job_id}/freeze", body)
        assert payload is not None
        return job_from_dict(payload["job"])

    def unfreeze_job(self, job_id: str | UUID, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/unfreeze",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def dead_letter_frozen(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        error: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if error is not None:
            body["error"] = error
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/freeze/dead", body
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def set_job_prefer_last_worker(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        enabled: bool,
    ) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/worker-affinity",
            {"tenant_id": tenant_id, "enabled": enabled},
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def set_job_priority(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        priority: int | None = None,
        delta: int | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if priority is not None:
            body["priority"] = priority
        if delta is not None:
            body["delta"] = delta
        payload = self._request(
            "POST", f"/v1/jobs/{job_id}/priority", body
        )
        assert payload is not None
        return job_from_dict(payload["job"])

    def clone_job(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        payload: dict[str, Any] | None = None,
        available_at: str | None = None,
        delay_seconds: float | int | None = None,
        dedupe_key: str | None = None,
    ) -> Job:
        body: dict[str, Any] = {"tenant_id": tenant_id}
        if payload is not None:
            body["payload"] = payload
        if available_at is not None:
            body["available_at"] = available_at
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        if dedupe_key is not None:
            body["dedupe_key"] = dedupe_key
        result = self._request("POST", f"/v1/jobs/{job_id}/clone", body)
        assert result is not None
        return job_from_dict(result["job"])

    def move_job_queue(
        self,
        job_id: str | UUID,
        *,
        tenant_id: str,
        queue_name: str,
    ) -> Job:
        result = self._request(
            "POST",
            f"/v1/jobs/{job_id}/queue",
            {"tenant_id": tenant_id, "queue_name": queue_name},
        )
        assert result is not None
        return job_from_dict(result["job"])

    def cancel_job(self, job_id: str | UUID, *, tenant_id: str) -> Job:
        payload = self._request(
            "POST",
            f"/v1/jobs/{job_id}/cancel",
            {"tenant_id": tenant_id},
        )
        assert payload is not None
        return job_from_dict(payload["job"])


def _bulk_freeze_from_payload(
    payload: dict[str, Any], tenant_id: str
) -> BulkFreezeResult:
    jobs = tuple(job_from_dict(j) for j in (payload.get("jobs") or []))
    return BulkFreezeResult(
        tenant_id=str(payload.get("tenant_id") or tenant_id),
        action=str(payload.get("action") or "freeze"),
        count=int(payload.get("count") or len(jobs)),
        jobs=jobs,
        complete=bool(payload.get("complete", True)),
        remaining_estimate=int(payload.get("remaining_estimate") or 0),
        batches=int(payload.get("batches") or 1),
    )
