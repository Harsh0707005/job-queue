"""Coordinator API handlers."""

from __future__ import annotations

from job_queue.coordinator.auth import (
    _UUID_RE,
)

from typing import Any
from uuid import UUID

from job_queue.codec import (
    job_to_dict,
    audit_consistency_report_to_dict,
    job_audit_event_to_dict,
)


class AuditApiMixin:
    def _job_audit(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        # /v1/jobs/{uuid}/audit?tenant_id=
        parts = path.strip("/").split("/")
        if len(parts) != 4:
            return 400, {"error": "invalid audit path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        after_seq = int((query.get("after_seq") or ["0"])[0])
        limit = int((query.get("limit") or ["1000"])[0])
        try:
            events = self.queue.list_job_audit(
                UUID(job_id_s),
                tenant_id=str(tenants[0]),
                after_seq=after_seq,
                limit=limit,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "events": [job_audit_event_to_dict(e) for e in events],
            "count": len(events),
        }

    def _job_audit_verify(
        self, path: str, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        # /v1/jobs/{uuid}/audit/verify?tenant_id=
        parts = path.strip("/").split("/")
        if len(parts) != 5:
            return 400, {"error": "invalid audit verify path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenants = query.get("tenant_id") or []
        if not tenants or not tenants[0]:
            return 400, {"error": "tenant_id query param required"}
        report = self.queue.verify_job_audit(UUID(job_id_s), tenant_id=str(tenants[0]))
        return 200, {"report": audit_consistency_report_to_dict(report)}

    def _rebuild_job(
        self, path: str, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        from job_queue.queue import RebuildError

        parts = path.strip("/").split("/")
        # v1 / jobs / {id} / rebuild
        if len(parts) != 4:
            return 400, {"error": "invalid rebuild path"}
        job_id_s = parts[2]
        if not _UUID_RE.match(job_id_s):
            return 400, {"error": "invalid job id"}
        tenant_id = body.get("tenant_id")
        try:
            job = self.queue.rebuild_job_from_audit(
                UUID(job_id_s),
                tenant_id=str(tenant_id) if tenant_id else None,
            )
        except RebuildError as exc:
            return 409, {"error": str(exc), "code": "rebuild_error"}
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 201, {"job": job_to_dict(job)}

    def _rebuild_jobs_batch(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        from job_queue.queue import RebuildError

        raw_ids = body.get("job_ids") or []
        if not isinstance(raw_ids, list) or not raw_ids:
            return 400, {"error": "job_ids required"}
        tenant_id = body.get("tenant_id")
        try:
            ids = [UUID(str(x)) for x in raw_ids]
            jobs = self.queue.rebuild_jobs_from_audit(
                ids,
                tenant_id=str(tenant_id) if tenant_id else None,
            )
        except RebuildError as exc:
            return 409, {
                "error": str(exc),
                "code": "rebuild_error",
                "job_id": str(exc.job_id) if exc.job_id else None,
            }
        except (ValueError, TypeError) as exc:
            return 400, {"error": str(exc)}
        return 201, {
            "jobs": [job_to_dict(j) for j in jobs],
            "count": len(jobs),
        }

    def _list_audit_orphans(
        self, query: dict[str, list[str]]
    ) -> tuple[int, dict[str, Any]]:
        tenant = (query.get("tenant_id") or [None])[0]
        limit = int((query.get("limit") or ["1000"])[0])
        try:
            ids = self.queue.list_orphaned_audit_jobs(
                tenant_id=str(tenant) if tenant else None,
                limit=limit,
            )
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "job_ids": [str(i) for i in ids],
            "count": len(ids),
        }
