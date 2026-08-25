"""JobQueue jobs query domain: get, list, count, results."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from job_queue.models import (
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _JOB_COLUMNS,
    _normalize_labels,
    _normalize_status_filter,
)


class JobsQueryMixin:
    def list_jobs(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        cloned_from_job_id: UUID | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs for *tenant_id*, optionally filtered by labels and status.

        *labels*: job must include **all** listed labels (AND). Order is
        newest ``created_at`` first. *cloned_from_job_id* lists re-runs of
        that source job.
        """
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")
        label_list = _normalize_labels(labels)
        status_list = _normalize_status_filter(status)

        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status_list:
            clauses.append("status = ANY(%s)")
            params.append(status_list)
        if label_list:
            clauses.append("labels @> %s::text[]")
            params.append(label_list)
        if cloned_from_job_id is not None:
            clauses.append("cloned_from_job_id = %s")
            params.append(cloned_from_job_id)
        params.extend([limit, offset])
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE {where}
                ORDER BY created_at DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            ).fetchall()
            return [self._job_readable(r) for r in rows]

    def get(self, job_id: UUID, *, tenant_id: str) -> Job | None:
        """Fetch a job by id for *tenant_id* only (cross-tenant → None)."""
        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            row = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE id = %s AND tenant_id = %s
                """,
                (job_id, tenant_id),
            ).fetchone()
            return self._job_readable(row, conn=conn) if row else None

    def get_result(self, job_id: UUID, *, tenant_id: str) -> Any | None:
        """Return the stashed ack *result* for a job, or None if missing/unset.

        Cross-tenant and unknown ids return None. Incomplete jobs typically
        have ``result is None`` until a successful ``ack(..., result=...)``.
        """
        job = self.get(job_id, tenant_id=tenant_id)
        if job is None:
            return None
        return job.result

    def count(
        self,
        *,
        tenant_id: str,
        queue_name: str | None = None,
        status: JobStatus | None = None,
    ) -> int:
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if status is not None:
            clauses.append("status = %s")
            params.append(status.value)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE {where}",
                params,
            ).fetchone()
            assert row is not None
            return int(row["n"])
