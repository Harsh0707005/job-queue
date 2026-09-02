"""Bulk freeze_matching / unfreeze_matching (tenant + labels + filters)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from job_queue.db import transaction
from job_queue.models import BulkFreezeResult, Job, JobStatus
from job_queue.queue._common import (
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _JOB_COLUMNS,
    _normalize_labels,
    _normalize_status_filter,
    _utcnow,
)
from job_queue.queue.jobs_freeze import _FREEZEABLE_VALUES


class JobsFreezeBulkMixin:
    def freeze_matching(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        reason: str | None = None,
        batch_size: int = _CANCEL_MATCHING_BATCH_DEFAULT,
        max_jobs: int | None = None,
        limit: int | None = None,
        lock_wait_seconds: float = 5.0,
        lock_retry_interval: float = 0.05,
    ) -> BulkFreezeResult:
        """Freeze all matching jobs for *tenant_id*. Other tenants keep leasing.

        Same filter shape as :meth:`cancel_matching` (*labels* AND-match,
        optional *status* / *queue_name*). Only pending, leased,
        awaiting_skills, and awaiting_capacity rows are held.
        Already-frozen rows are left as-is.
        """
        return self._sweep_freeze_matching(
            action="freeze",
            tenant_id=tenant_id,
            labels=labels,
            status=status,
            queue_name=queue_name,
            reason=reason,
            batch_size=batch_size,
            max_jobs=max_jobs,
            limit=limit,
            lock_wait_seconds=lock_wait_seconds,
            lock_retry_interval=lock_retry_interval,
        )

    def unfreeze_matching(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        queue_name: str | None = None,
        batch_size: int = _CANCEL_MATCHING_BATCH_DEFAULT,
        max_jobs: int | None = None,
        limit: int | None = None,
        lock_wait_seconds: float = 5.0,
        lock_retry_interval: float = 0.05,
    ) -> BulkFreezeResult:
        """Unfreeze matching frozen jobs so workers can lease them again."""
        return self._sweep_freeze_matching(
            action="unfreeze",
            tenant_id=tenant_id,
            labels=labels,
            status=JobStatus.FROZEN,
            queue_name=queue_name,
            reason=None,
            batch_size=batch_size,
            max_jobs=max_jobs,
            limit=limit,
            lock_wait_seconds=lock_wait_seconds,
            lock_retry_interval=lock_retry_interval,
        )

    def _sweep_freeze_matching(
        self,
        *,
        action: str,
        tenant_id: str,
        labels: Sequence[str] | str | None,
        status: JobStatus | str | Sequence[JobStatus | str] | None,
        queue_name: str | None,
        reason: str | None,
        batch_size: int,
        max_jobs: int | None,
        limit: int | None,
        lock_wait_seconds: float,
        lock_retry_interval: float,
    ) -> BulkFreezeResult:
        import time as _time

        if not tenant_id:
            raise ValueError("tenant_id is required")
        with self._pool.connection() as conn:
            self._assert_tenant_mutable(conn, tenant_id, action=action)
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if lock_wait_seconds < 0:
            raise ValueError("lock_wait_seconds must be >= 0")
        if lock_retry_interval <= 0:
            raise ValueError("lock_retry_interval must be > 0")
        if max_jobs is None and limit is not None:
            max_jobs = int(limit)
        if max_jobs is not None and max_jobs < 1:
            raise ValueError("max_jobs must be >= 1 when set")

        label_list = _normalize_labels(labels)
        if action == "freeze":
            allowed = set(_FREEZEABLE_VALUES)
            default = list(_FREEZEABLE_VALUES)
            noun = "freezeable"
        else:
            allowed = {JobStatus.FROZEN.value}
            default = [JobStatus.FROZEN.value]
            noun = "frozen"
        status_list = _normalize_status_filter(status)
        if status_list is None:
            target_statuses = default
        else:
            target_statuses = [s for s in status_list if s in allowed]
            if not target_statuses:
                raise ValueError(
                    f"status filter has no {noun} statuses "
                    f"(allowed={sorted(allowed)})"
                )
        err = (reason or "frozen by matching filter").strip() or "frozen by matching filter"

        sample: list[Job] = []
        total = 0
        batches = 0
        complete = False
        remaining = 0
        lock_wait_deadline = _time.monotonic() + float(lock_wait_seconds)

        while True:
            if max_jobs is not None and total >= max_jobs:
                remaining = self._count_matching_jobs(
                    tenant_id=tenant_id,
                    labels=label_list,
                    statuses=target_statuses,
                    queue_name=queue_name,
                )
                complete = remaining == 0
                break

            take = batch_size
            if max_jobs is not None:
                take = min(take, max_jobs - total)
            now = _utcnow()
            batch_n = 0
            with transaction(self._pool) as conn:
                clauses = ["tenant_id = %s", "status = ANY(%s)"]
                params: list[Any] = [tenant_id, target_statuses]
                if queue_name is not None:
                    clauses.append("queue_name = %s")
                    params.append(queue_name)
                if label_list:
                    clauses.append("labels @> %s::text[]")
                    params.append(label_list)
                params.append(take)
                where = " AND ".join(clauses)
                rows = conn.execute(
                    f"""
                    SELECT {_JOB_COLUMNS}
                    FROM jobs
                    WHERE {where}
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    tuple(params),
                ).fetchall()
                for row in rows:
                    if action == "freeze":
                        job = self._freeze_locked_row(
                            conn,
                            row=row,
                            tenant_id=tenant_id,
                            now=now,
                            reason=err,
                        )
                    else:
                        job = self._unfreeze_locked_row(
                            conn,
                            row=row,
                            tenant_id=tenant_id,
                            now=now,
                        )
                    if job is not None:
                        batch_n += 1
                        total += 1
                        if len(sample) < _CANCEL_MATCHING_RETURN_CAP:
                            sample.append(job)

            batches += 1
            if batch_n > 0:
                continue
            remaining = self._count_matching_jobs(
                tenant_id=tenant_id,
                labels=label_list,
                statuses=target_statuses,
                queue_name=queue_name,
            )
            if remaining == 0:
                complete = True
                break
            if _time.monotonic() >= lock_wait_deadline:
                complete = False
                break
            _time.sleep(float(lock_retry_interval))

        return BulkFreezeResult(
            tenant_id=tenant_id,
            action=action,
            count=total,
            jobs=tuple(sample),
            complete=complete,
            remaining_estimate=remaining if not complete else 0,
            batches=batches,
        )

    def _count_matching_jobs(
        self,
        *,
        tenant_id: str,
        labels: list[str],
        statuses: list[str],
        queue_name: str | None,
    ) -> int:
        clauses = ["tenant_id = %s", "status = ANY(%s)"]
        params: list[Any] = [tenant_id, statuses]
        if queue_name is not None:
            clauses.append("queue_name = %s")
            params.append(queue_name)
        if labels:
            clauses.append("labels @> %s::text[]")
            params.append(labels)
        where = " AND ".join(clauses)
        with self._pool.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE {where}",
                tuple(params),
            ).fetchone()
            return int(row["n"] if row else 0)
