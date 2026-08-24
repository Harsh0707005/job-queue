"""JobQueue bulk cancel_matching and cascade dependents."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from job_queue.db import transaction
from job_queue.models import (
    BulkCancelResult,
    Job,
    JobStatus,
)
from job_queue.queue._common import (
    _CANCELABLE_STATUSES,
    _CANCEL_MATCHING_BATCH_DEFAULT,
    _CANCEL_MATCHING_RETURN_CAP,
    _JOB_COLUMNS,
    _normalize_labels,
    _normalize_status_filter,
    _utcnow,
)


class JobsCancelBulkMixin:
    def cancel_matching(
        self,
        *,
        tenant_id: str,
        labels: Sequence[str] | str | None = None,
        status: JobStatus | str | Sequence[JobStatus | str] | None = None,
        queue_name: str | None = None,
        batch_size: int = _CANCEL_MATCHING_BATCH_DEFAULT,
        max_jobs: int | None = None,
        limit: int | None = None,
        lock_wait_seconds: float = 5.0,
        lock_retry_interval: float = 0.05,
    ) -> BulkCancelResult:
        """Cancel **all** cancelable jobs matching tenant + optional filters.

        Loops in batches of *batch_size* until no matching cancelable rows
        remain (or *max_jobs* total cancellations if set). Does **not**
        silently stop after one batch.

        Rows are locked with ``FOR UPDATE SKIP LOCKED``. An empty SKIP LOCKED
        page is **not** treated as success by itself: remaining matches are
        counted without locking. If matches still exist (held by other
        transactions), the sweep waits and retries until *lock_wait_seconds*
        elapses, then returns ``complete=False`` with ``remaining_estimate``.

        *limit* is a deprecated alias for *max_jobs*. Prefer *max_jobs*.

        Filters:
          * *labels*: job must include all listed labels
          * *status*: restrict to these statuses (must still be cancelable)
          * *queue_name*: optional queue scope

        When *status* is omitted, all cancelable statuses are targeted
        (pending, leased, awaiting_approval, awaiting_children,
        awaiting_skills, awaiting_capacity). Terminal
        jobs are never cancelled. Cascades dependents / join children like
        :meth:`cancel`.
        """
        import time as _time

        if not tenant_id:
            raise ValueError("tenant_id is required")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if lock_wait_seconds < 0:
            raise ValueError("lock_wait_seconds must be >= 0")
        if lock_retry_interval <= 0:
            raise ValueError("lock_retry_interval must be > 0")
        # Backward compat: old callers used limit= as a hard page size that
        # silently truncated. Treat limit as max_jobs (absolute cap).
        if max_jobs is None and limit is not None:
            max_jobs = int(limit)
        if max_jobs is not None and max_jobs < 1:
            raise ValueError("max_jobs must be >= 1 when set")

        label_list = _normalize_labels(labels)
        status_list = _normalize_status_filter(status)
        cancelable_values = {s.value for s in _CANCELABLE_STATUSES}
        if status_list is None:
            target_statuses = list(cancelable_values)
        else:
            target_statuses = [s for s in status_list if s in cancelable_values]
            if not target_statuses:
                raise ValueError(
                    "status filter has no cancelable statuses "
                    f"(cancelable={sorted(cancelable_values)})"
                )

        sample: list[Job] = []
        total = 0
        batches = 0
        complete = False
        remaining = 0
        lock_wait_deadline = _time.monotonic() + float(lock_wait_seconds)

        while True:
            if max_jobs is not None and total >= max_jobs:
                remaining = self._count_matching_cancelable(
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
                clauses = [
                    "tenant_id = %s",
                    "status = ANY(%s)",
                ]
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
                    job = self._cancel_locked_row(
                        conn,
                        row=row,
                        tenant_id=tenant_id,
                        now=now,
                        reason="bulk cancel matching filter",
                    )
                    if job is not None:
                        batch_n += 1
                        total += 1
                        if len(sample) < _CANCEL_MATCHING_RETURN_CAP:
                            sample.append(job)

                self._maybe_finalize_offboard(conn, tenant_id=tenant_id, now=now)

            batches += 1

            if batch_n > 0:
                # Progress made; keep sweeping (short page still may have
                # locked leftovers — next iteration will COUNT if empty).
                continue

            # Empty SKIP LOCKED page: either truly drained, or every remaining
            # match is locked by another transaction. Never assume success.
            remaining = self._count_matching_cancelable(
                tenant_id=tenant_id,
                labels=label_list,
                statuses=target_statuses,
                queue_name=queue_name,
            )
            if remaining == 0:
                complete = True
                break

            # Matches exist but are locked — wait and retry until deadline.
            if _time.monotonic() >= lock_wait_deadline:
                complete = False
                break
            _time.sleep(float(lock_retry_interval))

        return BulkCancelResult(
            tenant_id=tenant_id,
            cancelled=total,
            jobs=tuple(sample),
            complete=complete,
            remaining_estimate=remaining if not complete else 0,
            batches=batches,
        )

    def _count_matching_cancelable(
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

    def _cascade_cancel_dependents(
        self,
        conn: Any,
        *,
        parent_id: UUID,
        tenant_id: str,
        reason: str,
        now: datetime,
    ) -> None:
        """Cancel dependents of *parent_id* (recursive).

        Covers:
          * jobs with ``depends_on`` containing *parent_id*
          * spawned subtasks with ``parent_job_id = parent_id``

        Active statuses cancelled: pending, leased, awaiting_approval,
        awaiting_children, awaiting_skills, awaiting_capacity. Running
        (leased) holders have fencing tokens cleared.
        """
        active_pending = JobStatus.PENDING.value
        active_leased = JobStatus.LEASED.value
        active_approval = JobStatus.AWAITING_APPROVAL.value
        active_children = JobStatus.AWAITING_CHILDREN.value
        active_skills = JobStatus.AWAITING_SKILLS.value
        active_capacity = JobStatus.AWAITING_CAPACITY.value
        active_frozen = JobStatus.FROZEN.value
        frontier: list[UUID] = [parent_id]
        visited: set[UUID] = set()
        while frontier:
            current = frontier.pop()
            if current in visited:
                continue
            visited.add(current)
            children = conn.execute(
                f"""
                SELECT {_JOB_COLUMNS}
                FROM jobs
                WHERE tenant_id = %s
                  AND status IN (%s, %s, %s, %s, %s, %s, %s)
                  AND (
                      depends_on @> ARRAY[%s]::uuid[]
                      OR parent_job_id = %s
                  )
                FOR UPDATE
                """,
                (
                    tenant_id,
                    active_pending,
                    active_leased,
                    active_approval,
                    active_children,
                    active_skills,
                    active_capacity,
                    active_frozen,
                    current,
                    current,
                ),
            ).fetchall()
            for child in children:
                was_running = child["status"] == JobStatus.LEASED.value
                from_status = child["status"]
                err = reason
                if was_running:
                    err = f"{reason} (stopped while running)"
                updated = conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = %s,
                        leased_by = NULL,
                        lease_token = NULL,
                        lease_tenant_id = NULL,
                        lease_expires_at = NULL,
                        lease_started_at = NULL,
                        checkpoint = NULL,
                        checkpointed_at = NULL,
                        await_children_deadline_at = NULL,
                        await_children_outcome = CASE
                            WHEN status = %s
                            THEN COALESCE(await_children_outcome, 'cancelled')
                            ELSE await_children_outcome
                        END,
                        await_children_resolved_at = CASE
                            WHEN status = %s
                            THEN COALESCE(await_children_resolved_at, %s)
                            ELSE await_children_resolved_at
                        END,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                      AND tenant_id = %s
                      AND status IN (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING {_JOB_COLUMNS}
                    """,
                    (
                        JobStatus.CANCELLED.value,
                        JobStatus.AWAITING_CHILDREN.value,
                        JobStatus.AWAITING_CHILDREN.value,
                        now,
                        err,
                        now,
                        child["id"],
                        tenant_id,
                        active_pending,
                        active_leased,
                        active_approval,
                        active_children,
                        active_skills,
                        active_capacity,
                        active_frozen,
                    ),
                ).fetchone()
                if updated is not None:
                    self._append_job_audit(
                        conn,
                        job_row=updated,
                        event_type="cascade_cancelled",
                        from_status=from_status,
                        actor="system",
                        detail={"reason": err},
                        now=now,
                    )
                    if hasattr(self, "_record_lease_end_usage"):
                        self._record_lease_end_usage(
                            conn, child, now=now, cancels=1
                        )
                frontier.append(child["id"])
